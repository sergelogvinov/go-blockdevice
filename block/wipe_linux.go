// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package block

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	// FastWipeRange fast wipe block.
	FastWipeRange = 1024 * 1024
)

// Range is a range of bytes.
type Range struct {
	Offset uint64
	Size   uint64
}

// Wipe the device contents.
//
// In order of availability this tries to perform the following:
//   - secure discard (secure erase)
//   - discard with zeros
//   - zero out via ioctl
//   - zero out from userland
func (d *Device) Wipe() (string, error) {
	size, err := d.GetSize()
	if err != nil {
		return "", err
	}

	return d.WipeRange(0, size)
}

// FastWipe the device contents.
//
// This method is much faster than Wipe(), but it doesn't guarantee
// that device will be zeroed out completely.
//
// If ranges are given, only those ranges will be wiped.
// Otherwise, the first FastWipeRange and the last FastWipeRange bytes will be wiped.
func (d *Device) FastWipe(ranges ...Range) error {
	size, err := d.GetSize()
	if err != nil {
		return err
	}

	// BLKDISCARD is implemented via TRIM on SSDs, it might or might not zero out device contents.
	r := [2]uint64{0, size}

	// ignoring the error here as DISCARD might be not supported by the device
	unix.Syscall(unix.SYS_IOCTL, d.f.Fd(), unix.BLKDISCARD, uintptr(unsafe.Pointer(&r[0]))) //nolint: errcheck

	if len(ranges) == 0 {
		// zero out the first N bytes of the device to clear any partition table
		wipeLength := min(size, uint64(FastWipeRange))

		_, err = d.WipeRange(0, wipeLength)
		if err != nil {
			return err
		}

		// wipe the last FastWipeRange bytes of the device as well
		if size >= FastWipeRange*2 {
			_, err = d.WipeRange(size-FastWipeRange, FastWipeRange)
			if err != nil {
				return err
			}
		}

		return nil
	}

	for _, r := range ranges {
		_, err = d.WipeRange(r.Offset, r.Size)
		if err != nil {
			return fmt.Errorf("error wiping %d:+%d: %w", r.Offset, r.Size, err)
		}
	}

	return nil
}

// WipeRange the device [start, start+length).
func (d *Device) WipeRange(start, length uint64) (string, error) {
	if length == 0 {
		return "noop", nil
	}

	// verify alignment before starting to use ioctl ways
	if start&0x7ff == 0 && length&0x7ff == 0 {
		r := [2]uint64{start, length}

		if _, _, errno := unix.Syscall(unix.SYS_IOCTL, d.f.Fd(), unix.BLKSECDISCARD, uintptr(unsafe.Pointer(&r[0]))); errno == 0 {
			runtime.KeepAlive(d)

			return "blksecdiscard", nil
		}

		if _, _, errno := unix.Syscall(unix.SYS_IOCTL, d.f.Fd(), unix.BLKZEROOUT, uintptr(unsafe.Pointer(&r[0]))); errno == 0 {
			runtime.KeepAlive(d)

			return "blkzeroout", nil
		}
	}

	if length >= 65536 { // arbitrary threshold to use /dev/zero instead of allocating a zero slice
		zero, err := os.Open("/dev/zero")
		if err != nil {
			return "", err
		}

		defer zero.Close() //nolint: errcheck

		_, err = d.f.Seek(int64(start), io.SeekStart)
		if err != nil {
			return "", err
		}

		_, err = io.CopyN(d.f, zero, int64(length))

		return "writezeroes", err
	}

	zeroes := make([]byte, length)

	_, err := d.f.WriteAt(zeroes, int64(start))

	return "writezero", err
}
