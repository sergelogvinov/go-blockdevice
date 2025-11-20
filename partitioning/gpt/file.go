// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package gpt

import (
	"fmt"
	"os"
)

type fileWrapper struct {
	f    *os.File
	size uint64
}

// DeviceFromFile creates a new Device from an os.File for use with disk image files.
// This is useful for creating partition tables on file-based disk images without
// requiring an actual block device.
//
// Note: The file size is captured at the time of device creation. If the file is
// truncated or resized after creating the Device, create a new Device to reflect
// the updated size.
func DeviceFromFile(f *os.File) (Device, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return &fileWrapper{
		f:    f,
		size: uint64(info.Size()),
	}, nil
}

// ReadAt implements Device interface.
func (d *fileWrapper) ReadAt(p []byte, off int64) (int, error) {
	return d.f.ReadAt(p, off)
}

// WriteAt implements Device interface.
func (d *fileWrapper) WriteAt(p []byte, off int64) (int, error) {
	return d.f.WriteAt(p, off)
}

// GetSectorSize implements Device interface.
func (d *fileWrapper) GetSectorSize() uint {
	return 512 // Common default sector size for files
}

// GetSize implements Device interface.
func (d *fileWrapper) GetSize() uint64 {
	return d.size
}

// GetIOSize implements Device interface.
func (d *fileWrapper) GetIOSize() (uint, error) {
	return 512, nil
}

// Sync implements Device interface.
func (d *fileWrapper) Sync() error {
	return d.f.Sync()
}

// GetKernelLastPartitionNum is not supported for file-based devices.
func (d *fileWrapper) GetKernelLastPartitionNum() (int, error) {
	return 0, nil
}

// KernelPartitionAdd is not supported for file-based devices.
func (d *fileWrapper) KernelPartitionAdd(no int, start, length uint64) error {
	return nil
}

// KernelPartitionResize is not supported for file-based devices.
func (d *fileWrapper) KernelPartitionResize(no int, first, length uint64) error {
	return nil
}

// KernelPartitionDelete is not supported for file-based devices.
func (d *fileWrapper) KernelPartitionDelete(no int) error {
	return nil
}
