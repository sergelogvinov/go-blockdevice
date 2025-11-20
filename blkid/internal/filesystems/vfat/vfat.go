// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package vfat probes FAT12/FAT16/FAT32 filesystems.
package vfat

//go:generate go run ../../../../internal/cstruct/cstruct.go -pkg vfat -struct MSDOSSB -input msdos.h -endianness LittleEndian

//go:generate go run ../../../../internal/cstruct/cstruct.go -pkg vfat -struct VFATSB -input vfat.h -endianness LittleEndian

//go:generate go run ../../../../internal/cstruct/cstruct.go -pkg vfat -struct DirEntry -input direntry.h -endianness LittleEndian

import (
	"encoding/binary"
	"strings"

	"github.com/siderolabs/go-blockdevice/v2/blkid/internal/magic"
	"github.com/siderolabs/go-blockdevice/v2/blkid/internal/probe"
	"github.com/siderolabs/go-blockdevice/v2/blkid/internal/utils"
)

var (
	fatMagic1 = magic.Magic{
		Offset: 0x52,
		Value:  []byte("MSWIN"),
	}

	fatMagic2 = magic.Magic{
		Offset: 0x52,
		Value:  []byte("FAT32   "),
	}

	fatMagic3 = magic.Magic{
		Offset: 0x36,
		Value:  []byte("MSDOS"),
	}

	fatMagic4 = magic.Magic{
		Offset: 0x36,
		Value:  []byte("FAT16   "),
	}

	fatMagic5 = magic.Magic{
		Offset: 0x36,
		Value:  []byte("FAT12   "),
	}

	fatMagic6 = magic.Magic{
		Offset: 0x36,
		Value:  []byte("FAT     "),
	}
)

// Probe for the filesystem.
type Probe struct{}

// Magic returns the magic value for the filesystem.
func (p *Probe) Magic() []*magic.Magic {
	return []*magic.Magic{
		&fatMagic1,
		&fatMagic2,
		&fatMagic3,
		&fatMagic4,
		&fatMagic5,
		&fatMagic6,
	}
}

// Name returns the name of the filesystem.
func (p *Probe) Name() string {
	return "vfat"
}

// Probe runs the further inspection and returns the result if successful.
func (p *Probe) Probe(r probe.Reader, _ magic.Magic) (*probe.Result, error) {
	vfatBuf := make([]byte, VFATSB_SIZE)
	msdosBuf := make([]byte, MSDOSSB_SIZE)

	if _, err := r.ReadAt(vfatBuf, 0); err != nil {
		return nil, err
	}

	if _, err := r.ReadAt(msdosBuf, 0); err != nil {
		return nil, err
	}

	vfatSB := VFATSB(vfatBuf)
	msdosSB := MSDOSSB(msdosBuf)

	fatSize, valid := isValid(msdosSB, vfatSB)

	if !valid {
		return nil, nil //nolint:nilnil
	}

	sectorCount := uint32(msdosSB.Get_ms_sectors())
	if sectorCount == 0 {
		sectorCount = msdosSB.Get_ms_total_sect()
	}

	sectorSize := uint32(msdosSB.Get_ms_sector_size())

	var label *string

	if msdosSB.Get_ms_fat_length() > 0 {
		rootStart := int64(uint32(msdosSB.Get_ms_reserved())+fatSize) * int64(sectorSize)
		rootDirEntries := uint32(vfatSB.Get_vs_dir_entries())

		dosLabel, err := p.searchFATLabel(r, rootStart, rootDirEntries)
		if err == nil {
			dosLabel = strings.TrimRight(dosLabel, " ")

			label = &dosLabel
		}
	} else if vfatSB.Get_vs_fat32_length() > 0 {
		maxLoops := 100

		// search the FAT32 root dir for the label attribute
		bufSize := uint32(vfatSB.Get_vs_cluster_size()) * sectorSize
		buf := make([]byte, bufSize)
		startDataSector := uint32(msdosSB.Get_ms_reserved()) + fatSize
		entries := uint32((uint64(vfatSB.Get_vs_fat32_length()) * uint64(sectorSize)) / 4)
		next := vfatSB.Get_vs_root_cluster()

		for next >= 2 && next < entries && maxLoops > 0 {
			nextSectOff := (next - 2) * uint32(vfatSB.Get_vs_cluster_size())
			nextOff := uint64(startDataSector+nextSectOff) * uint64(sectorSize)

			count := bufSize / DIRENTRY_SIZE

			vfatLabel, err := p.searchFATLabel(r, int64(nextOff), count)
			if err == nil {
				vfatLabel = strings.TrimRight(vfatLabel, " ")
				label = &vfatLabel

				break
			}

			// get FAT entry
			fatEntryOff := ((uint64(msdosSB.Get_ms_reserved()) * uint64(sectorSize)) + (uint64(next) * 4))

			if _, err := r.ReadAt(buf, int64(fatEntryOff)); err != nil {
				// ignore error
				break
			}

			// set next cluster
			next = binary.LittleEndian.Uint32(buf) & 0x0fffffff
		}
	}

	res := &probe.Result{
		BlockSize:           sectorSize,
		FilesystemBlockSize: uint32(vfatSB.Get_vs_cluster_size()) * sectorSize,
		ProbedSize:          uint64(sectorCount) * uint64(sectorSize),
		Label:               label,
	}

	return res, nil //nolint:nilerr
}

func isValid(msdosSB MSDOSSB, vfatSB VFATSB) (uint32, bool) {
	if msdosSB.Get_ms_fats() == 0 {
		return 0, false
	}

	if msdosSB.Get_ms_reserved() == 0 {
		return 0, false
	}

	if !(0xf8 <= msdosSB.Get_ms_media() || msdosSB.Get_ms_media() == 0xf0) { //nolint:staticcheck
		return 0, false
	}

	if !utils.IsPowerOf2(msdosSB.Get_ms_cluster_size()) {
		return 0, false
	}

	if !utils.IsPowerOf2(msdosSB.Get_ms_sector_size()) {
		return 0, false
	}

	if msdosSB.Get_ms_sector_size() < 512 || msdosSB.Get_ms_sector_size() > 4096 {
		return 0, false
	}

	fatLength := uint32(msdosSB.Get_ms_fat_length())
	if fatLength == 0 {
		fatLength = vfatSB.Get_vs_fat32_length()
	}

	fatSize := fatLength * uint32(msdosSB.Get_ms_fats())

	return fatSize, true
}

// FAT directory constants.
//
//nolint:revive
const (
	FAT_ENTRY_FREE     = 0xe5
	FAT_ATTR_LONG_NAME = 0x0f
	FAT_ATTR_MASK      = 0x3f
	FAT_ATTR_VOLUME_ID = 0x08
	FAT_ATTR_DIR       = 0x10
)

func (p *Probe) searchFATLabel(r probe.Reader, offset int64, entries uint32) (string, error) {
	buf := make([]byte, entries*DIRENTRY_SIZE)

	if _, err := r.ReadAt(buf, offset); err != nil {
		return "", err
	}

	for i := range entries {
		dir := DirEntry(buf[i*DIRENTRY_SIZE : (i+1)*DIRENTRY_SIZE])

		if dir.Get_name()[0] == 0x00 {
			break
		}

		if dir.Get_name()[0] == FAT_ENTRY_FREE || dir.Get_cluster_high() != 0 || dir.Get_cluster_low() != 0 || (dir.Get_attr()&FAT_ATTR_MASK) == FAT_ATTR_LONG_NAME {
			continue
		}

		if (dir.Get_attr() & (FAT_ATTR_VOLUME_ID | FAT_ATTR_DIR)) == FAT_ATTR_VOLUME_ID {
			label := string(dir.Get_name())

			if label[0] == 0x05 {
				label = "\xe5" + label[1:]
			}

			return label, nil
		}
	}

	return "", nil
}
