// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package probe

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/talos-systems/go-blockdevice/blockdevice"
	"github.com/talos-systems/go-blockdevice/blockdevice/filesystem"
	"github.com/talos-systems/go-blockdevice/blockdevice/filesystem/iso9660"
	"github.com/talos-systems/go-blockdevice/blockdevice/filesystem/vfat"
	"github.com/talos-systems/go-blockdevice/blockdevice/filesystem/xfs"
	"github.com/talos-systems/go-blockdevice/blockdevice/partition/gpt"
	"github.com/talos-systems/go-blockdevice/blockdevice/util"
)

// ProbedBlockDevice represents a probed block device.
type ProbedBlockDevice struct {
	*blockdevice.BlockDevice

	SuperBlock filesystem.SuperBlocker
	Path       string
}

// SelectOption is a callback matcher for All block devices probes.
type SelectOption func(device *ProbedBlockDevice) (bool, error)

// WithPartitionLabel search for a block device which has partitions with some specific label.
func WithPartitionLabel(label string) SelectOption {
	return func(device *ProbedBlockDevice) (bool, error) {
		pt, err := device.PartitionTable()
		if err != nil {
			return false, err
		}

		return pt.Partitions().FindByName(label) != nil, nil
	}
}

// WithFileSystemLabel search for a block device which has filesystem on root level
// and that filesystem is labeled as provided label.
func WithFileSystemLabel(label string) SelectOption {
	return func(device *ProbedBlockDevice) (bool, error) {
		superblock, err := filesystem.Probe(device.Device().Name())
		if err != nil {
			return false, err
		}

		if superblock != nil {
			switch sb := superblock.(type) {
			case *iso9660.SuperBlock:
				trimmed := bytes.Trim(sb.VolumeID[:], " \x00")
				if bytes.Equal(trimmed, []byte(label)) {
					return true, nil
				}
			case *vfat.SuperBlock:
				trimmed := bytes.Trim(sb.Label[:], " \x00")
				if bytes.Equal(trimmed, []byte(label)) {
					return true, nil
				}
			case *xfs.SuperBlock:
				trimmed := bytes.Trim(sb.Fname[:], " \x00")
				if bytes.Equal(trimmed, []byte(label)) {
					return true, nil
				}
			}
		}

		return false, nil
	}
}

// WithSingleResult enforces a single result from All function.
func WithSingleResult() SelectOption {
	count := 0

	return func(device *ProbedBlockDevice) (bool, error) {
		if count > 0 {
			return false, fmt.Errorf("got more than one blockdevice with provided criteria")
		}

		return true, nil
	}
}

// all probes a block device's file system for the given label.
func all(mode int, options ...SelectOption) (all []*ProbedBlockDevice, err error) {
	var infos []os.FileInfo

	if infos, err = ioutil.ReadDir("/sys/block"); err != nil {
		return nil, err
	}

	for _, info := range infos {
		devpath := "/dev/" + info.Name()

		probed := probePartitions(devpath, blockdevice.WithMode(mode))

		for _, dev := range probed {
			add := true
			for _, matches := range options {
				add, err = matches(dev)
				if err != nil {
					if e := dev.Close(); e != nil {
						return nil, e
					}

					return nil, err
				}

				if !add {
					err = dev.Close()
					if err != nil {
						return nil, err
					}

					break
				}
			}

			if add {
				all = append(all, dev)
			}
		}
	}

	return all, nil
}

// DevForPartitionLabel finds and opens partition as a blockdevice.
func DevForPartitionLabel(devname, label string) (*blockdevice.BlockDevice, error) {
	bd, err := blockdevice.Open(devname)
	if err != nil {
		return nil, err
	}

	return bd.OpenPartition(label)
}

func probe(devpath string) (devpaths []string) {
	devpaths = []string{}

	// Start by opening the block device.
	// If a partition table was not found, it is still possible that a
	// file system exists without a partition table.
	bd, err := blockdevice.Open(devpath, blockdevice.WithMode(blockdevice.ReadonlyMode))
	if err != nil {
		//nolint: errcheck
		if sb, _ := filesystem.Probe(devpath); sb != nil {
			devpaths = append(devpaths, devpath)
		}

		return devpaths
	}
	//nolint: errcheck
	defer bd.Close()

	// A partition table was not found, and we have already checked for
	// a file system on the block device. Let's check if the block device
	// has partitions.
	pt, err := bd.PartitionTable()
	if err != nil {
		//nolint: errcheck
		if sb, _ := filesystem.Probe(devpath); sb != nil {
			devpaths = append(devpaths, devpath)
		}

		return devpaths
	}

	// A partition table was found, now probe each partition's file system.
	name := filepath.Base(devpath)

	for _, part := range pt.Partitions().Items() {
		partpath, err := util.PartPath(name, int(part.Number))
		if err != nil {
			return devpaths
		}

		//nolint: errcheck
		if sb, _ := filesystem.Probe(partpath); sb != nil {
			devpaths = append(devpaths, partpath)
		}
	}

	return devpaths
}

// GetDevWithPartitionName probes all known block device's partition
// table for a parition with the specified name.
func GetDevWithPartitionName(name string) (bd *ProbedBlockDevice, err error) {
	probed, err := all(blockdevice.DefaultMode, WithPartitionLabel(name), WithSingleResult())
	if err != nil {
		return nil, err
	}

	if len(probed) == 0 {
		return nil, os.ErrNotExist
	}

	return probed[0], nil
}

// GetDevWithFileSystemLabel probes all known block device's file systems for
// the given label.
func GetDevWithFileSystemLabel(value string) (probe *ProbedBlockDevice, err error) {
	var probed []*ProbedBlockDevice

	if probed, err = all(blockdevice.DefaultMode, WithFileSystemLabel(value), WithSingleResult()); err != nil {
		return nil, err
	}

	if len(probed) == 0 {
		return nil, os.ErrNotExist
	}

	return probed[0], nil
}

// GetDevPathWithFileSystemLabel probes all known block device's file systems for
// the given label, returns path of device.
func GetDevPathWithFileSystemLabel(value string) (path string, err error) {
	var probed []*ProbedBlockDevice

	if probed, err = all(blockdevice.ReadonlyMode, WithFileSystemLabel(value), WithSingleResult()); err != nil {
		return "", err
	}

	if len(probed) == 0 {
		return "", os.ErrNotExist
	}

	return probed[0].Path, nil
}

// GetPartitionWithName probes all known block device's partition
// table for a parition with the specified name.
//
//nolint: gocyclo
func GetPartitionWithName(name string) (part *gpt.Partition, err error) {
	device, err := GetDevWithPartitionName(name)
	if err != nil {
		return nil, err
	}

	return device.GetPartition(name)
}

func probePartitions(devpath string, opts ...blockdevice.Option) (probed []*ProbedBlockDevice) {
	for _, path := range probe(devpath) {
		var (
			bd  *blockdevice.BlockDevice
			sb  filesystem.SuperBlocker
			err error
		)

		bd, err = blockdevice.Open(devpath, opts...)
		if err != nil {
			continue
		}

		probed = append(probed, &ProbedBlockDevice{BlockDevice: bd, SuperBlock: sb, Path: path})
	}

	return probed
}
