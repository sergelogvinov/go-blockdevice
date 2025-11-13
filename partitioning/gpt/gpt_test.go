// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package gpt_test

import (
	"embed"
	"errors"
	randv2 "math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/freddierice/go-losetup/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/siderolabs/go-blockdevice/v2/block"
	"github.com/siderolabs/go-blockdevice/v2/partitioning/gpt"
)

const (
	MiB = 1024 * 1024
	GiB = 1024 * MiB
)

func sfdiskDump(t *testing.T, devPath string) string {
	t.Helper()

	cmd := exec.Command("sfdisk", "--dump", devPath)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	assert.NoError(t, err)

	output := string(out)
	output = regexp.MustCompile(`device:[^\n]+\n`).ReplaceAllString(output, "")
	output = regexp.MustCompile(`/dev/[^:]+:\s+`).ReplaceAllString(output, "")

	t.Log("sfdisk output:\n", output)

	return output
}

func gdiskDump(t *testing.T, devPath string) string {
	t.Helper()

	cmd := exec.Command("gdisk", "-l", devPath)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	assert.NoError(t, err)

	output := string(out)
	output = regexp.MustCompile(`^GPT [^\n]+\n\n`).ReplaceAllString(output, "")
	output = regexp.MustCompile(`Disk /dev[^:+]+:`).ReplaceAllString(output, "")
	output = strings.ReplaceAll(output, "\a", "")

	t.Log("gdisk output:\n", output)

	return output
}

//go:embed testdata/*
var testdataFs embed.FS

func loadTestdata(t *testing.T, name string) string {
	t.Helper()

	data, err := testdataFs.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)

	return string(data)
}

func assertAllocated(t *testing.T, expectedIndex int) func(_ int, _ gpt.Partition, err error) {
	return func(index int, _ gpt.Partition, err error) {
		t.Helper()

		require.NoError(t, err)
		assert.Equal(t, expectedIndex, index)
	}
}

func TestGPT(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("test requires root privileges")
	}

	partType1 := uuid.MustParse("C12A7328-F81F-11D2-BA4B-00A0C93EC93B")
	partType2 := uuid.MustParse("E6D6D379-F507-44C2-A23C-238F2A3DF928")

	for _, test := range []struct { //nolint:govet
		name string

		opts []gpt.Option

		diskSize uint64

		allocator func(*testing.T, *gpt.Table)

		expectedSfdiskDump string
		expectedGdiskDump  string
	}{
		{
			name:     "empty",
			diskSize: 2 * GiB,
			opts: []gpt.Option{
				gpt.WithDiskGUID(uuid.MustParse("D815C311-BDED-43FE-A91A-DCBE0D8025D5")),
			},

			expectedSfdiskDump: loadTestdata(t, "empty.sfdisk"),
			expectedGdiskDump:  loadTestdata(t, "empty.gdisk"),
		},
		{
			name:     "empty without PMBR",
			diskSize: 2 * GiB,
			opts: []gpt.Option{
				gpt.WithDiskGUID(uuid.MustParse("D815C311-BDED-43FE-A91A-DCBE0D8025D5")),
				gpt.WithSkipPMBR(),
			},

			expectedGdiskDump: loadTestdata(t, "empty-no-mbr.gdisk"),
		},
		{
			name:     "simple allocate",
			diskSize: 6 * GiB,
			opts: []gpt.Option{
				gpt.WithDiskGUID(uuid.MustParse("B6D003E5-7D1D-45E3-9F4B-4A2430B46D4A")),
			},
			allocator: func(t *testing.T, table *gpt.Table) {
				t.Helper()

				assertAllocated(t, 1)(table.AllocatePartition(1*GiB, "1G", partType1,
					gpt.WithUniqueGUID(uuid.MustParse("DA66737E-1ED4-4DDF-B98C-70CEBFE3ADA0")),
				))
				assertAllocated(t, 2)(table.AllocatePartition(100*MiB, "100M", partType1,
					gpt.WithUniqueGUID(uuid.MustParse("3D0FE86B-7791-4659-B564-FC49A542866D")),
					gpt.WithLegacyBIOSBootableAttribute(true),
				))
				assertAllocated(t, 3)(table.AllocatePartition(2.5*GiB, "2.5G", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("EE1A711E-DE12-4D9F-98FF-672F7AD638F8")),
				))
				assertAllocated(t, 4)(table.AllocatePartition(1*GiB, "1G", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("15E609C8-9775-4E86-AF59-8A87E7C03FAB")),
				))
			},

			expectedSfdiskDump: loadTestdata(t, "allocate.sfdisk"),
			expectedGdiskDump:  loadTestdata(t, "allocate.gdisk"),
		},
		{
			name:     "allocate with small delete",
			diskSize: 4 * GiB,
			opts: []gpt.Option{
				gpt.WithDiskGUID(uuid.MustParse("B6D003E5-7D1D-45E3-9F4B-4A2430B46D4A")),
			},
			allocator: func(t *testing.T, table *gpt.Table) {
				t.Helper()

				assertAllocated(t, 1)(table.AllocatePartition(1*GiB, "1G", partType1,
					gpt.WithUniqueGUID(uuid.MustParse("DA66737E-1ED4-4DDF-B98C-70CEBFE3ADA0")),
				))
				assertAllocated(t, 2)(table.AllocatePartition(100*MiB, "100M", partType1,
					gpt.WithUniqueGUID(uuid.MustParse("3D0FE86B-7791-4659-B564-FC49A542866D")),
					gpt.WithLegacyBIOSBootableAttribute(true),
				))
				assertAllocated(t, 3)(table.AllocatePartition(
					table.LargestContiguousAllocatable(), "2.5G", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("EE1A711E-DE12-4D9F-98FF-672F7AD638F8")),
				))

				require.NoError(t, table.DeletePartition(1))

				assert.EqualValues(t, 100*MiB, table.LargestContiguousAllocatable())

				assertAllocated(t, 2)(table.AllocatePartition(100*MiB, "100M", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("15E609C8-9775-4E86-AF59-8A87E7C03FAB")),
				))
			},

			expectedSfdiskDump: loadTestdata(t, "smalldelete.sfdisk"),
			expectedGdiskDump:  loadTestdata(t, "smalldelete.gdisk"),
		},
		{
			name:     "allocate with deletes",
			diskSize: 6 * GiB,
			opts: []gpt.Option{
				gpt.WithDiskGUID(uuid.MustParse("B6D003E5-7D1D-45E3-9F4B-4A2430B46D4A")),
			},
			allocator: func(t *testing.T, table *gpt.Table) {
				t.Helper()

				// allocate 4 1G partitions first, and delete two in the middle

				assertAllocated(t, 1)(table.AllocatePartition(1*GiB, "1G1", partType1,
					gpt.WithUniqueGUID(uuid.MustParse("DA66737E-1ED4-4DDF-B98C-70CEBFE3ADA0")),
				))
				assertAllocated(t, 2)(table.AllocatePartition(1*GiB, "1G2", partType1))
				assertAllocated(t, 3)(table.AllocatePartition(1*GiB, "1G3", partType1))
				assertAllocated(t, 4)(table.AllocatePartition(1*GiB, "1G4", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("3D0FE86B-7791-4659-B564-FC49A542866D")),
				))

				require.NoError(t, table.DeletePartition(1))
				require.NoError(t, table.DeletePartition(2))

				// gap is 2 GiB, while the tail available space is < 2 GiB, so small partitions will be appended to the end
				assertAllocated(t, 5)(table.AllocatePartition(200*MiB, "200M", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("EE1A711E-DE12-4D9F-98FF-672F7AD638F8")),
				))
				assertAllocated(t, 6)(table.AllocatePartition(400*MiB, "400M", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("15E609C8-9775-4E86-AF59-8A87E7C03FAB")),
				))

				// bigger partition will fill the gap
				assertAllocated(t, 2)(table.AllocatePartition(1500*MiB, "1500M", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("15E609C8-9775-4E86-AF59-8A87E7C03FAC")),
				))
			},

			expectedSfdiskDump: loadTestdata(t, "mix-allocate.sfdisk"),
			expectedGdiskDump:  loadTestdata(t, "mix-allocate.gdisk"),
		},
		{
			name:     "resize",
			diskSize: 6 * GiB,
			opts: []gpt.Option{
				gpt.WithDiskGUID(uuid.MustParse("B6D003E5-7D1D-45E3-9F4B-4A2430B46D4A")),
			},
			allocator: func(t *testing.T, table *gpt.Table) {
				t.Helper()

				// allocate 2 1G partitions first, and grow the last one
				assertAllocated(t, 1)(table.AllocatePartition(1*GiB, "1G", partType1,
					gpt.WithUniqueGUID(uuid.MustParse("DA66737E-1ED4-4DDF-B98C-70CEBFE3ADA0")),
				))
				assertAllocated(t, 2)(table.AllocatePartition(1*GiB, "GROW", partType2,
					gpt.WithUniqueGUID(uuid.MustParse("3D0FE86B-7791-4659-B564-FC49A542866D")),
				))

				// attempt to grow the first one
				growth, err := table.AvailablePartitionGrowth(0)
				require.NoError(t, err)

				assert.EqualValues(t, 0, growth)

				// grow the second one
				growth, err = table.AvailablePartitionGrowth(1)
				require.NoError(t, err)

				assert.EqualValues(t, 4*GiB-(2048+2048)*512, growth)

				require.NoError(t, table.GrowPartition(1, growth))
			},

			expectedSfdiskDump: loadTestdata(t, "grow.sfdisk"),
			expectedGdiskDump:  loadTestdata(t, "grow.gdisk"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			rawImage := filepath.Join(tmpDir, "image.raw")

			f, err := os.Create(rawImage)
			require.NoError(t, err)

			require.NoError(t, f.Truncate(int64(test.diskSize)))
			require.NoError(t, f.Close())

			loDev := losetupAttachHelper(t, rawImage, false)

			t.Cleanup(func() {
				assert.NoError(t, loDev.Detach())
			})

			disk, err := os.OpenFile(loDev.Path(), os.O_RDWR, 0)
			require.NoError(t, err)

			t.Cleanup(func() {
				assert.NoError(t, disk.Close())
			})

			blkdev := block.NewFromFile(disk)

			gptdev, err := gpt.DeviceFromBlockDevice(blkdev)
			require.NoError(t, err)

			table, err := gpt.New(gptdev, test.opts...)
			require.NoError(t, err)

			assert.EqualValues(t, test.diskSize-(2048+2048)*512, table.LargestContiguousAllocatable())

			if test.allocator != nil {
				test.allocator(t, table)
			}

			require.NoError(t, table.Write())

			if test.expectedSfdiskDump != "" {
				assert.Equal(t, test.expectedSfdiskDump, sfdiskDump(t, loDev.Path()))
			}

			if test.expectedGdiskDump != "" {
				assert.Equal(t, test.expectedGdiskDump, gdiskDump(t, loDev.Path()))
			}

			// re-read the table and check if it's the same
			table2, err := gpt.Read(gptdev, test.opts...)
			require.NoError(t, err)

			assert.Equal(t, table.Partitions(), table2.Partitions())

			// re-write the partition table
			require.NoError(t, table2.Write())

			if test.expectedSfdiskDump != "" {
				assert.Equal(t, test.expectedSfdiskDump, sfdiskDump(t, loDev.Path()))
			}

			if test.expectedGdiskDump != "" {
				assert.Equal(t, test.expectedGdiskDump, gdiskDump(t, loDev.Path()))
			}
		})
	}
}

func TestGPTOverwrite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("test requires root privileges")
	}

	if hostname, _ := os.Hostname(); hostname == "buildkitsandbox" { //nolint: errcheck
		t.Skip("test not supported under buildkit as partition devices are not propagated from /dev")
	}

	partType1 := uuid.MustParse("C12A7328-F81F-11D2-BA4B-00A0C93EC93B")
	partType2 := uuid.MustParse("E6D6D379-F507-44C2-A23C-238F2A3DF928")

	// create a partition table, and then overwrite it with a new one with incompatible layout
	tmpDir := t.TempDir()

	rawImage := filepath.Join(tmpDir, "image.raw")

	f, err := os.Create(rawImage)
	require.NoError(t, err)

	require.NoError(t, f.Truncate(int64(3*GiB)))
	require.NoError(t, f.Close())

	loDev := losetupAttachHelper(t, rawImage, false)

	t.Cleanup(func() {
		assert.NoError(t, loDev.Detach())
	})

	disk, err := os.OpenFile(loDev.Path(), os.O_RDWR, 0)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, disk.Close())
	})

	blkdev := block.NewFromFile(disk)

	gptdev, err := gpt.DeviceFromBlockDevice(blkdev)
	require.NoError(t, err)

	table, err := gpt.New(gptdev)
	require.NoError(t, err)

	// allocate 2 1G partitions first
	assertAllocated(t, 1)(table.AllocatePartition(100*MiB, "1G", partType1))
	assertAllocated(t, 2)(table.AllocatePartition(1*GiB, "2G", partType2))

	require.NoError(t, table.Write())

	assert.FileExists(t, loDev.Path()+"p1")
	assert.FileExists(t, loDev.Path()+"p2")

	// now attempt to overwrite the partition table with a new one with different layout
	table2, err := gpt.New(gptdev)
	require.NoError(t, err)

	// allocate new partitions first
	assertAllocated(t, 1)(table2.AllocatePartition(600*MiB, "1P", partType1))
	assertAllocated(t, 2)(table2.AllocatePartition(600*MiB, "2P", partType2))
	assertAllocated(t, 3)(table2.AllocatePartition(600*MiB, "3P", partType2))

	require.NoError(t, table2.Write())

	assert.FileExists(t, loDev.Path()+"p1")
	assert.FileExists(t, loDev.Path()+"p2")
	assert.FileExists(t, loDev.Path()+"p3")
}

func TestGPTDifferentIOSize(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("test requires root privileges")
	}

	tmpDir := t.TempDir()

	rawImage := filepath.Join(tmpDir, "image.raw")

	f, err := os.Create(rawImage)
	require.NoError(t, err)

	require.NoError(t, f.Truncate(2*GiB))
	require.NoError(t, f.Close())

	loDev := losetupAttachHelper(t, rawImage, false)

	t.Cleanup(func() {
		assert.NoError(t, loDev.Detach())
	})

	disk, err := os.OpenFile(loDev.Path(), os.O_RDWR, 0)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, disk.Close())
	})

	blkdev := block.NewFromFile(disk)

	gptdev, err := gpt.DeviceFromBlockDevice(blkdev)
	require.NoError(t, err)

	table, err := gpt.New(gptdev)
	require.NoError(t, err)

	partType1 := uuid.MustParse("C12A7328-F81F-11D2-BA4B-00A0C93EC93B")

	// allocate two partitions
	assertAllocated(t, 1)(table.AllocatePartition(1*MiB, "1M", partType1))
	assertAllocated(t, 2)(table.AllocatePartition(1*GiB, "1G", partType1))

	require.NoError(t, table.Write())

	// now wrap the gpt devices with a new IO size
	ioSizeWrapper := &ioSizeWrapper{
		Device: gptdev,
		ioSize: 2 * MiB,
	}

	table2, err := gpt.Read(ioSizeWrapper)
	require.NoError(t, err)

	// the partition table should be the same even if the IO size is different
	assert.Equal(t, table.Partitions(), table2.Partitions())
}

func TestGPTSkipBlocks(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("test requires root privileges")
	}

	tmpDir := t.TempDir()

	rawImage := filepath.Join(tmpDir, "image.raw")

	f, err := os.Create(rawImage)
	require.NoError(t, err)

	require.NoError(t, f.Truncate(2*GiB))
	require.NoError(t, f.Close())

	loDev := losetupAttachHelper(t, rawImage, false)

	t.Cleanup(func() {
		assert.NoError(t, loDev.Detach())
	})

	disk, err := os.OpenFile(loDev.Path(), os.O_RDWR, 0)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, disk.Close())
	})

	blkdev := block.NewFromFile(disk)

	gptdev, err := gpt.DeviceFromBlockDevice(blkdev)
	require.NoError(t, err)

	table, err := gpt.New(gptdev, gpt.WithSkipLBAs(10240))
	require.NoError(t, err)

	partType1 := uuid.MustParse("C12A7328-F81F-11D2-BA4B-00A0C93EC93B")

	// allocate partition spanning whole disk
	assertAllocated(t, 1)(table.AllocatePartition(table.LargestContiguousAllocatable(), "1G", partType1))

	assert.EqualValues(t, 0, table.LargestContiguousAllocatable())

	require.NoError(t, table.Write())

	table2, err := gpt.Read(gptdev)
	require.NoError(t, err)

	// if we read the table back, it should honor first LBA skip
	assert.Equal(t, table2.LargestContiguousAllocatable(), table.LargestContiguousAllocatable())
}

type ioSizeWrapper struct {
	gpt.Device

	ioSize uint
}

func (w *ioSizeWrapper) GetIOSize() (uint, error) {
	return w.ioSize, nil
}

//nolint:unparam
func losetupAttachHelper(t *testing.T, rawImage string, readonly bool) losetup.Device {
	t.Helper()

	for range 10 {
		loDev, err := losetup.Attach(rawImage, 0, readonly)
		if err != nil {
			if errors.Is(err, unix.EBUSY) {
				spraySleep := max(randv2.ExpFloat64(), 2.0)

				t.Logf("retrying after %v seconds", spraySleep)

				time.Sleep(time.Duration(spraySleep * float64(time.Second)))

				continue
			}
		}

		require.NoError(t, err)

		return loDev
	}

	t.Fatal("failed to attach loop device") //nolint:revive

	panic("unreachable")
}
