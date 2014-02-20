// +build linux,amd64

package devmapper

import (
	"fmt"
	"github.com/dotcloud/docker/graphdriver"
	"github.com/dotcloud/docker/utils"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

func init() {
	graphdriver.Register("devicemapper", Init)
}

// Placeholder interfaces, to be replaced
// at integration.

// End of placeholder interfaces.

type Driver struct {
	*DeviceSet
	home string
}

var Init = func(home string) (graphdriver.Driver, error) {
	deviceSet, err := NewDeviceSet(home, true)
	if err != nil {
		return nil, err
	}
	d := &Driver{
		DeviceSet: deviceSet,
		home:      home,
	}
	return d, nil
}

func (d *Driver) String() string {
	return "devicemapper"
}

func (d *Driver) Status() [][2]string {
	s := d.DeviceSet.Status()

	status := [][2]string{
		{"Pool Name", s.PoolName},
		{"Data file", s.DataLoopback},
		{"Metadata file", s.MetadataLoopback},
		{"Data Space Used", fmt.Sprintf("%.1f Mb", float64(s.Data.Used)/(1024*1024))},
		{"Data Space Total", fmt.Sprintf("%.1f Mb", float64(s.Data.Total)/(1024*1024))},
		{"Metadata Space Used", fmt.Sprintf("%.1f Mb", float64(s.Metadata.Used)/(1024*1024))},
		{"Metadata Space Total", fmt.Sprintf("%.1f Mb", float64(s.Metadata.Total)/(1024*1024))},
	}
	return status
}

func byteSizeFromString(arg string) (int64, error) {
	digits := ""
	rest := ""
	last := strings.LastIndexAny(arg, "0123456789")
	if last >= 0 {
		digits = arg[:last+1]
		rest = arg[last+1:]
	}

	val, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return val, err
	}

	rest = strings.ToLower(strings.TrimSpace(rest))

	var multiplier int64 = 1
	switch rest {
	case "":
		multiplier = 1
	case "k", "kb":
		multiplier = 1024
	case "m", "mb":
		multiplier = 1024 * 1024
	case "g", "gb":
		multiplier = 1024 * 1024 * 1024
	case "t", "tb":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("Unknown size unit: %s", rest)
	}

	return val * multiplier, nil
}

func (d *Driver) Operation(op string, args []string) error {
	switch op {
	case "trim-pool":
		if len(args) != 0 {
			return fmt.Errorf("Usage: trim-pool")
		}

		err := d.DeviceSet.TrimPool()
		if err != nil {
			return fmt.Errorf("Error trimming pool: %s", err.Error())
		}

		return nil
	case "resize-pool":
		if len(args) != 1 {
			return fmt.Errorf("Usage: resize-pool NEW_SIZE")
		}

		size, err := byteSizeFromString(args[0])
		if err != nil {
			return fmt.Errorf("Invalid size: %s", args[0])
		}

		err = d.DeviceSet.ResizePool(size)
		if err != nil {
			return fmt.Errorf("Error resizing pool: %s", err.Error())
		}

		return nil
	case "resize":
		if len(args) != 2 {
			return fmt.Errorf("Usage: resize IMAGE/CONTAINER NEW_SIZE")
		}

		size, err := byteSizeFromString(args[1])
		if err != nil {
			return fmt.Errorf("Invalid size: %s", args[0])
		}

		err = d.DeviceSet.ResizeDevice(args[0], size)
		if err != nil {
			return fmt.Errorf("Error resizing %s: %s", args[0], err.Error())
		}

		return nil
	default:
		return fmt.Errorf("Operation %s not supported", op)
	}
}

func (d *Driver) Cleanup() error {
	return d.DeviceSet.Shutdown()
}

func (d *Driver) Create(id, parent string) error {
	if err := d.DeviceSet.AddDevice(id, parent); err != nil {
		return err
	}

	mp := path.Join(d.home, "mnt", id)
	if err := d.mount(id, mp); err != nil {
		return err
	}

	if err := osMkdirAll(path.Join(mp, "rootfs"), 0755); err != nil && !osIsExist(err) {
		return err
	}

	// Create an "id" file with the container/image id in it to help reconscruct this in case
	// of later problems
	if err := ioutil.WriteFile(path.Join(mp, "id"), []byte(id), 0600); err != nil {
		return err
	}

	// We float this reference so that the next Get call can
	// steal it, so we don't have to unmount
	if err := d.DeviceSet.UnmountDevice(id, UnmountFloat); err != nil {
		return err
	}

	return nil
}

func (d *Driver) Remove(id string) error {
	// Sink the float from create in case no Get() call was made
	if err := d.DeviceSet.UnmountDevice(id, UnmountSink); err != nil {
		return err
	}
	// This assumes the device has been properly Get/Put:ed and thus is unmounted
	if err := d.DeviceSet.DeleteDevice(id); err != nil {
		return err
	}

	mp := path.Join(d.home, "mnt", id)
	if err := os.RemoveAll(mp); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (d *Driver) Get(id string) (string, error) {
	mp := path.Join(d.home, "mnt", id)
	if err := d.mount(id, mp); err != nil {
		return "", err
	}

	return path.Join(mp, "rootfs"), nil
}

func (d *Driver) Put(id string) {
	if err := d.DeviceSet.UnmountDevice(id, UnmountRegular); err != nil {
		utils.Errorf("Warning: error unmounting device %s: %s\n", id, err)
	}
}

func (d *Driver) mount(id, mountPoint string) error {
	// Create the target directories if they don't exist
	if err := osMkdirAll(mountPoint, 0755); err != nil && !osIsExist(err) {
		return err
	}
	// Mount the device
	return d.DeviceSet.MountDevice(id, mountPoint)
}

func (d *Driver) Exists(id string) bool {
	return d.Devices[id] != nil
}
