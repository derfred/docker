package devmapper

import (
	"fmt"
	"os"
	"io/ioutil"
	"os/exec"
	"encoding/json"
	"path"
	"log"
	"syscall"
)

const BaseVolumeHash = "0"
const defaultDataLoopbackSize int64 = 100*1024*1024*1024
const defaultMetaDataLoopbackSize int64 = 2*1024*1024*1024
const defaultBaseFsSize uint64 = 10*1024*1024*1024

type VolumeInfo struct {
	Hash string `json:-`
	DeviceId int `json:"device-id"`
	Size uint64 `json:size`
	TransactionId uint64 `json:transaction-id`
}

type MetaData struct {
	TransactionId uint64
	Devices map[string]*VolumeInfo `json:devices`
}

type VolumeSetDM struct {
	root string
	MetaData
	nextFreeDevice int
}

func (volumes *VolumeSetDM) loopbackDir() string {
	return path.Join(volumes.root, "loopback")
}

func (volumes *VolumeSetDM) jsonFile() string {
	return path.Join(volumes.loopbackDir(), "json")
}

func (volumes *VolumeSetDM) getDevName(name string) string {
	return "/dev/mapper/" + name
}

func (volumes *VolumeSetDM) getPoolName() string {
	return "docker-pool"
}

func (volumes *VolumeSetDM) getPoolDevName() string {
	return volumes.getDevName(volumes.getPoolName())
}

func (volumes *VolumeSetDM) getNameForDevice(deviceId int) string {
	return fmt.Sprintf("docker-%d", deviceId)
}

func (volumes *VolumeSetDM) getDevNameForDevice(deviceId int) string {
	return volumes.getDevName(volumes.getNameForDevice(deviceId))
}

func (volumes *VolumeSetDM) createTask(t TaskType, name string) (*Task, error) {
	task := TaskCreate(t)
	if task == nil {
		return nil, fmt.Errorf("Can't create task of type %d", int(t))
	}
	err := task.SetName(name)
	if err != nil {
		return nil, fmt.Errorf("Can't set task name %s", name)
	}
	return task, nil
}

func (volumes *VolumeSetDM) getInfo(name string) (*Info, error) {
	task, err := volumes.createTask(DeviceInfo, name)
	if task == nil {
		return nil, err
	}
	err = task.Run()
	if err != nil {
		return nil, err
	}
	info, err := task.GetInfo()
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (volumes *VolumeSetDM) hasImage(name string) bool {
	dirname := volumes.loopbackDir()
	filename := path.Join(dirname, name)

	_, err := os.Stat(filename)
	return err == nil
}


func (volumes *VolumeSetDM) ensureImage(name string, size int64) (string, error) {
	dirname := volumes.loopbackDir()
	filename := path.Join(dirname, name)

	if err := os.MkdirAll(dirname, 0700); err != nil && !os.IsExist(err) {
		return "", err
	}

	_, err := os.Stat(filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		log.Printf("Creating loopback file %s for device-manage use", filename)
		file, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE, 0600)
		if err != nil {
			return "", err
		}
		err = file.Truncate(size)
		if err != nil {
			return "", err
		}
	}
	return filename, nil
}

func (volumes *VolumeSetDM) createPool(dataFile *os.File, metadataFile *os.File) error {
	log.Printf("Activating device-mapper pool %s", volumes.getPoolName())
	task, err := volumes.createTask(DeviceCreate, volumes.getPoolName())
	if task == nil {
		return err
	}

	size, err := GetBlockDeviceSize(dataFile)
	if err != nil {
		return fmt.Errorf("Can't get data size")
	}

	params := metadataFile.Name() + " " + dataFile.Name() + " 512 8192"
	err = task.AddTarget (0, size / 512, "thin-pool", params)
	if err != nil {
		return fmt.Errorf("Can't add target")
	}

	err = task.SetAddNode(AddNodeOnResume)
	if err != nil {
		return fmt.Errorf("Can't set add mode")
	}

	var cookie uint32 = 0
	err = task.SetCookie (&cookie, 32)
	if err != nil {
		return fmt.Errorf("Can't set cookie")
	}

	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running DeviceCreate")
	}

	UdevWait(cookie)

	return nil
}

func (volumes *VolumeSetDM) suspendDevice(deviceId int) error {
	task, err := volumes.createTask(DeviceSuspend, volumes.getNameForDevice(deviceId))
	if task == nil {
		return err
	}
	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running DeviceSuspend")
	}
	return nil
}

func (volumes *VolumeSetDM) resumeDevice(deviceId int) error {
	task, err := volumes.createTask(DeviceResume, volumes.getNameForDevice(deviceId))
	if task == nil {
		return err
	}
	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running DeviceSuspend")
	}
	return nil
}

func (volumes *VolumeSetDM) createDevice(deviceId int) error {
	task, err := volumes.createTask(DeviceTargetMsg, volumes.getPoolDevName())
	if task == nil {
		return err
	}

	err = task.SetSector(0)
	if err != nil {
		return fmt.Errorf("Can't set sector")
	}

	message := fmt.Sprintf("create_thin %d", deviceId)
	err = task.SetMessage(message)
	if err != nil {
		return fmt.Errorf("Can't set message")
	}

	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running createDevice")
	}
	return nil
}

func (volumes *VolumeSetDM) deleteDevice(deviceId int) error {
	task, err := volumes.createTask(DeviceTargetMsg, volumes.getPoolDevName())
	if task == nil {
		return err
	}

	err = task.SetSector(0)
	if err != nil {
		return fmt.Errorf("Can't set sector")
	}

	message := fmt.Sprintf("delete %d", deviceId)
	err = task.SetMessage(message)
	if err != nil {
		return fmt.Errorf("Can't set message")
	}

	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running deleteDevice")
	}
	return nil
}

func (volumes *VolumeSetDM) createSnapDevice(deviceId int, baseDeviceId int) error {
	err := volumes.suspendDevice(baseDeviceId)
	if err != nil {
		return err
	}
	
	task, err := volumes.createTask(DeviceTargetMsg, volumes.getPoolDevName())
	if task == nil {
		_ = volumes.resumeDevice(baseDeviceId)
		return err
	}
	err = task.SetSector(0)
	if err != nil {
		_ = volumes.resumeDevice(baseDeviceId)
		return fmt.Errorf("Can't set sector")
	}

	message := fmt.Sprintf("create_snap %d %d", deviceId, baseDeviceId)
	err = task.SetMessage(message)
	if err != nil {
		_ = volumes.resumeDevice(baseDeviceId)
		return fmt.Errorf("Can't set message")
	}

	err = task.Run()
	if err != nil {
		_ = volumes.resumeDevice(baseDeviceId)
		return fmt.Errorf("Error running DeviceCreate")
	}

	err = volumes.resumeDevice(baseDeviceId)
	if err != nil {
		return err
	}
	
	return nil
}

func (volumes *VolumeSetDM) activateDevice(deviceId int, sizeInBytes uint64) error {
	task, err := volumes.createTask(DeviceCreate, volumes.getNameForDevice(deviceId))
	if task == nil {
		return err
	}

	params := fmt.Sprintf("%s %d", volumes.getPoolDevName(), deviceId)
	err = task.AddTarget (0, sizeInBytes / 512, "thin", params)
	if err != nil {
		return fmt.Errorf("Can't add target")
	}

	err = task.SetAddNode(AddNodeOnResume)
	if err != nil {
		return fmt.Errorf("Can't set add mode")
	}

	var cookie uint32 = 0
	err = task.SetCookie (&cookie, 32)
	if err != nil {
		return fmt.Errorf("Can't set cookie")
	}

	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running DeviceCreate")
	}

	UdevWait(cookie)
	
	return nil
}

func (volumes *VolumeSetDM) allocateDeviceId() int {
	// TODO: Add smarter reuse of deleted devices
	id := volumes.nextFreeDevice
	volumes.nextFreeDevice = volumes.nextFreeDevice + 1
	return id
}

func (volumes *VolumeSetDM) addVolume(id int, hash string, size uint64) error {
	volumes.TransactionId = volumes.TransactionId + 1
	
	volumes.Devices[hash]  = &VolumeInfo{
		Hash: hash,
		DeviceId: id,
		Size: size,
		TransactionId: volumes.TransactionId,
	}

	jsonData, err := json.Marshal(volumes.MetaData)
	if err == nil {
		err = ioutil.WriteFile(volumes.jsonFile(), jsonData, 0600)
	}
	if err != nil {
		// Try to remove unused device
		volumes.Devices[hash] = nil
		return err
	}

	// TODO: fsync the file (and maybe the metadata loopback?) and update the transaction id in the thin-pool

	return nil
}

func (volumes *VolumeSetDM) AddVolume(hash, baseHash string) error {
	if (baseHash == "") {
		baseHash = BaseVolumeHash
	}

	if volumes.Devices[hash] != nil {
		return fmt.Errorf("hash %s already exists", hash)
	}

	baseInfo := volumes.Devices[baseHash]
	if baseInfo == nil {
		return fmt.Errorf("Unknown base hash %s", baseHash)
	}

	deviceId := volumes.allocateDeviceId()

	err := volumes.createSnapDevice(deviceId, baseInfo.DeviceId);
	if err != nil {
		return err
	}

	err = volumes.addVolume(deviceId, hash, baseInfo.Size)
	if err != nil {
		_ = volumes.deleteDevice(deviceId)
		return err
	}
	return nil
}

func (volumes *VolumeSetDM) activateVolume(hash string) error {
	info := volumes.Devices[hash]
	if info == nil {
		return fmt.Errorf("Unknown volume %s", hash)
	}

	name := volumes.getNameForDevice(info.DeviceId)
	devinfo, _ := volumes.getInfo(name)
	if devinfo != nil && devinfo.Exists != 0 {
		return nil
	}
	
	return volumes.activateDevice(info.DeviceId, info.Size)
}

func (volumes *VolumeSetDM) createSnapVolume(deviceId int, baseDeviceId int) error {
	file, err := os.OpenFile(volumes.getDevNameForDevice(baseDeviceId), os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	size, err := GetBlockDeviceSize(file)
	file.Close ()
	if err != nil {
		return fmt.Errorf("Can't get device size")
	}
	
	err = volumes.createSnapDevice(deviceId, baseDeviceId);
	if err != nil {
		return err
	}

	return volumes.activateDevice(deviceId, size)
}

func (volumes *VolumeSetDM) createFilesystem(deviceId int) error {
	devname := volumes.getDevNameForDevice(deviceId)
	
	err := exec.Command("mkfs.ext4", "-E",
		"discard,lazy_itable_init=0,lazy_journal_init=0", devname).Run()
	if err != nil {
		return err
	}
	return nil
}

func (volumes *VolumeSetDM) loadMetaData() error {
	jsonData, err := ioutil.ReadFile(volumes.jsonFile())
	if err != nil {
		return err
	}

	metadata := &MetaData {}
	if err := json.Unmarshal(jsonData, metadata); err != nil {
		return err
	}
	volumes.MetaData = *metadata

	for _, d := range volumes.Devices {
		if d.DeviceId >= volumes.nextFreeDevice {
			volumes.nextFreeDevice = d.DeviceId + 1
		}
	}

	return nil
}

func (volumes *VolumeSetDM) createBaseLayer(dir string) error {
	for pth, typ := range map[string]string{
		"/dev/pts":         "dir",
		"/dev/shm":         "dir",
		"/proc":            "dir",
		"/sys":             "dir",
		"/.dockerinit":     "file",
		"/etc/resolv.conf": "file",
		// "var/run": "dir",
		// "var/lock": "dir",
	} {
		if _, err := os.Stat(path.Join(dir, pth)); err != nil {
			if os.IsNotExist(err) {
				switch typ {
				case "dir":
					if err := os.MkdirAll(path.Join(dir, pth), 0755); err != nil {
						return err
					}
				case "file":
					if err := os.MkdirAll(path.Join(dir, path.Dir(pth)), 0755); err != nil {
						return err
					}

					if f, err := os.OpenFile(path.Join(dir, pth), os.O_CREATE, 0755); err != nil {
						return err
					} else {
						f.Close()
					}
				}
			} else {
				return err
			}
		}
	}
	return nil
}


func (volumes *VolumeSetDM) initDevmapper() error {
	info, err := volumes.getInfo(volumes.getPoolName())
	if info == nil {
		return err
	}

	if info.Exists != 0 {
		/* Pool exists, assume everything is up */
		return volumes.loadMetaData()
	}

	doInit := false
	if !volumes.hasImage("data") || !volumes.hasImage("metadata") {
		/* If we create the loopback mounts we also need to initialize the base fs */
		doInit = true
	}
	fmt.Println("doInit:", doInit)

	data, err := volumes.ensureImage("data", defaultDataLoopbackSize)
	if err != nil {
		return err
	}

	metadata, err := volumes.ensureImage("metadata", defaultMetaDataLoopbackSize)
	if err != nil {
		return err
	}

	dataFile, err := AttachLoopDevice(data)
	if err != nil {
		return err
	}
	defer dataFile.Close()

	metadataFile, err := AttachLoopDevice(metadata)
	if err != nil {
		return err
	}
	defer metadataFile.Close()

	err = volumes.createPool(dataFile, metadataFile);
	if err != nil {
		return err
	}

	if (doInit) {
		// TODO: Tear down pool and remove images on failure
		log.Printf("Initializing base device-manager snapshot")

		volumes.Devices = make(map[string]*VolumeInfo)

		id := volumes.allocateDeviceId()
	
		// Create initial volume
		err := volumes.createDevice(id)
		if err != nil {
			return err
		}
		
		err = volumes.addVolume(id, BaseVolumeHash, defaultBaseFsSize)
		if err != nil {
			_ = volumes.deleteDevice (id)
			return err
		}

		log.Printf("Creating filesystem on base device-manager snapshot")
		err = volumes.activateVolume(BaseVolumeHash)
		if err != nil {
			return err
		}

		err = volumes.createFilesystem(id)
		if err != nil {
			return err
		}

		tmpDir := path.Join(volumes.loopbackDir(), "basefs")
		if err = os.MkdirAll(tmpDir, 0700); err != nil && !os.IsExist(err) {
			return err
		}

		err = volumes.MountVolume(BaseVolumeHash, tmpDir)
		if err != nil {
			return err
		}

		err = volumes.createBaseLayer(tmpDir)
		if err != nil {
			_ = syscall.Unmount(tmpDir, 0)
			return err
		}

		err = syscall.Unmount(tmpDir, 0)
		if err != nil {
			return err
		}

		_ = os.Remove (tmpDir)

		// TODO: Fsync loopback devices
	} else {
		err = volumes.loadMetaData()
		if err != nil {
			return err
		}
	}

	return nil
}

func (volumes *VolumeSetDM) MountVolume(hash, path string) error {
	if (hash == "") {
		hash = BaseVolumeHash
	}

	err := volumes.activateVolume(hash)
	if err != nil {
		return err
	}

	info := volumes.Devices[hash]

	// TODO: Call mount without shelling out
	err = exec.Command("mount", "-o", "discard", volumes.getDevNameForDevice(info.DeviceId), path).Run()
	if err != nil {
		return err
	}
	return nil
}

func (volumes *VolumeSetDM) HasVolume(hash string) bool {
	if (hash == "") {
		hash = BaseVolumeHash
	}

	info := volumes.Devices[hash]
	return info != nil
}

func NewVolumeSetDM(root string) (*VolumeSetDM, error) {
	SetDevDir("/dev");
	volumes := &VolumeSetDM{
		root:    root,
	}
	err := volumes.initDevmapper()
	if err != nil {
		return nil, err
	}
	return volumes, nil
}
