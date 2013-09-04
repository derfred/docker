package devmapper

import (
	"fmt"
	"os"
	"io"
	"io/ioutil"
	"os/exec"
	"encoding/json"
	"path"
	"path/filepath"
	"log"
	"syscall"
)

const defaultDataLoopbackSize int64 = 100*1024*1024*1024
const defaultMetaDataLoopbackSize int64 = 2*1024*1024*1024
const defaultBaseFsSize uint64 = 10*1024*1024*1024

type VolumeInfo struct {
	Hash string `json:-`
	DeviceId int `json:"device-id"`
	Size uint64 `json:size`
	TransactionId uint64 `json:transaction-id`
	Initialized bool `json:initialized`
}

type MetaData struct {
	Devices map[string]*VolumeInfo `json:devices`
}

type VolumeSetDM struct {
	root string
	MetaData
	TransactionId uint64
	NewTransactionId uint64
	nextFreeDevice int
}

func getDevName(name string) string {
	return  "/dev/mapper/" + name
}

func (info *VolumeInfo) Name() string {
	hash := info.Hash
	if hash == "" {
		hash = "base"
	}
	return fmt.Sprintf("docker-%s", hash)
}

func (info *VolumeInfo) DevName() string {
	return getDevName(info.Name())
}

func (volumes *VolumeSetDM) loopbackDir() string {
	return path.Join(volumes.root, "loopback")
}

func (volumes *VolumeSetDM) jsonFile() string {
	return path.Join(volumes.loopbackDir(), "json")
}

func (volumes *VolumeSetDM) getPoolName() string {
	return "docker-pool"
}

func (volumes *VolumeSetDM) getPoolDevName() string {
	return getDevName(volumes.getPoolName())
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

func (volumes *VolumeSetDM) getStatus(name string) (uint64, uint64, string, string, error) {
	task, err := volumes.createTask(DeviceStatus, name)
	if task == nil {
		return 0, 0, "", "", err
	}
	err = task.Run()
	if err != nil {
		return 0, 0, "", "", err
	}

	devinfo, err := task.GetInfo()
	if err != nil {
		return 0, 0, "", "", err
	}
	if devinfo.Exists == 0 {
		return 0, 0, "", "", fmt.Errorf("Non existing device %s", name)
	}

	var next uintptr = 0
	next, start, length, target_type, params := task.GetNextTarget(next)

	return start, length, target_type, params, nil
}

func (volumes *VolumeSetDM) setTransactionId(oldId uint64, newId uint64) error {
	task, err := volumes.createTask(DeviceTargetMsg, volumes.getPoolDevName())
	if task == nil {
		return err
	}

	err = task.SetSector(0)
	if err != nil {
		return fmt.Errorf("Can't set sector")
	}

	message := fmt.Sprintf("set_transaction_id %d %d", oldId, newId)
	err = task.SetMessage(message)
	if err != nil {
		return fmt.Errorf("Can't set message")
	}

	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running setTransactionId")
	}
	return nil
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

func (volumes *VolumeSetDM) suspendDevice(info *VolumeInfo) error {
	task, err := volumes.createTask(DeviceSuspend, info.Name())
	if task == nil {
		return err
	}
	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running DeviceSuspend")
	}
	return nil
}

func (volumes *VolumeSetDM) resumeDevice(info *VolumeInfo) error {
	task, err := volumes.createTask(DeviceResume, info.Name())
	if task == nil {
		return err
	}

	var cookie uint32 = 0
	err = task.SetCookie (&cookie, 32)
	if err != nil {
		return fmt.Errorf("Can't set cookie")
	}

	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running DeviceSuspend")
	}

	UdevWait(cookie)

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

func (volumes *VolumeSetDM) createSnapDevice(deviceId int, baseInfo *VolumeInfo) error {
	err := volumes.suspendDevice(baseInfo)
	if err != nil {
		return err
	}

	task, err := volumes.createTask(DeviceTargetMsg, volumes.getPoolDevName())
	if task == nil {
		_ = volumes.resumeDevice(baseInfo)
		return err
	}
	err = task.SetSector(0)
	if err != nil {
		_ = volumes.resumeDevice(baseInfo)
		return fmt.Errorf("Can't set sector")
	}

	message := fmt.Sprintf("create_snap %d %d", deviceId, baseInfo.DeviceId)
	err = task.SetMessage(message)
	if err != nil {
		_ = volumes.resumeDevice(baseInfo)
		return fmt.Errorf("Can't set message")
	}

	err = task.Run()
	if err != nil {
		_ = volumes.resumeDevice(baseInfo)
		return fmt.Errorf("Error running DeviceCreate")
	}

	err = volumes.resumeDevice(baseInfo)
	if err != nil {
		return err
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

func (volumes *VolumeSetDM) removeDevice(info *VolumeInfo) error {
	task, err := volumes.createTask(DeviceRemove, info.Name())
	if task == nil {
		return err
	}
	err = task.Run()
	if err != nil {
		return fmt.Errorf("Error running removeDevice")
	}
	return nil
}

func (volumes *VolumeSetDM) activateDevice(info *VolumeInfo) error {
	task, err := volumes.createTask(DeviceCreate, info.Name())
	if task == nil {
		return err
	}

	params := fmt.Sprintf("%s %d", volumes.getPoolDevName(),info.DeviceId)
	err = task.AddTarget (0, info.Size / 512, "thin", params)
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

func (volumes *VolumeSetDM) allocateTransactionId() uint64 {
	volumes.NewTransactionId = volumes.NewTransactionId + 1
	return volumes.NewTransactionId
}


func (volumes *VolumeSetDM) saveMetadata() (error) {
	jsonData, err := json.Marshal(volumes.MetaData)
	if err != nil {
		return err
	}
	tmpFile, err := ioutil.TempFile(filepath.Dir(volumes.jsonFile()), ".json")
	if err != nil {
		return err
	}

	n, err := tmpFile.Write(jsonData)
	if err != nil {
		return err
	}
	if n < len(jsonData) {
		err = io.ErrShortWrite
	}
	err = tmpFile.Sync()
	if err != nil {
		return err
	}
	err = tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(tmpFile.Name(), volumes.jsonFile())
	if err != nil {
		return err
	}

	if volumes.NewTransactionId != volumes.TransactionId {
		err = volumes.setTransactionId(volumes.TransactionId, volumes.NewTransactionId)
		if err != nil {
			return err
		}
		volumes.TransactionId = volumes.NewTransactionId
	}

	return nil
}

func (volumes *VolumeSetDM) registerVolume(id int, hash string, size uint64) (*VolumeInfo, error) {
	transaction := volumes.allocateTransactionId()

	info := &VolumeInfo{
		Hash: hash,
		DeviceId: id,
		Size: size,
		TransactionId: transaction,
		Initialized: false,
	}

	volumes.Devices[hash] = info
	err := volumes.saveMetadata()
	if err != nil {
		// Try to remove unused device
		volumes.Devices[hash] = nil
		return nil, err
	}

	return info, nil
}

func (volumes *VolumeSetDM) activateVolume(hash string) error {
	info := volumes.Devices[hash]
	if info == nil {
		return fmt.Errorf("Unknown volume %s", hash)
	}

	name := info.Name()
	devinfo, _ := volumes.getInfo(name)
	if devinfo != nil && devinfo.Exists != 0 {
		return nil
	}

	return volumes.activateDevice(info)
}

func (volumes *VolumeSetDM) createFilesystem(info *VolumeInfo) error {
	devname := info.DevName()

	err := exec.Command("mkfs.ext4", "-E",
		"discard,lazy_itable_init=0,lazy_journal_init=0", devname).Run()
	if err != nil {
		return err
	}
	return nil
}

func (volumes *VolumeSetDM) loadMetaData() error {
	_, _, _, params, err := volumes.getStatus(volumes.getPoolName())
	if err != nil {
		return err
	}
	var currentTransaction uint64
	_, err = fmt.Sscanf(params, "%d", &currentTransaction)
	if err != nil {
		return err
	}

	volumes.TransactionId = currentTransaction
	volumes.NewTransactionId = volumes.TransactionId

	jsonData, err := ioutil.ReadFile(volumes.jsonFile())
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	metadata := &MetaData {
		Devices: make(map[string]*VolumeInfo),
	}
	if jsonData != nil {
		if err := json.Unmarshal(jsonData, metadata); err != nil {
			return err
		}
	}
	volumes.MetaData = *metadata

	for hash, d := range volumes.Devices {
		if d.DeviceId >= volumes.nextFreeDevice {
			volumes.nextFreeDevice = d.DeviceId + 1
		}

		// If the transaction id is larger than the actual one we lost the volume due to some crash
		if d.TransactionId > currentTransaction {
			log.Printf("Removing lost volume %s with id %d", hash, d.TransactionId)
			delete(volumes.Devices, hash)
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

func (volumes *VolumeSetDM) setupBaseImage() error {
	oldInfo := volumes.Devices[""]
	if oldInfo != nil && oldInfo.Initialized {
		return nil
	}

	if oldInfo != nil && !oldInfo.Initialized {
		log.Printf("Removing uninitialized base image")
		if err := volumes.RemoveVolume(""); err != nil {
			return err
		}
	}

	log.Printf("Initializing base device-manager snapshot")

	id := volumes.allocateDeviceId()

	// Create initial volume
	err := volumes.createDevice(id)
	if err != nil {
		return err
	}

	info, err := volumes.registerVolume(id, "", defaultBaseFsSize)
	if err != nil {
		_ = volumes.deleteDevice (id)
		return err
	}

	log.Printf("Creating filesystem on base device-manager snapshot")

	err = volumes.activateVolume("")
	if err != nil {
		return err
	}

	err = volumes.createFilesystem(info)
	if err != nil {
		return err
	}

	tmpDir := path.Join(volumes.loopbackDir(), "basefs")
	if err = os.MkdirAll(tmpDir, 0700); err != nil && !os.IsExist(err) {
		return err
	}

	err = volumes.MountVolume("", tmpDir)
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

	info.Initialized = true

	err = volumes.saveMetadata()
	if err != nil {
		info.Initialized = false
		return err
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
		err = volumes.loadMetaData()
		if err != nil {
			return err
		}
		err = volumes.setupBaseImage()
		if err != nil {
			return err
		}
		return nil
	}

	createdLoopback := false
	if !volumes.hasImage("data") || !volumes.hasImage("metadata") {
		/* If we create the loopback mounts we also need to initialize the base fs */
		createdLoopback = true
	}

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

	if (!createdLoopback) {
		err = volumes.loadMetaData()
		if err != nil {
			return err
		}
	}

	err = volumes.setupBaseImage()
	if err != nil {
		return err
	}

	return nil
}

func (volumes *VolumeSetDM) AddVolume(hash, baseHash string) error {
	if volumes.Devices[hash] != nil {
		return fmt.Errorf("hash %s already exists", hash)
	}

	baseInfo := volumes.Devices[baseHash]
	if baseInfo == nil {
		return fmt.Errorf("Unknown base hash %s", baseHash)
	}

	deviceId := volumes.allocateDeviceId()

	err := volumes.createSnapDevice(deviceId, baseInfo);
	if err != nil {
		return err
	}

	_, err = volumes.registerVolume(deviceId, hash, baseInfo.Size)
	if err != nil {
		_ = volumes.deleteDevice(deviceId)
		return err
	}
	return nil
}

func (volumes *VolumeSetDM) RemoveVolume(hash string) error {
	info := volumes.Devices[hash]
	if info == nil {
		return fmt.Errorf("hash %s doesn't exists", hash)
	}

	devinfo, _ := volumes.getInfo(info.Name())
	if devinfo != nil && devinfo.Exists != 0 {
		err := volumes.removeDevice(info)
		if err != nil {
			return err
		}
	}

	if info.Initialized {
		info.Initialized = false
		err := volumes.saveMetadata()
		if err != nil {
			return err
		}
	}

	err := volumes.deleteDevice(info.DeviceId)
	if err != nil {
		return err
	}

	_ = volumes.allocateTransactionId()
	delete(volumes.Devices, info.Hash)

	err = volumes.saveMetadata()
	if err != nil {
		volumes.Devices[info.Hash] = info
		return err
	}

	return nil
}

func (volumes *VolumeSetDM) MountVolume(hash, path string) error {
	err := volumes.activateVolume(hash)
	if err != nil {
		return err
	}

	info := volumes.Devices[hash]

	err = syscall.Mount(info.DevName(), path, "ext4", syscall.MS_MGC_VAL, "discard")
	if err != nil {
		return err
	}
	return nil
}

func (volumes *VolumeSetDM) HasVolume(hash string) bool {
	info := volumes.Devices[hash]
	return info != nil
}

func (volumes *VolumeSetDM) HasInitializedVolume(hash string) bool {
	info := volumes.Devices[hash]
	return info != nil && info.Initialized
}

func (volumes *VolumeSetDM) SetInitialized(hash string) error {
	info := volumes.Devices[hash]
	if info == nil {
		return fmt.Errorf("Unknown volume %s", hash)
	}

	info.Initialized = true
	err := volumes.saveMetadata()
	if err != nil {
		info.Initialized = false
		return err
	}

	return nil
}

func NewVolumeSetDM(root string) (*VolumeSetDM, error) {
	SetDevDir("/dev");
	volumes := &VolumeSetDM{
		root:    root,
	}
	volumes.Devices = make(map[string]*VolumeInfo)

	err := volumes.initDevmapper()
	if err != nil {
		return nil, err
	}
	return volumes, nil
}
