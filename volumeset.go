package docker

type VolumeSet interface {
	AddVolume(hash, baseHash string) error
	MountVolume(hash, path string) error
	HasVolume(hash string) bool
}

type VolumeSetFactory func(string) (VolumeSet, error)
