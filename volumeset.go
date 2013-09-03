package docker

type VolumeSet interface {
	AddVolume(hash, baseHash string) error
	SetInitialized(hash string) error
	RemoveVolume(hash string) error
	MountVolume(hash, path string) error
	HasVolume(hash string) bool
	HasInitializedVolume(hash string) bool
}

type VolumeSetFactory func(string) (VolumeSet, error)
