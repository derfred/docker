// +build linux

package overlayfs

import (
	"fmt"
	"github.com/dotcloud/docker/archive"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

type CopyFlags int

const (
	CopyHardlink CopyFlags = 1 << iota
)

func copyRegular(srcPath, dstPath string, mode os.FileMode) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)

	return err
}

func copyXattr(srcPath, dstPath, attr string) error {
	data, err := Lgetxattr(srcPath, attr)
	if err != nil {
		return err
	}
	if data != nil {
		if err := Lsetxattr(dstPath, attr, data, 0); err != nil {
			return err
		}
	}
	return nil
}

func copyDir(srcDir, dstDir string, flags CopyFlags) error {
	err := filepath.Walk(srcDir, func(srcPath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip root
		if srcDir == srcPath {
			return nil
		}

		// Rebase path
		relPath, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dstDir, relPath)
		if err != nil {
			return err
		}

		stat, ok := f.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("Unable to get raw syscall.Stat_t data for %s", srcPath)
		}

		switch f.Mode() & os.ModeType {
		case 0: // Regular file
			if flags&CopyHardlink != 0 {
				if err := os.Link(srcPath, dstPath); err != nil {
					return err
				}
			} else {
				if err := copyRegular(srcPath, dstPath, f.Mode()); err != nil {
					return err
				}
			}

		case os.ModeDir:
			if err := os.Mkdir(dstPath, f.Mode()); err != nil {
				return err
			}

		case os.ModeSymlink:
			link, err := os.Readlink(srcPath)
			if err != nil {
				return err
			}

			if err := os.Symlink(link, dstPath); err != nil {
				return err
			}

		case os.ModeNamedPipe:
			fallthrough
		case os.ModeSocket:
			if err := syscall.Mkfifo(dstPath, stat.Mode); err != nil {
				return err
			}

		case os.ModeDevice:
			if err := syscall.Mknod(dstPath, stat.Mode, int(stat.Rdev)); err != nil {
				return err
			}

		default:
			return fmt.Errorf("Unknown file type for %s\n", srcPath)
		}

		if err := os.Lchown(dstPath, int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}

		if err := copyXattr(srcPath, dstPath, "trusted.overlay.whiteout"); err != nil {
			return err
		}

		isSymlink := f.Mode()&os.ModeSymlink != 0

		// There is no LChmod, so ignore mode for symlink. Also, this
		// must happen after chown, as that can modify the file mode
		if !isSymlink {
			if err := os.Chmod(dstPath, f.Mode()); err != nil {
				return err
			}
		}

		ts := []syscall.Timespec{stat.Atim, stat.Mtim}
		// syscall.UtimesNano doesn't support a NOFOLLOW flag atm, and
		if !isSymlink {
			if err := archive.UtimesNano(dstPath, ts); err != nil {
				return err
			}
		} else {
			if err := archive.LUtimesNano(dstPath, ts); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Returns a nil slice and nil error if the xattr is not set
func Lgetxattr(path string, attr string) ([]byte, error) {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return nil, err
	}
	attrBytes, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return nil, err
	}

	dest := make([]byte, 128)
	destBytes := unsafe.Pointer(&dest[0])
	sz, _, errno := syscall.Syscall6(syscall.SYS_LGETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(destBytes), uintptr(len(dest)), 0, 0)
	if errno == syscall.ENODATA {
		return nil, nil
	}
	if errno == syscall.ERANGE {
		dest = make([]byte, sz)
		destBytes := unsafe.Pointer(&dest[0])
		sz, _, errno = syscall.Syscall6(syscall.SYS_LGETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(destBytes), uintptr(len(dest)), 0, 0)
	}
	if errno != 0 {
		return nil, errno
	}

	return dest, nil
}

var _zero uintptr

func Lsetxattr(path string, attr string, data []byte, flags int) error {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}
	attrBytes, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return err
	}
	var dataBytes unsafe.Pointer
	if len(data) > 0 {
		dataBytes = unsafe.Pointer(&data[0])
	} else {
		dataBytes = unsafe.Pointer(&_zero)
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_LSETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(dataBytes), uintptr(len(data)), uintptr(flags), 0)
	if errno != 0 {
		return errno
	}
	return nil
}
