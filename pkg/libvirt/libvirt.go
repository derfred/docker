// +build linux

package libvirt

/*
#cgo LDFLAGS: -lvirt
#include <stdlib.h>
#include <libvirt/libvirt.h>
#include <libvirt/virterror.h>
#include <string.h>

static void vir_error_func(void *userData, virErrorPtr error)
{

}

static virErrorFunc vir_error_func_ptr() { return vir_error_func; }

*/
import "C"

import (
	"fmt"
	"unsafe"
)

type Connection struct {
	ptr C.virConnectPtr
}

type Domain struct {
	ptr C.virDomainPtr
}

func (dom *Domain) Destroy() error {
	ret := C.virDomainDestroy(dom.ptr)
	if ret == -1 {
		return libvirtError("virDomainDestroy")
	}
	return nil
}

func (dom *Domain) GetId() (uint32, error) {
	libvirtPid := C.virDomainGetID(dom.ptr)
	if C.int(libvirtPid) == -1 {
		return 0, libvirtError("virDomainGetID")
	}
	return uint32(libvirtPid), nil
}

func (dom *Domain) Free() {
	C.virDomainFree(dom.ptr)
}

func Init() {
	// Register a no-op error handling function with libvirt so that it
	// won't print to stderr
	C.virSetErrorFunc(nil, C.vir_error_func_ptr())
}

func Connect() (*Connection, error) {
	uri := C.CString("lxc:///")
	defer C.free(unsafe.Pointer(uri))
	conn := C.virConnectOpenAuth(uri, C.virConnectAuthPtrDefault, 0)
	if conn == nil {
		return nil, libvirtError("virConnectOpenAuth")
	}
	return &Connection{ptr: conn}, nil
}

func (conn *Connection) Close() {
	C.virConnectClose(conn.ptr)
}

func (conn *Connection) Version() string {
	var version C.ulong
	ret := C.virConnectGetLibVersion(conn.ptr, &version)
	if ret == -1 {
		return "unknown"
	} else {
		major := version / 1000000
		version = version % 1000000
		minor := version / 1000
		rel := version % 1000
		return fmt.Sprintf("%d.%d.%d", major, minor, rel)
	}
}

func (conn *Connection) DomainCreateXML(xml string) (*Domain, error) {
	xmlC := C.CString(xml)
	defer C.free(unsafe.Pointer(xmlC))
	domain := C.virDomainCreateXML(conn.ptr, xmlC, 0)
	if domain == nil {
		return nil, libvirtError("virDomainCreateXML")
	}
	return &Domain{ptr: domain}, nil
}

func (conn *Connection) DomainLookupByName(name string) (*Domain, error) {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	domain := C.virDomainLookupByName(conn.ptr, nameC)
	if domain == nil {
		return nil, libvirtError("virDomainLookupByName")
	}
	return &Domain{ptr: domain}, nil
}

func libvirtError(str string) error {
	lastError := C.virGetLastError()

	// There's no virGetLastErrorMessage() in RHEL 6, so implement it here
	// for maximum compatibility.
	var libvirtErrorStr string
	if lastError == nil || lastError.code == C.VIR_ERR_OK {
		libvirtErrorStr = "no error"
	} else if lastError.message == nil {
		libvirtErrorStr = "unknown error"
	} else {
		libvirtErrorStr = C.GoString(lastError.message)
	}

	return fmt.Errorf(str + ": " + libvirtErrorStr)
}
