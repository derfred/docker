package main

import (
	"flag"
	"fmt"
	"github.com/dotcloud/docker"
	"github.com/dotcloud/docker/utils"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	GITCOMMIT string
	VERSION   string
)

func main() {
	if selfPath := utils.SelfPath(); selfPath == "/sbin/init" || selfPath == "/.dockerinit" {
		// Running in init mode
		docker.SysInit()
		return
	}
	// FIXME: Switch d and D ? (to be more sshd like)
	flVersion := flag.Bool("v", false, "Print version information and quit")
	flDebug := flag.Bool("D", false, "Debug mode")
	bridgeName := flag.String("b", "", "Attach containers to a pre-existing network bridge. Use 'none' to disable container networking")
	flHosts := docker.ListOpts{fmt.Sprintf("unix://%s", docker.DEFAULTUNIXSOCKET)}
	flag.Var(&flHosts, "H", "tcp://host:port to bind/connect to or unix://path/to/socket to use")
	flag.Parse()
	if *flVersion {
		showVersion()
		return
	}
	if len(flHosts) > 1 {
		flHosts = flHosts[1:] //trick to display a nice default value in the usage
	}
	for i, flHost := range flHosts {
		flHosts[i] = utils.ParseHost(docker.DEFAULTHTTPHOST, docker.DEFAULTHTTPPORT, flHost)
	}

	if *bridgeName != "" {
		docker.NetworkBridgeIface = *bridgeName
	} else {
		docker.NetworkBridgeIface = docker.DefaultNetworkBridge
	}
	if *flDebug {
		os.Setenv("DEBUG", "1")
	}
	docker.GITCOMMIT = GITCOMMIT
	docker.VERSION = VERSION
	if len(flHosts) > 1 {
		log.Fatal("Please specify only one -H")
		return
	}
	protoAddrParts := strings.SplitN(flHosts[0], "://", 2)
	if err := docker.ParseCommands(protoAddrParts[0], protoAddrParts[1], flag.Args()...); err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
}

func showVersion() {
	fmt.Printf("Docker version %s, build %s\n", VERSION, GITCOMMIT)
}

func createPidFile(pidfile string) error {
	if pidString, err := ioutil.ReadFile(pidfile); err == nil {
		pid, err := strconv.Atoi(string(pidString))
		if err == nil {
			if _, err := os.Stat(fmt.Sprintf("/proc/%d/", pid)); err == nil {
				return fmt.Errorf("pid file found, ensure docker is not running or delete %s", pidfile)
			}
		}
	}

	file, err := os.Create(pidfile)
	if err != nil {
		return err
	}

	defer file.Close()

	_, err = fmt.Fprintf(file, "%d", os.Getpid())
	return err
}

func removePidFile(pidfile string) {
	if err := os.Remove(pidfile); err != nil {
		log.Printf("Error removing %s: %s", pidfile, err)
	}
}
