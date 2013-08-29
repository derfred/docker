package main

import (
	"fmt"
	"os"
	"github.com/dotcloud/docker"
)

func usage() {
	fmt.Printf("Usage: %s [snap new-id base-id] | [mount id mountpoint]\n", os.Args[0])
	os.Exit(1);
}

func main() {
	volumes, err := docker.NewVolumeSet("/var/lib/docker")
	if err != nil {
		fmt.Println("Setup failed:", err)
		os.Exit(1);
	}

	if len(os.Args) < 2 {
		usage()
	}
	
	cmd := os.Args[1]
	if cmd == "snap" {
		if len(os.Args) < 4 {
			usage()
		}
		
		err = volumes.AddVolume(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println("Can't create snap volume: ", err)
			os.Exit(1);
		}
	} else if cmd == "mount" {
		if len(os.Args) < 4 {
			usage()
		}
		
		err = volumes.MountVolume(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println("Can't create snap volume: ", err)
			os.Exit(1);
		}
	} else {
		fmt.Printf("Unknown command %s\n", cmd)
		if len(os.Args) < 4 {
			usage()
		}
		
		os.Exit(1);
	}
	
	return
}
