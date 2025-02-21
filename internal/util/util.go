package util

import (
	"log"
	"os"
	"path/filepath"
)

func GetProjectDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("could not get current path:", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if dir == "/" {
			log.Fatal("could not find project root path:", err)
		}
		dir = filepath.Dir(dir)
	}
}
