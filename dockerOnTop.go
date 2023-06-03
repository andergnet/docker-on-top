package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// internalError wraps the given error in the "docker-on-top internal error: #{help}: #{err}" message. It is useful for
// when the error is reported to the docker daemon so that the end user knows it's not their mistake but an internal
// error.
func internalError(help string, err error) error {
	// Maybe make a custom error type instead?
	return fmt.Errorf("docker-on-top internal error: %s: %w", help, err)
}

// DockerOnTop contains internal data of the docker-on-top volume driver and implements the `volume.Driver` interface
// (from docker's go-plugins-helpers).
//
// **MUST BE** created with `NewDockerOnTop` only.
type DockerOnTop struct {
	// dotRootDir is the base directory of docker-on-top, where all the internal information is stored.
	// Must contain a trailing slash.
	dotRootDir string
}

// NewDockerOnTop creates a new `DockerOnTop` object using the given directory as the dot root directory. If it doesn't
// exist, it is created as in `mkdir -p`. On error, it is returned and `DockerOnTop` is not created.
func NewDockerOnTop(dotRootDir string) (*DockerOnTop, error) {
	if len(dotRootDir) == 0 {
		return nil, errors.New("`dotRootDir` cannot be empty")
	}

	if dotRootDir[len(dotRootDir)-1] != '/' {
		dotRootDir += "/"
	}

	err := os.MkdirAll(dotRootDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dotRootDir)
	if err != nil {
		return nil, err
	}

	dot := DockerOnTop{dotRootDir: dotRootDir}

	for _, entry := range entries {
		volumeName := entry.Name()
		resetErr := dot.volumeTreeOnBootReset(volumeName)
		if resetErr == nil {
			log.Debugf("Successfully reset volume %s on boot", volumeName)
		} else if errors.Is(resetErr, syscall.EBUSY) {
			log.Infof("Detected an overlay mounted for volume %s. Skipped active mounts reset for it",
				volumeName)
		} else {
			log.Errorf("Failed to reset volume %s on boot: %v", volumeName, resetErr)
			return nil, resetErr
		}
	}

	return &dot, nil
}

// MustNewDockerOnTop behaves as `NewDockerOnTop` but panics in case of an error
func MustNewDockerOnTop(baseDir string) *DockerOnTop {
	driver, err := NewDockerOnTop(baseDir)
	if err != nil {
		panic(fmt.Errorf("the call NewDockerOnTop(%+v) failed: %v", baseDir, err))
	}
	return driver
}
