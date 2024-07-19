package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/docker/go-plugins-helpers/volume"
)

type ActiveMount struct {
	UsageCount int
}

func (d *DockerOnTop) Activate(request *volume.MountRequest, activemountsdir lockedFile) (bool, error) {
	var activeMount ActiveMount
	var result bool

	_, readDirErr := activemountsdir.ReadDir(1) // Check if there are any files inside activemounts dir
	if readDirErr == nil {
		// There is something no need to mount the filesystem again
		result = false
	} else if errors.Is(readDirErr, io.EOF) {
		// The directory is empty, mount the filesystem
		result = true
	} else {
		log.Errorf("Failed to list the activemounts directory: %v", readDirErr)
		return false, fmt.Errorf("failed to list activemounts/ %v", readDirErr)
	}

	activemountFilePath := d.activemountsdir(request.Name) + request.ID
	file, err := os.Open(activemountFilePath)

	if err == nil {
		// The file can exist from a previous mount when doing a docker cp on an already mounted container, no need to mount the filesystem again
		payload, _ := io.ReadAll(file)
		json.Unmarshal(payload, &activeMount)
		file.Close()
	} else if os.IsNotExist(err) {
		// Default case, we need to create a new active mount, the filesystem needs to be mounted
		activeMount = ActiveMount{UsageCount: 0}
	} else {
		log.Errorf("Active mount file %s exists but cannot be read.", activemountFilePath)
		return false, fmt.Errorf("active mount file %s exists but cannot be read", activemountFilePath)
	}

	activeMount.UsageCount++

	payload, _ := json.Marshal(activeMount)
	err = os.WriteFile(activemountFilePath, payload, 0o666)
	if err != nil {
		log.Errorf("Active mount file %s cannot be written.", activemountFilePath)
		return false, fmt.Errorf("active mount file %s cannot be written", activemountFilePath)
	}

	return result, nil
}

func (d *DockerOnTop) Deactivate(request *volume.UnmountRequest, activemountsdir lockedFile) (bool, error) {

	dirEntries, readDirErr := activemountsdir.ReadDir(2) // Check if there is any _other_ container using the volume
	if errors.Is(readDirErr, io.EOF) {
		// If directory is empty, unmount overlay and clean up
		log.Errorf("There are no active mount files and one was expected. Unmounting.")
		return true, fmt.Errorf("there are no active mount files and one was expected. Unmounting")
	} else if readDirErr != nil {
		log.Errorf("Failed to list the activemounts directory: %v", readDirErr)
		return false, fmt.Errorf("failed to list activemounts/ %v", readDirErr)
	}

	var activeMount ActiveMount
	activemountFilePath := d.activemountsdir(request.Name) + request.ID

	otherVolumesPresent := len(dirEntries) > 1 || dirEntries[0].Name() != request.ID

	file, err := os.Open(activemountFilePath)

	if err == nil {
		payload, _ := io.ReadAll(file)
		json.Unmarshal(payload, &activeMount)
		file.Close()
	} else if os.IsNotExist(err) {
		log.Errorf("The active mount file %s was expected but is not there", activemountFilePath)
		return !otherVolumesPresent, fmt.Errorf("the active mount file %s was expected but is not there", activemountFilePath)
	} else {
		log.Errorf("The active mount file %s could not be opened", activemountFilePath)
		return false, fmt.Errorf("the active mount file %s could not be opened", activemountFilePath)
	}

	activeMount.UsageCount--

	if activeMount.UsageCount == 0 {
		err := os.Remove(activemountFilePath)
		if err != nil {
			log.Errorf("The active mount file %s could not be deleted", activemountFilePath)
			return false, fmt.Errorf("the active mount file %s could not be deleted", activemountFilePath)
		}
		return !otherVolumesPresent, nil
	} else {
		payload, _ := json.Marshal(activeMount)
		err = os.WriteFile(activemountFilePath, payload, 0o666)
		if err != nil {
			log.Errorf("The active mount file %s could not be updated", activemountFilePath)
			return false, fmt.Errorf("the active mount file %s could not be updated", activemountFilePath)
		}
		return false, nil
	}
}
