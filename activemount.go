package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/docker/go-plugins-helpers/volume"
)

type activeMount struct {
	UsageCount int
}

// activateVolume checks if the volume that has been requested to be mounted (as in docker volume mounting)
// actually requires to be mounted (as an overlay fs mount). For that purpose check if other containers
// have already mounted the volume (by reading in `activemountsdir`). It is also possible that the volume
// has already been been mounted by the same container (when doing a `docker cp` while the container is running),
// in that case the file named with the `request.ID` will contain the number of times the container has
// been requested the volume to be mounted. That number will be increased each time `activateVolume` is
// called and decreased on `deactivateVolume`.
// Parameters:
//
//	request: Incoming Docker mount request.
//	activemountsdir: Folder where Docker-On-Top mounts are tracked.
//
// Return:
//
//	mount: Returns true if the request requires the filesystem to be mounted, false if not.
//	err: If the function encountered an error, the error itself, nil if everything went right.
func (d *DockerOnTop) activateVolume(request *volume.MountRequest, activemountsdir lockedFile) (bool, error) {
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

	var activeMountInfo activeMount
	activemountFilePath := d.activemountsdir(request.Name) + request.ID
	file, err := os.Open(activemountFilePath)

	if err == nil {
		// The file can exist from a previous mount when doing a docker cp on an already mounted container, no need to mount the filesystem again
		payload, _ := io.ReadAll(file)
		json.Unmarshal(payload, &activeMountInfo)
		file.Close()
	} else if os.IsNotExist(err) {
		// Default case, we need to create a new active mount, the filesystem needs to be mounted
		activeMountInfo = activeMount{UsageCount: 0}
	} else {
		log.Errorf("Active mount file %s exists but cannot be read.", activemountFilePath)
		return false, fmt.Errorf("active mount file %s exists but cannot be read", activemountFilePath)
	}

	activeMountInfo.UsageCount++

	// Convert activeMountInfo to JSON to store it in a file. We can safely ignore Marshal errors, since the
	// activeMount structure is simple enought not to contain "strage" floats, unsupported datatypes or cycles
	// which are the error causes for json.Marshal
	payload, _ := json.Marshal(activeMountInfo)
	err = os.WriteFile(activemountFilePath, payload, 0o666)
	if err != nil {
		log.Errorf("Active mount file %s cannot be written.", activemountFilePath)
		return false, fmt.Errorf("active mount file %s cannot be written", activemountFilePath)
	}

	return result, nil
}

// deactivateVolume checks if the volume that has been requested to be unmounted (as in docker volume unmounting)
// actually requires to be unmounted (as an overlay fs unmount). It will check the number of times the container
// has been requested to mount the volume in the file named `request.ID` and decrease the number, when the number
// reaches zero it will delete the `request.ID` file since this container no longer mounts the volume. It will
// also check if other containers are mounting this volume by checking for other files in the active mounts folder.
// Parameters:
//
//	request: Incoming Docker unmount request.
//	activemountsdir: Folder where Docker-On-Top mounts are tracked.
//
// Return:
//
//	unmount: Returns true if there are not other usages of this volume and the filesystem can be unmounted.
//	err: If the function encountered an error, the error itself, nil if everything went right.
func (d *DockerOnTop) DeactivateVolume(request *volume.UnmountRequest, activemountsdir lockedFile) (bool, error) {

	dirEntries, readDirErr := activemountsdir.ReadDir(2) // Check if there is any _other_ container using the volume
	if errors.Is(readDirErr, io.EOF) {
		// If directory is empty, unmount overlay and clean up
		log.Errorf("There are no active mount files and one was expected. Unmounting.")
		return true, fmt.Errorf("there are no active mount files and one was expected. Unmounting")
	} else if readDirErr != nil {
		log.Errorf("Failed to list the activemounts directory: %v", readDirErr)
		return false, fmt.Errorf("failed to list activemounts/ %v", readDirErr)
	}

	otherVolumesPresent := len(dirEntries) > 1 || dirEntries[0].Name() != request.ID
	var activeMountInfo activeMount

	activemountFilePath := d.activemountsdir(request.Name) + request.ID
	file, err := os.Open(activemountFilePath)

	if err == nil {
		payload, _ := io.ReadAll(file)
		json.Unmarshal(payload, &activeMountInfo)
		file.Close()
	} else if os.IsNotExist(err) {
		log.Errorf("The active mount file %s was expected but is not there", activemountFilePath)
		return !otherVolumesPresent, fmt.Errorf("the active mount file %s was expected but is not there", activemountFilePath)
	} else {
		log.Errorf("The active mount file %s could not be opened", activemountFilePath)
		return false, fmt.Errorf("the active mount file %s could not be opened", activemountFilePath)
	}

	activeMountInfo.UsageCount--

	if activeMountInfo.UsageCount == 0 {
		err := os.Remove(activemountFilePath)
		if err != nil {
			log.Errorf("The active mount file %s could not be deleted", activemountFilePath)
			return false, fmt.Errorf("the active mount file %s could not be deleted", activemountFilePath)
		}
		return !otherVolumesPresent, nil
	} else {
		// Convert activeMountInfo to JSON to store it in a file. We can safely ignore Marshal errors, since the
		// activeMount structure is simple enought not to contain "strage" floats, unsupported datatypes or cycles
		// which are the error causes for json.Marshal
		payload, _ := json.Marshal(activeMountInfo)
		err = os.WriteFile(activemountFilePath, payload, 0o666)
		if err != nil {
			log.Errorf("The active mount file %s could not be updated", activemountFilePath)
			return false, fmt.Errorf("the active mount file %s could not be updated", activemountFilePath)
		}
		return false, nil
	}
}
