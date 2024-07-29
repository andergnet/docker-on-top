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
//	doMountFs: Returns true if the request requires the filesystem to be mounted, false if not.
//	err: If the function encountered an error, the error itself, nil if everything went right.
func (d *DockerOnTop) activateVolume(request *volume.MountRequest, activemountsdir lockedFile) (bool, error) {
	var doMountFs bool

	_, readDirErr := activemountsdir.ReadDir(1) // Check if there are any files inside activemounts dir
	if readDirErr == nil {
		// There is something no need to mount the filesystem again
		doMountFs = false
	} else if errors.Is(readDirErr, io.EOF) {
		// The directory is empty, mount the filesystem
		doMountFs = true
	} else {
		return false, fmt.Errorf("failed to list activemounts/ %v", readDirErr)
	}

	var activeMountInfo activeMount
	activemountFilePath := d.activemountsdir(request.Name) + request.ID
	file, err := os.Open(activemountFilePath)

	if err == nil {
		// The file can exist from a previous mount when doing a docker cp on an already mounted container, no need to mount the filesystem again
		payload, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			return false, fmt.Errorf("active mount file %s has been opened but cannot be read %w", activemountFilePath, readErr)
		} else if closeErr != nil {
			return false, fmt.Errorf("active mount file %s has been opened but cannot be closed %w", activemountFilePath, readErr)
		}
		unmarshalErr := json.Unmarshal(payload, &activeMountInfo)
		if unmarshalErr != nil {
			return false, fmt.Errorf("active mount file %s contents are invalid %w", activemountFilePath, unmarshalErr)
		}
	} else if os.IsNotExist(err) {
		// Default case, we need to create a new active mount, the filesystem needs to be mounted
		activeMountInfo = activeMount{UsageCount: 0}
	} else {
		return false, fmt.Errorf("active mount file %s exists but cannot be opened %w", activemountFilePath, err)
	}

	activeMountInfo.UsageCount++

	// Convert activeMountInfo to JSON to store it in a file. We can safely ignore Marshal errors, since the
	// activeMount structure is simple enought not to contain "strage" floats, unsupported datatypes or cycles
	// which are the error causes for json.Marshal
	payload, _ := json.Marshal(activeMountInfo)
	err = os.WriteFile(activemountFilePath, payload, 0o644)
	if err != nil {
		return false, fmt.Errorf("active mount file %s cannot be written %w", activemountFilePath, err)
	}

	return doMountFs, nil
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
//	doUnmountFs: Returns true if there are not other usages of this volume and the filesystem can be unmounted.
//	err: If the function encountered an error, the error itself, nil if everything went right.
func (d *DockerOnTop) deactivateVolume(request *volume.UnmountRequest, activemountsdir lockedFile) (bool, error) {

	dirEntries, readDirErr := activemountsdir.ReadDir(2) // Check if there is any _other_ container using the volume
	if errors.Is(readDirErr, io.EOF) {
		// If directory is empty, unmount overlay and clean up
		return true, fmt.Errorf("there are no active mount files and one was expected. Unmounting")
	} else if readDirErr != nil {
		return false, fmt.Errorf("failed to list activemounts/ %v", readDirErr)
	}

	otherVolumesPresent := len(dirEntries) > 1 || dirEntries[0].Name() != request.ID
	var activeMountInfo activeMount

	activemountFilePath := d.activemountsdir(request.Name) + request.ID
	file, err := os.Open(activemountFilePath)

	if err == nil {
		payload, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			return false, fmt.Errorf("active mount file %s has been opened but cannot be read %w", activemountFilePath, readErr)
		} else if closeErr != nil {
			return false, fmt.Errorf("active mount file %s has been opened but cannot be closed %w", activemountFilePath, readErr)
		}
		unmarshalErr := json.Unmarshal(payload, &activeMountInfo)
		if unmarshalErr != nil {
			return false, fmt.Errorf("active mount file %s contents are invalid %w", activemountFilePath, unmarshalErr)
		}
	} else if os.IsNotExist(err) {
		return !otherVolumesPresent, fmt.Errorf("the active mount file %s was expected but is not there %w", activemountFilePath, err)
	} else {
		return false, fmt.Errorf("the active mount file %s could not be opened %w", activemountFilePath, err)
	}

	activeMountInfo.UsageCount--

	if activeMountInfo.UsageCount == 0 {
		err := os.Remove(activemountFilePath)
		if err != nil {
			return false, fmt.Errorf("the active mount file %s could not be deleted %w", activemountFilePath, err)
		}
		return !otherVolumesPresent, nil
	} else {
		// Convert activeMountInfo to JSON to store it in a file. We can safely ignore Marshal errors, since the
		// activeMount structure is simple enought not to contain "strage" floats, unsupported datatypes or cycles
		// which are the error causes for json.Marshal
		payload, _ := json.Marshal(activeMountInfo)
		err = os.WriteFile(activemountFilePath, payload, 0o644)
		if err != nil {
			return false, fmt.Errorf("the active mount file %s could not be updated %w", activemountFilePath, err)
		}
		return false, nil
	}
}
