package gluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	L "hualu.com/logger"
)

var SIZE_KB uint64 = 1024
var SIZE_MB uint64 = 1048576
var SIZE_GB uint64 = 1073741824
var SIZE_TB uint64 = 1099511627776
var SIZE_PB uint64 = 1125899906842624

type FSStats struct {
	Blocks     uint64 // Total number of data blocks in a file system.
	BlockFree  uint64 // Free blocks in a file system.
	BlockAvail uint64
	BlockUsed  uint64
	UsePercent float64
}

// Mount describes a mounted filesytem. Please see man fstab for further details.
type Mount struct {
	FileSystem    string  // The field describes the block special device or remote filesystem to be mounted.
	MountPoint    string  // Describes the mount point for the filesytem.
	Type          string  // Describes the type of the filesystem.
	MntOps        string  // Describes the mount options associated with the filesystem.
	DumpFrequency int     // Dump frequency in days.
	PassNo        int     // Pass number on parallel fsck.
	FSStats       FSStats // Filesystem data, may be nil.
}

type CommonMountResponse struct {
	Result string `json:"result"`
	Errors string `json:"errors,omitempty"`
}

type CommonMountRequest struct {
	Volname string `json:"volname"`
	Type    string `json:"type"`
	Mount   string `json:"mount"`
}

type MountDeleteRequest struct {
	CommonMountRequest
	Force string `json:"force"`
}

type MountListResponse struct {
	CommonMountResponse
	MountLists []MountList `json:"data,omitempty"`
}

type MountList struct {
	FileSystem string `json:"filesystem"`
	Type       string `json:"type"`
	Size       string `json:"size"`
	Used       string `json:"used"`
	Avail      string `json:"avail"`
	UsePercent string `json:"use_percent"`
	MountPoint string `json:"mount_point"`
}

func ProcessMountAdd(w http.ResponseWriter, r *http.Request) {

	var rsp CommonMountResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write(buf)
	}()

	// analyses request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}
	defer r.Body.Close()

	var mountAddRequest CommonMountRequest
	e = json.Unmarshal(body, &mountAddRequest)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	L.Gluster.Debugf("After Unmarshall > mountAddRequest is: %+v", mountAddRequest)

	//mkdir
	cmdString := fmt.Sprintf("mkdir -p %s", mountAddRequest.Mount)
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		//rsp.Result = "ERROR"
		//rsp.Errors = string(output)
		// return
	}

	//run command in docker
	//cmdString = fmt.Sprintf("docker exec glusterfs mount -t %s localhost:/%s %s", mountAddRequest.Type, mountAddRequest.Volname, mountAddRequest.Mount)
	cmdString = fmt.Sprintf("mount -t %s localhost:%s %s", mountAddRequest.Type, mountAddRequest.Volname, mountAddRequest.Mount)
	L.Gluster.Info(cmdString)
	cmd = exec.Command("sh", "-c", cmdString)
	output, e = cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	rsp.Result = "OK"
}

func ProcessMountDelete(w http.ResponseWriter, r *http.Request) {

	var rsp CommonMountResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write(buf)
	}()

	// analyse request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}
	defer r.Body.Close()

	var mountDeleteReq MountDeleteRequest
	e = json.Unmarshal(body, &mountDeleteReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}
	L.Gluster.Debugf("After Unmarshall > mountDeleteReq is: %+v", mountDeleteReq)

	//run command in docker
	//var cmdString string
	//cmdString = fmt.Sprintf("docker exec glusterfs umount %s", mountDeleteReq.Mount)
	cmdString := fmt.Sprintf("umount %s", mountDeleteReq.Mount)
	if mountDeleteReq.Force == "true" {
		//cmdString = fmt.Sprintf("docker exec glusterfs umount -fl %s", mountDeleteReq.Mount)
		cmdString = fmt.Sprintf("umount -fl %s", mountDeleteReq.Mount)
	}
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	cmdString = fmt.Sprintf("rm -fr %s", mountDeleteReq.Mount)
	L.Gluster.Info(cmdString)
	cmd = exec.Command("sh", "-c", cmdString)
	cmd.CombinedOutput()

	rsp.Result = "OK"
}

func ParseMounts(mounts map[string]Mount, reader io.Reader, mounttype string) {
	br := bufio.NewReader(reader)
	for s, err := br.ReadString('\n'); err == nil; s, err = br.ReadString('\n') {
		mnt := Mount{}
		if _, err := fmt.Sscanf(s, "%s %s %s %s %d %d", &mnt.FileSystem, &mnt.MountPoint, &mnt.Type, &mnt.MntOps, &mnt.DumpFrequency, &mnt.PassNo); err != nil {
			continue
		}
		if mnt.Type == mounttype {
			statfs := syscall.Statfs_t{}
			if err = syscall.Statfs(mnt.MountPoint, &statfs); err == nil {
				fsStats := FSStats{}
				fsStats.Blocks = statfs.Blocks * (uint64)(statfs.Bsize)
				fsStats.BlockFree = statfs.Bfree * (uint64)(statfs.Bsize)
				fsStats.BlockAvail = statfs.Bavail * (uint64)(statfs.Bsize)
				fsStats.BlockUsed = (statfs.Blocks - statfs.Bavail) * (uint64)(statfs.Bsize)
				UsePercent := (1 - (float64)(statfs.Bavail)/(float64)(statfs.Blocks)) * 100
				fsStats.UsePercent, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", UsePercent), 64)
				mnt.FSStats = fsStats
			}
			if mnt.FSStats.Blocks > 0 {
				mounts[mnt.FileSystem] = mnt
			}
		}
	}
}
func BlockSizeToString(blockSize uint64) (rtn string) {
	var tempFloat float64

	if blockSize > SIZE_PB {
		tempFloat = float64(blockSize) / float64(SIZE_PB)
		rtn = fmt.Sprintf("%.1f", tempFloat) + "P"
		return
	}
	if blockSize > SIZE_TB {
		tempFloat = float64(blockSize) / float64(SIZE_TB)
		rtn = fmt.Sprintf("%.1f", tempFloat) + "T"
		return
	}
	if blockSize > SIZE_GB {
		tempFloat = float64(blockSize) / float64(SIZE_GB)
		rtn = fmt.Sprintf("%.1f", tempFloat) + "G"
		return
	}
	if blockSize > SIZE_MB {
		tempFloat = float64(blockSize) / float64(SIZE_MB)
		rtn = fmt.Sprintf("%.1f", tempFloat) + "M"
		return
	}
	if blockSize > SIZE_KB {
		tempFloat = float64(blockSize) / float64(SIZE_KB)
		rtn = fmt.Sprintf("%.1f", tempFloat) + "K"
		return
	}
	rtn = fmt.Sprintf("%d", blockSize)
	return
}

func ProcessMountList(w http.ResponseWriter, r *http.Request) {

	var rsp MountListResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write(buf)
	}()

	// analyse request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}
	defer r.Body.Close()

	var mountListReq CommonMountRequest
	e = json.Unmarshal(body, &mountListReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}
	L.Gluster.Debugf("After Unmarshall > mountListReq is: %+v", mountListReq)

	f, err := os.Open("/etc/mtab")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer f.Close()

	mounts := make(map[string]Mount)
	mountType := "fuse.glusterfs"
	ParseMounts(mounts, f, mountType)
	for _, mount := range mounts {
		var tempMount MountList
		fileSystemSlice := strings.Split(mount.FileSystem, ":")
		if len(fileSystemSlice) >= 2 {
			tempMount.FileSystem = fileSystemSlice[1]
		} else {
			tempMount.FileSystem = mount.FileSystem
		}
		tempMount.Type = mountType
		tempMount.Size = BlockSizeToString(mount.FSStats.Blocks)
		tempMount.Used = BlockSizeToString(mount.FSStats.BlockUsed)
		tempMount.Avail = BlockSizeToString(mount.FSStats.BlockAvail)
		tempMount.UsePercent = fmt.Sprintf("%.1f", mount.FSStats.UsePercent) + "%"
		tempMount.MountPoint = mount.MountPoint
		rsp.MountLists = append(rsp.MountLists, tempMount)
	}

	if len(rsp.MountLists) == 0 {
		rsp.MountLists = make([]MountList, 0)
	}
	rsp.Result = "OK"

}
