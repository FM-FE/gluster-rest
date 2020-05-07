package gluster

import (
	"encoding/xml"
	"fmt"
	"os/exec"

	L "hualu.com/logger"
)

type RemoveBrickStatusResponse struct {
	CommonVolumeResponse
	RemoveBrickStatusXML
}

type RemoveBrickStatusXML struct {
	XMLName        xml.Name       `xml:"cliOutput" json:"-"`
	VolRemoveBrick VolRemoveBrick `xml:"volRemoveBrick" json:"volremovebrick"`
}

type VolRemoveBrick struct {
	TaskId    string    `xml:"task-id" json:"taskid"`
	Nodes     []Node    `xml:"node" json:"nodes"`
	Aggregate Aggregate `xml:"aggregate" json:"aggregate"`
}

type Node struct {
	NodeName  string `xml:"nodeName" json:"nodename"`
	Files     string `xml:"files" json:"files"`
	Size      string `xml:"size" json:"size"`
	Failures  string `xml:"failures" json:"failures"`
	Skipped   string `xml:"skipped" json:"skipped"`
	StatusStr string `xml:"statusStr" json:"statusstr"`
	Runtime   string `xml:"runtime" json:"runtime"`
}

type Aggregate struct {
	Files     string `xml:"files" json:"files"`
	Size      string `xml:"size" json:"size"`
	Failures  string `xml:"failures" json:"failures"`
	Skipped   string `xml:"skipped" json:"skipped"`
	StatusStr string `xml:"statusStr" json:"statusstr"`
	Runtime   string `xml:"runtime" json:"runtime"`
}

func RemoveBrick(volumeRemoveBrickReq VolumeRemoveBrickRequest) (rsp CommonVolumeResponse) {
	// run command in docker
	L.Gluster.Debug(volumeRemoveBrickReq.Options)
	var bricks string
	//for i := range volumeRemoveBrickReq.Bricks {
	//	L.Gluster.Debug(bricks)
	//	bricks = bricks + volumeRemoveBrickReq.Bricks[i].Name + " "
	//}
	//L.Gluster.Debugf("bricks is %s", bricks)
	for _, brick := range volumeRemoveBrickReq.Bricks {
		bricks = brick + " " + bricks
	}

	//cmdString := fmt.Sprintf("docker exec glusterfs sh -c \"gluster volume remove-brick %s %s %s <<< y\"", volumeRemoveBrickReq.Volname, bricks, volumeRemoveBrickReq.Options)
	cmdString := fmt.Sprintf("gluster volume remove-brick %s %s %s force<<< y", volumeRemoveBrickReq.Volname, bricks, volumeRemoveBrickReq.Options)
	L.Gluster.Info(cmdString)

	//run command in docker
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(e.Error())
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	rsp.Result = "OK"

	// make response
	return rsp
}

func RemoveBrickStatus(volumeRemoveBrickReq VolumeRemoveBrickRequest) (rsp RemoveBrickStatusResponse) {
	L.Gluster.Debug("rsp is RemoveBrickStatusResponse")

	var bricks string
	//for i := range volumeRemoveBrickReq.Bricks {
	//	L.Gluster.Debug(bricks)
	//	bricks = bricks + volumeRemoveBrickReq.Bricks[i].Name + " "
	//}
	//L.Gluster.Debugf("bricks is %s", bricks)
	for _, brick := range volumeRemoveBrickReq.Bricks {
		bricks = brick + " " + bricks
	}

	//cmdString := fmt.Sprintf("docker exec glusterfs gluster volume remove-brick %s %s %s --xml", volumeRemoveBrickReq.Volname, bricks, volumeRemoveBrickReq.Options)
	cmdString := fmt.Sprintf("gluster volume remove-brick %s %s %s --xml", volumeRemoveBrickReq.Volname, bricks, volumeRemoveBrickReq.Options)
	L.Gluster.Info(cmdString)

	//run command in docker
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(e.Error())
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}
	L.Gluster.Debug(string(output))

	var removeBrickStatusXML RemoveBrickStatusXML
	e = xml.Unmarshal(output, &removeBrickStatusXML)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	//L.Gluster.Debugf("removeBrickStatusXML is %+v", removeBrickStatusXML)

	rsp.RemoveBrickStatusXML = removeBrickStatusXML
	rsp.Result = "OK"

	// make response
	return rsp
}
