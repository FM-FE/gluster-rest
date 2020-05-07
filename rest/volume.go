package gluster

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"

	"github.com/errors"

	L "hualu.com/logger"
)

// struct used by RESTful API
type CommonVolumeRequest struct {
	Volname string `json:"volname"`
}

type CommonVolumeResponse struct {
	Result string `json:"result"`
	Errors string `json:"errors,omitempty"`
}

type VolumeCreateRequest struct {
	CommonVolumeRequest
	Type       string   `json:"type,omitempty"`
	Transport  string   `json:"transport"`
	Count      string   `json:"count"`
	Redundancy string   `json:"redundancy"`
	Bricks     []string `json:"bricks"`
	Force      string   `json:"force"`
}

type VolumeAddBrickRequest struct {
	CommonVolumeRequest
	Bricks []string `json:"bricks"`
}

type VolumeRemoveBrickRequest struct {
	CommonVolumeRequest
	Bricks  []string `json:"bricks"`
	Options string   `json:"options"`
}

type VolumeReBalanceRequest struct {
	CommonVolumeRequest
	Options string `json:"options"` // start, stop, status
}

// Volume Re_balance
type VolumeReBalanceResponse struct {
	CommonVolumeResponse
	VolumeReBalanceXML
}

type VolumeReBalanceXML struct {
	XMLName      xml.Name     `xml:"cliOutput" json:"-"`
	OpRet        int          `xml:"opRet" json:"op_ret,omitempty"`
	OpErrno      int          `xml:"opErrno" json:"-"`
	OpErrstr     string       `xml:"opErrstr" json:"op_errstr,omitempty"`
	VolReBalance VolReBalance `xml:"volRebalance" json:"vol_rebalance,omitempty"`
}

type VolReBalance struct {
	TaskId    string              `xml:"task-id" json:"task_id,omitempty"`
	Op        int                 `xml:"op" json:"op,omitempty"`
	NodeCount int                 `xml:"nodeCount"json:"node_count,omitempty"`
	Node      []NodeInRebalance   `xml:"node" json:"node,omitempty"`
	Aggregate AggregateInRebalace `xml:"aggregate" json:"aggregate,omitempty"`
}

type NodeInRebalance struct {
	NodeName  string `xml:"nodeName" json:"node_name,omitempty"`
	ID        string `xml:"id" json:"id,omitempty"`
	Files     int    `xml:"files" json:"files,omitempty"`
	Size      int    `xml:"size" json:"size,omitempty"`
	Lookups   int    `xml:"lookups" json:"lookups,omitempty"`
	Failures  int    `xml:"failures" json:"failures,omitempty"`
	Skipped   int    `xml:"skipped" json:"skipped,omitempty"`
	Status    int    `xml:"status" json:"status,omitempty"`
	StatusStr string `xml:"statusStr" json:"status_str,omitempty"`
	Runtime   string `xml:"runtime" json:"runtime,omitempty"`
}

type AggregateInRebalace struct {
	Files     int    `xml:"files" json:"files,omitempty"`
	Size      int    `xml:"size" json:"size,omitempty"`
	Lookups   int    `xml:"lookups" json:"lookups,omitempty"`
	Failures  int    `xml:"failures" json:"failures,omitempty"`
	Skipped   int    `xml:"skipped" json:"skipped,omitempty"`
	Status    int    `xml:"status" json:"status,omitempty"`
	StatusStr string `xml:"statusStr" json:"status_str,omitempty"`
	Runtime   string `xml:"runtime" json:"runtime,omitempty"`
}

// Volume Info
type VolumeInfoResponse struct {
	CommonVolumeResponse
	VolumeInfoXML
}

type VolumeInfoXML struct {
	XMLName xml.Name `xml:"cliOutput" json:"-"`
	OpRet   int      `xml:"opRet" json:"-"`
	VolInfo VolInfo  `xml:"volInfo"`
}

type VolInfo struct {
	Volumes Volumes `xml:"volumes"`
}

type Volumes struct {
	Volume []Volume `xml:"volume"`
	Count  int      `xml:"count"`
}

type Volume struct {
	Name      string `xml:"name" json:"name"`
	Id        string `xml:"id" json:"id"`
	Status    string `xml:"status" json:"status"`
	StatusStr string `xml:"statusStr" json:"status_str"`
	Type      string `xml:"type" json:"type"`
	TypeStr   string `xml:"typeStr" json:"type_str"`
	//SnapshotCount string   `xml:"snapshotCount"json:"snapshot_count"`
	BrickCount string `xml:"brickCount" json:"brick_count"`
	//StripeCount   string   `xml:"stripeCount" json:"stripe_count"`
	ReplicaCount    string   `xml:"replicaCount" json:"replica_count"`       //Replicate卷冗余个数
	DisperseCount   string   `xml:"disperseCount" json:"disperse_count"`     //Disperse卷每组个数: 数据+冗余
	RedundancyCount string   `xml:"redundancyCount" json:"redundancy_count"` //Disperse卷冗余个数
	Transport       string   `xml:"transport" json:"transports"`
	Bricks          []Brick  `xml:"bricks>brick" json:"bricks"`
	Options         []Option `xml:"options>option" json:"options"`
}

type Brick struct {
	Name string `xml:"name" json:"brick"`
}

type Option struct {
	Name  string `xml:"name" json:"name"`
	Value string `xml:"value" json:"value"`
}

// Volume Status
type VolumeStatusResponse struct {
	CommonVolumeResponse
	VolumeStatusXML
}

type VolumeStatusXML struct {
	XMLName   xml.Name  `xml:"cliOutput" json:"-"`
	OpRet     int       `xml:"opRet" json:"-"`
	VolStatus VolStatus `xml:"volStatus" json:"vol_status"`
}

type VolStatus struct {
	VolumesInStatus VolumesInStatus `xml:"volumes" json:"volumes"`
}

type VolumesInStatus struct {
	VolumeInStatus []VolumeInStatus `xml:"volume" json:"volume"`
}

type VolumeInStatus struct {
	VolName   string         `xml:"volName" json:"vol_name"`
	NodeCount string         `xml:"nodeCount" json:"node_count"`
	Node      []NodeInStatus `xml:"node" json:"node"`
}

type NodeInStatus struct {
	Hostname string `xml:"hostname" json:"hostname"`
	Path     string `xml:"path" json:"path"`
	PeerId   string `xml:"peerid" json:"peer_id"`
	Status   string `xml:"status" json:"status"`
	Port     string `xml:"port" json:"port"`
	Ports    Ports  `xml:"ports" json:"ports"`
	Pid      string `xml:"pid" json:"pid"`
}

type Ports struct {
	Tcp  string `xml:"tcp" json:"tcp"`
	Rdma string `xml:"rdma" json:"rdma"`
}

func ProcessVolumeCreate(w http.ResponseWriter, r *http.Request) {

	var rsp CommonVolumeResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	var volumeCreateReq VolumeCreateRequest
	e = json.Unmarshal(body, &volumeCreateReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	//L.Gluster.Debugf("After Unmarshall > VolumeCreateReq is: %+v", volumeCreateReq)

	// TODO
	// run command in docker
	var bricks string
	for _, brick := range volumeCreateReq.Bricks {
		bricks = brick + " " + bricks
	}
	//L.Gluster.Debugf("bricks is [%s]", bricks)

	var force string
	if volumeCreateReq.Force == "true" {
		force = "force"
	} else {
		force = ""
	}

	//cmdString := fmt.Sprintf(
	//	"docker exec glusterfs sh -c \"gluster volume create %s %s %s transport %s %s %s\"",
	//	volumeCreateReq.Volname, volumeCreateReq.Type, volumeCreateReq.Count, volumeCreateReq.Transport, bricks, force)

	cmdString := fmt.Sprintf(
		"gluster volume create %s %s %s transport %s %s %s",
		volumeCreateReq.Volname, volumeCreateReq.Type, volumeCreateReq.Count, volumeCreateReq.Transport, bricks, force)

	if volumeCreateReq.Force == "true" {
		//cmdString = fmt.Sprintf(
		//	"docker exec glusterfs sh -c \"gluster volume create %s %s %s transport %s %s %s <<<y\"",
		//	volumeCreateReq.Volname, volumeCreateReq.Type, volumeCreateReq.Count, volumeCreateReq.Transport, bricks, force)

		cmdString = fmt.Sprintf(
			"gluster volume create %s %s %s transport %s %s %s <<<y",
			volumeCreateReq.Volname, volumeCreateReq.Type, volumeCreateReq.Count, volumeCreateReq.Transport, bricks, force)
	}

	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		L.Gluster.Error(string(output))
		return
	}

	// start
	cmdString = fmt.Sprintf("gluster volume start %s", volumeCreateReq.Volname)
	L.Gluster.Info(cmdString)
	cmd = exec.Command("sh", "-c", cmdString)
	cmd.CombinedOutput()

	rsp.Result = "OK"
	rsp.Errors = string(output)
}

func ProcessVolumeStart(w http.ResponseWriter, r *http.Request) {

	var rsp CommonVolumeResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	var volumeStartReq CommonVolumeRequest
	e = json.Unmarshal(body, &volumeStartReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs gluster volume start %s", volumeStartReq.Volname)
	cmdString := fmt.Sprintf("gluster volume start %s", volumeStartReq.Volname)

	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	rsp.Result = "OK"
	rsp.Errors = string(output)
}

func ProcessVolumeStop(w http.ResponseWriter, r *http.Request) {
	var rsp CommonVolumeResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	var volumeStopReq CommonVolumeRequest
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	e = json.Unmarshal(body, &volumeStopReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs sh -c 'gluster volume stop %s force <<< y' ", volumeStopReq.Volname)
	cmdString := fmt.Sprintf("gluster volume stop %s force <<< y ", volumeStopReq.Volname)
	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	rsp.Result = "OK"
	rsp.Errors = string(output)
}

func ProcessVolumeDelete(w http.ResponseWriter, r *http.Request) {
	var rsp CommonVolumeResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	var volumeDeleteReq CommonVolumeRequest
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	e = json.Unmarshal(body, &volumeDeleteReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// run command
	cmdString := fmt.Sprintf("gluster volume stop %s force <<< y ", volumeDeleteReq.Volname)
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	cmd.CombinedOutput()

	//cmdString := fmt.Sprintf("docker exec glusterfs sh -c \"gluster volume delete %s <<<y\"", volumeDeleteReq.Volname)
	cmdString = fmt.Sprintf("gluster volume delete %s <<<y", volumeDeleteReq.Volname)
	L.Gluster.Info(cmdString)
	cmd = exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	rsp.Result = "OK"
	rsp.Errors = string(output)
}

func ProcessVolumeInfo(w http.ResponseWriter, r *http.Request) {
	var rsp VolumeInfoResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			L.Gluster.Error(e.Error())
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	var volumeInfoReq CommonVolumeRequest
	e = json.Unmarshal(body, &volumeInfoReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs  gluster volume info %s --xml", volumeInfoReq.Volname)
	cmdString := fmt.Sprintf("gluster volume info %s --xml", volumeInfoReq.Volname)
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	L.Gluster.Debug(string(output))

	var volinfoXML VolumeInfoXML
	e = xml.Unmarshal(output, &volinfoXML)
	if e != nil {
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		L.Gluster.Error(e.Error())
	}
	L.Gluster.Infof("XML is %+v", volinfoXML)

	rsp.VolumeInfoXML = volinfoXML
	rsp.Result = "OK"

}

func ProcessVolumeAddBrick(w http.ResponseWriter, r *http.Request) {
	var rsp CommonVolumeResponse
	defer func() {
		buf, e := json.Marshal(rsp)
		if e != nil {
			L.Gluster.Error(e.Error())
			w.WriteHeader(500)
			return
		}
		w.Write(buf) // w.Write([]byte(buf))
	}()

	// analyze request
	var volumeAddBrickReq VolumeAddBrickRequest
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	//L.Gluster.Debugf("volumeAddBrickReqJSON is: %s", string(body))

	e = json.Unmarshal(body, &volumeAddBrickReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	//L.Gluster.Debugf("volumeAddBrickReq is: %+v", volumeAddBrickReq)

	// run command in docker
	var bricks string
	for i := range volumeAddBrickReq.Bricks {
		bricks = fmt.Sprintf("%s %s", bricks, volumeAddBrickReq.Bricks[i])
	}

	//L.Gluster.Info("bricks is:", bricks)
	//cmdString := fmt.Sprintf("docker exec glusterfs gluster volume add-brick %s %s", volumeAddBrickReq.Volname, bricks)
	cmdString := fmt.Sprintf("gluster volume add-brick %s %s force<<<y", volumeAddBrickReq.Volname, bricks)
	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error("error is " + e.Error())
		L.Gluster.Error("cmd output is " + string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	//L.Gluster.Debugf("cmd output is: " + string(output))

	rsp.Result = "OK"
	rsp.Errors = string(output)
}

func ProcessVolumeRemoveBrick(w http.ResponseWriter, r *http.Request) {
	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		L.Gluster.Error(e.Error())
		w.WriteHeader(500)
		return
	}

	var volRemoveBrickReq VolumeRemoveBrickRequest
	e = json.Unmarshal(body, &volRemoveBrickReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		w.WriteHeader(500)
		return
	}

	//L.Gluster.Debugf("volRemoveBrickReq is %+v", volRemoveBrickReq)

	rsp := RemoveBrick(volRemoveBrickReq)
	buf, e := json.Marshal(rsp)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}
	w.Write(buf)
	return
}

func ProcessVolumeStatus(w http.ResponseWriter, r *http.Request) {
	var rsp VolumeStatusResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			L.Gluster.Error(e.Error())
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	var volumeInfoReq CommonVolumeRequest
	e = json.Unmarshal(body, &volumeInfoReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs  gluster volume info %s --xml", volumeInfoReq.Volname)
	cmdString := fmt.Sprintf("gluster volume status %s --xml", volumeInfoReq.Volname)
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	L.Gluster.Debug(string(output))

	var volstatusXML VolumeStatusXML
	e = xml.Unmarshal(output, &volstatusXML)
	if e != nil {
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		L.Gluster.Error(e.Error())
	}
	//L.Gluster.Infof("XML is %+v", volstatusXML)

	rsp.VolumeStatusXML = volstatusXML
	rsp.Result = "OK"

}

func ProcessVolumeHealth(w http.ResponseWriter, r *http.Request) {
	var rsp CommonVolumeResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			L.Gluster.Error(e.Error())
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	var volumeInfoReq CommonVolumeRequest
	e = json.Unmarshal(body, &volumeInfoReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	if volumeInfoReq.Volname == "" {
		L.Gluster.Error(errors.New("Volume Name cannot be empty"))
		rsp.Result = "ERROR"
		rsp.Errors = "Volume Name cannot be empty"
		return
	}

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs  gluster volume info %s --xml", volumeInfoReq.Volname)
	cmdString := fmt.Sprintf("gluster volume heal %s full", volumeInfoReq.Volname)
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	L.Gluster.Debug(string(output))

	rsp.Errors = string(output)
	rsp.Result = "OK"
}

func ProcessVolumeReBalance(w http.ResponseWriter, r *http.Request) {
	var rsp VolumeReBalanceResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			L.Gluster.Error(e.Error())
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// analyze request
	body, e := ioutil.ReadAll(r.Body)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	var volumeReBalanceReq VolumeReBalanceRequest
	e = json.Unmarshal(body, &volumeReBalanceReq)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	if volumeReBalanceReq.Volname == "" {
		L.Gluster.Error(errors.New("Volume Name cannot be empty"))
		rsp.Result = "ERROR"
		rsp.Errors = "Volume Name cannot be empty"
		return
	}

	switch volumeReBalanceReq.Options {
	case "start":
		break
	case "stop":
		break
	case "status":
		break
	default:
		L.Gluster.Error(errors.New("Volume Options illegal"))
		rsp.Result = "ERROR"
		rsp.Errors = "Volume Options illegal"
		return
	}

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs  gluster volume info %s --xml", volumeInfoReq.Volname)
	cmdString := fmt.Sprintf("gluster volume rebalance %s %s --xml", volumeReBalanceReq.Volname, volumeReBalanceReq.Options)
	L.Gluster.Info(cmdString)
	cmd := exec.Command("sh", "-c", cmdString)
	output, err := cmd.CombinedOutput()

	L.Gluster.Debug(string(output))

	var volumeReBalanceXML VolumeReBalanceXML
	e = xml.Unmarshal(output, &volumeReBalanceXML)
	if e != nil {
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		L.Gluster.Error(e.Error())
	}

	if err != nil {
		L.Gluster.Error(volumeReBalanceXML)
		rsp.Result = "ERROR"
		rsp.Errors = volumeReBalanceXML.OpErrstr
		return
	}

	//L.Gluster.Infof("XML is %+v", volumeReBalanceXML)
	rsp.VolumeReBalanceXML = volumeReBalanceXML

	rsp.Result = "OK"
}
