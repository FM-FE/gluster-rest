package gluster

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strings"

	L "hualu.com/logger"
)

type CommonPeerRequest struct {
	Hostname string `json:"hostname,omitempty"`
}

type CommonPeerResponse struct {
	Result string `json:"result"`
	Errors string `json:"errors,omitempty"`
}

type PeerInfo struct {
	UUID      string `json:"uuid"`
	Hostname  string `json:"hostname"`
	State     string `json:"state"`
	Localhost bool   `json:"localhost"`
}

type PeerListResponse struct {
	CommonPeerResponse
	Peers []PeerInfo `json:"hosts"`
}

type PeerStatusResponse struct {
	CommonPeerResponse
	PeerStatusXML
}

type PeerStatusXML struct {
	XMLName    xml.Name   `xml:"cliOutput" json:"-"`
	PeerStatus PeerStatus `xml:"peerStatus" json:"peerstatus"`
}

type PeerStatus struct {
	Peers []Peer `xml:"peer" json:"peers"`
}

type Peer struct {
	UUID      string `xml:"uuid" json:"uuid"`
	HostName  string `xml:"hostname" json:"hostname"`
	Connected int    `xml:"connected" json:"connected"`
	State     int    `xml:"state" json:"state"`
	StateStr  string `xml:"stateStr" json:"status"`
}

/*
[example]
docker exec  glusterfs sh -c "gluster peer probe 10.2.174.237"

curl -X POST http://127.0.0.1:7030/gluster/peer/add  -H 'Content-Type: application/json' -d '{
 "hostname": "10.2.174.237"
}'
<- {"result":"OK"}
*/
func ProcessPeerAdd(w http.ResponseWriter, r *http.Request) {
	var rsp CommonPeerResponse
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		w.WriteHeader(500)
		return
	}
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
		}
		w.Write([]byte(buf))
	}()

	// request
	var req CommonPeerRequest
	if e := json.Unmarshal(body, &req); e != nil {
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// cmd
	if len(req.Hostname) <= 0 {
		rsp.Result = "ERROR"
		rsp.Errors = "parameter is not valid"
		return
	}

	//cmdString := fmt.Sprintf(`docker exec glusterfs sh -c "gluster peer probe %s" `, req.Hostname)
	cmdString := fmt.Sprintf(`gluster peer probe %s `, req.Hostname)
	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	// response
	rsp.Result = "OK"
	rsp.Errors = string(output)
}

/*
[example]
docker exec glusterfs sh -c "gluster peer detach 10.2.174.237"

curl -X POST http://127.0.0.1:7030/gluster/peer/delete -H 'Content-Type: application/json' -d '{
"hostname": "10.2.174.237"
}'
<- {"result":"OK"}
*/
func ProcessPeerDelete(w http.ResponseWriter, r *http.Request) {
	var rsp CommonPeerResponse
	body, e := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if e != nil {
		w.WriteHeader(500)
		return
	}
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
		}
		w.Write([]byte(buf))
	}()

	// request
	var req CommonPeerRequest
	if e := json.Unmarshal(body, &req); e != nil {
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
		return
	}

	// cmd
	if len(req.Hostname) <= 0 {
		rsp.Result = "ERROR"
		rsp.Errors = "parameter is not valid"
		return
	}
	//cmdString := fmt.Sprintf(`/usr/bin/docker exec glusterfs sh -c "gluster peer detach %s" `, req.Hostname)
	cmdString := fmt.Sprintf(`gluster peer detach %s <<<y`, req.Hostname)
	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	// response
	rsp.Result = "OK"
	rsp.Errors = string(output)
}

/*
[example]
docker exec  glusterfs sh -c "gluster pool list<<<y|awk NR!=1"

curl -X GET http://127.0.0.1:7030/gluster/peer/list
<- {"result":"OK"}
*/
func ProcessPeerList(w http.ResponseWriter, r *http.Request) {
	var rsp PeerListResponse
	var peers []PeerInfo

	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
		}
		w.Write([]byte(buf))
	}()

	// request

	// cmd
	//cmdString := fmt.Sprintf(`docker exec glusterfs sh -c "gluster pool list<<<y|awk NR!=1"`)
	cmdString := fmt.Sprintf(`gluster pool list<<<y|awk NR!=1`)
	L.Gluster.Info(cmdString)

	cmd := exec.Command("sh", "-c", cmdString)
	output, e := cmd.CombinedOutput()
	if e != nil {
		L.Gluster.Error(string(output))
		rsp.Result = "ERROR"
		rsp.Errors = string(output)
		return
	}

	// response
	var peerInfo *PeerInfo
	s := string(output)
	s = strings.TrimSpace(s)
	lines := strings.Split(s, "\n")
	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) != 3 { // Here is the pool info line
			break
		}
		peerInfo = &PeerInfo{UUID: parts[0], Hostname: parts[1], State: parts[2], Localhost: false}
		if parts[1] == "localhost" {
			cmdString = fmt.Sprintf("hostname")
			cmd := exec.Command("sh", "-c", "hostname")
			output, e := cmd.Output()
			if e != nil {
				L.Gluster.Error(e.Error())
				rsp.Result = "ERROR"
				rsp.Errors = e.Error()
				return
			}
			hostname := string(output)
			L.Gluster.Debug(hostname)
			hostname = strings.Replace(hostname, "\n", "", 1)
			peerInfo = &PeerInfo{UUID: parts[0], Hostname: hostname, State: parts[2], Localhost: true}
		}
		peers = append(peers, *peerInfo)
	}
	rsp.Result = "OK"
	rsp.Peers = peers
}

func ProcessPeerStatus(w http.ResponseWriter, r *http.Request) {
	var rsp PeerStatusResponse
	defer func() {
		buf, e := json.Marshal(&rsp)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(buf))
	}()

	// run command in docker
	//cmdString := fmt.Sprintf("docker exec glusterfs gluster peer status --xml")
	cmdString := fmt.Sprintf("gluster peer status --xml")
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

	var peerStatusXML PeerStatusXML
	e = xml.Unmarshal(output, &peerStatusXML)
	if e != nil {
		L.Gluster.Error(e.Error())
		rsp.Result = "ERROR"
		rsp.Errors = e.Error()
	}

	L.Gluster.Debug(peerStatusXML)

	rsp.PeerStatusXML = peerStatusXML
	rsp.Result = "OK"

}
