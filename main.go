package main

import (
	"fmt"
	"net/http"

	"runtime/debug"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	L "hualu.com/logger"

	gluster "hualu.com/gluster-rest/rest"
)

var Router *mux.Router

func main() {
	L.Gluster.Info("In Gluster-rest")

	// coredump stack
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			L.Zfs.Critical(string(debug.Stack()))
		}
	}()

	// http router
	Router = mux.NewRouter()

	// peer
	Router.HandleFunc("/gluster/peer/add", gluster.ProcessPeerAdd).Methods("POST")
	Router.HandleFunc("/gluster/peer/delete", gluster.ProcessPeerDelete).Methods("POST")
	Router.HandleFunc("/gluster/peer/list", gluster.ProcessPeerList).Methods("GET")
	Router.HandleFunc("/gluster/peer/status", gluster.ProcessPeerStatus).Methods("GET")

	// volume
	Router.HandleFunc("/gluster/volume/create", gluster.ProcessVolumeCreate).Methods("POST")
	Router.HandleFunc("/gluster/volume/start", gluster.ProcessVolumeStart).Methods("POST")
	Router.HandleFunc("/gluster/volume/stop", gluster.ProcessVolumeStop).Methods("POST")
	Router.HandleFunc("/gluster/volume/delete", gluster.ProcessVolumeDelete).Methods("POST")
	Router.HandleFunc("/gluster/volume/info", gluster.ProcessVolumeInfo).Methods("POST")
	Router.HandleFunc("/gluster/volume/status", gluster.ProcessVolumeStatus).Methods("POST")
	Router.HandleFunc("/gluster/volume/health", gluster.ProcessVolumeHealth).Methods("POST")
	Router.HandleFunc("/gluster/volume/rebalance", gluster.ProcessVolumeReBalance).Methods("POST")

	// brick
	Router.HandleFunc("/gluster/volume/brick/add", gluster.ProcessVolumeAddBrick).Methods("POST")
	Router.HandleFunc("/gluster/volume/brick/remove", gluster.ProcessVolumeRemoveBrick).Methods("POST")

	// mount
	Router.HandleFunc("/gluster/mount/add", gluster.ProcessMountAdd).Methods("POST")
	Router.HandleFunc("/gluster/mount/delete", gluster.ProcessMountDelete).Methods("POST")
	Router.HandleFunc("/gluster/mount/list", gluster.ProcessMountList).Methods("POST")

	// http server
	svr := http.Server{
		Addr:         ":7030",
		ReadTimeout:  300 * time.Second,
		WriteTimeout: 300 * time.Second,
		Handler: handlers.CORS(
			handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}),
			handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS", "DELETE"}),
			handlers.AllowedOrigins([]string{"*"}))(Router),
	}
	e := svr.ListenAndServe()
	if e != nil {
		fmt.Println(e.Error())
	}
}
