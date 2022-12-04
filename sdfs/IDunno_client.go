package Sdfs

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
)


type InferenceService struct{}


func (is *InferenceService) Inference(args *Args, reply *string) error {
	// SdfsClient.GetFile(args.LocalName, args.SdfsName)
	var cmd = exec.Command("python3.9", args.ModelPath, args.InputPath, args.OutputPath)
	// var res []byte
	var err error
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	// fmt.Println(res)
	tmp := strings.Split(args.OutputPath,"/")
	sdfsName := tmp[len(tmp)-1]
	SdfsClient.PutFile(args.OutputPath, sdfsName)
	*reply = "Output have been save to " + string(sdfsName) + " on sdfs.\n"
	return nil
}

func (idunno *IDUNNOMaster) RegisterRpc() {

	inference := new(InferenceService)
	rpc.Register(inference)

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8890")
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		rpc.ServeConn(conn)
	}

}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
