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
	var res []byte
	var err error
	res, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	cmd = exec.Command("cat", args.OutputPath)
	res, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}
	tmp := strings.Split(args.OutputPath,"/")
	tmpName := tmp[len(tmp)-1]
	taskName := tmpName[0:len(tmpName)-4]
	fmt.Println("Finished inferencing ",taskName)
	// SdfsClient.PutFile(args.OutputPath, sdfsName)
	*reply = string(res)
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
