package Sdfs

// import (
// 	"fmt"
// 	"net"
// 	"net/rpc"
// 	"os"
// 	"os/exec"
// )


// type InferenceService string
// var InferenceClient Inference

// func (t *InferenceService) Inference(args *Args, reply *string) error {
// 	SdfsClient.GetFile(args.LocalName, args.SdfsName)
// 	var cmd = exec.Command("python3", args.ModelPath, args.LocalName, args.LocalName+"_out")
// 	SdfsClient.PutFile(args.LocalName+"_out", args.SdfsName+"_out")
// 	var res []byte
// 	var err error
// 	res, err = cmd.CombinedOutput()
// 	if err != nil {
// 		return err
// 	}
// 	*reply = string(res)
// 	return nil
// }

// func (t *InferenceService) Listen() {

// 	inference := new(InferenceService)
// 	rpc.Register(inference)

// 	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8890")
// 	checkError(err)

// 	listener, err := net.ListenTCP("tcp", tcpAddr)
// 	checkError(err)

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			continue
// 		}
// 		rpc.ServeConn(conn)
// 	}

// }

// func checkError(err error) {
// 	if err != nil {
// 		fmt.Println("Fatal error ", err.Error())
// 		os.Exit(1)
// 	}
// }
