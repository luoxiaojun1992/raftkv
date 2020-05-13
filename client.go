package main

import (
	"context"
	pb "github.com/luoxiaojun1992/raftkv/pb"
	"google.golang.org/grpc"
	"log"
	"os"
	"time"
)

func main ()  {
	grpcPort := os.Args[1]

	// Set up a connection to the server.
	conn, err := grpc.Dial(grpcPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKVClient(conn)

	//setCtx, setCancel := context.WithTimeout(context.TODO(), 10 * time.Second)
	//defer setCancel()
	//
	//setReply, errSet := c.Set(setCtx, &pb.SetRequest{Key: "foo", Value: "bar19"})
	//if errSet != nil {
	//	log.Fatalf("could not set: %v", errSet)
	//}
	//
	//if setReply.GetResult() {
	//	log.Println("Success")
	//} else {
	//	log.Println("Failed to set")
	//}

	getCtx, getCancel := context.WithTimeout(context.TODO(), 10 * time.Second)
	defer getCancel()

	getReply, errGet := c.Get(getCtx, &pb.GetRequest{Key: "raftLeaderGrpcPort"})
	if errGet != nil {
		log.Fatalf("could not get: %v", errGet)
	}

	log.Println("Value: " + getReply.GetValue())
}
