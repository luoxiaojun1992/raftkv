package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"os"
	"time"
	pb "github.com/luoxiaojun1992/raftkv/pb"
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	setReply, errSet := c.Set(ctx, &pb.SetRequest{Key: "foo", Value: "bar"})
	if errSet != nil {
		log.Fatalf("could not set: %v", errSet)
	}

	if setReply.GetResult() {
		log.Println("Success")
	} else {
		log.Println("Failed to set")
	}

	getReply, errGet := c.Get(ctx, &pb.GetRequest{Key: "foo"})
	if errGet != nil {
		log.Fatalf("could not set: %v", errGet)
	}

	log.Println("Value: " + getReply.GetValue())
}
