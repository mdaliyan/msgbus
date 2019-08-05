package main

import (
	"context"
	"fmt"
	"github.com/mdaliyan/msgbus"
	"github.com/mdaliyan/msgbus/examples/myMsg"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"time"
)

var randomR = rand.New(rand.NewSource(time.Now().UnixNano()))
var publisherAddrRR = "127.0.0.1:2222"

func main() {

	bus, err := msgbus.NewMessageBusPublisher(publisherAddrRR)
	if err != nil {
		panic(err)
	}

	for {

		time.Sleep(time.Second /10)

		id := strconv.FormatInt(int64(randomR.Intn(90000)), 10)

		msg := &myMsg.MyMsg{Id: id}

		fmt.Print("going to call", msg)

		bus.CallRoundRobin("user_registered", func(conn *grpc.ClientConn) {

			myMsg.NewCustomMessageClient(conn).UserRegistered(context.Background(), msg)

			fmt.Print(" sent ")

		})

		fmt.Println()

	}

	time.Sleep(time.Second * 60)
}
