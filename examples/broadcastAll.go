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

var random = rand.New(rand.NewSource(time.Now().UnixNano()))
var publisherAddress = "127.0.0.1:2222"

func main() {

	bus, err := msgbus.NewMessageBusPublisher(publisherAddress)
	if err != nil {
		panic(err)
	}

	for {

		time.Sleep(time.Second /2 )

		id := strconv.FormatInt(int64(random.Intn(90000)), 10)

		msg := &myMsg.MyMsg{Id: id}

		fmt.Print("going to broadcast", msg)

		bus.Broadcast("user_registered", func(conn *grpc.ClientConn) {

			myMsg.NewCustomMessageClient(conn).UserRegistered(context.Background(), msg)

			fmt.Print(" sent ")

		})

		fmt.Println()

	}

	time.Sleep(time.Second * 60)
}
