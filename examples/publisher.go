package main

import (
	"github.com/mdaliyan/msgbus"
	"time"
	"google.golang.org/grpc"
	"github.com/mdaliyan/msgbus/examples/myMsg"
	"context"
	"strconv"
	"math/rand"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {

	bus, err := msgbus.NewMessageBusPublisher("127.0.0.1:2222")
	if err != nil {
		panic(err)
	}

	for {

		time.Sleep(time.Millisecond * time.Duration(random.Intn(1000)))

		id := strconv.FormatInt(int64(random.Intn(90000)), 10)

		msg := &myMsg.MyMsg{Id: id}

		bus.Broadcast("user_registered" +
			"" +
			"" +
			"" +
			"" +
			"" +
			"" +
			"" +
			"" +
			" " +
			"" +
			"", func(conn *grpc.ClientConn) {
			myMsg.NewCustomMessageClient(conn).
				UserRegistered(context.Background(), msg)
		})
	}

	time.Sleep(time.Second * 60)
}
