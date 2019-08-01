package main

import (
	"github.com/mdaliyan/msgbus"
	"fmt"
	"context"
	"github.com/mdaliyan/msgbus/examples/myMsg"
	"github.com/mdaliyan/govert"
	"math/rand"
	"time"
)

var rando = rand.New(rand.NewSource(time.Now().UnixNano()))

var broadcastAddr = "127.0.0.1:" + govert.String(rando.Intn(30000)+10000)
var publisherAddr = "127.0.0.1:2222"

func main() {

	bus := msgbus.NewSubscriber(broadcastAddr, true)

	myMsg.RegisterCustomMessageServer(
		bus.SubscriberConnection(), new(userEvents))

	err := bus.ListenForBroadcasts(publisherAddr, []string{"user_registered"})

	fmt.Println(err)
}

type userEvents struct {
}

func (mb userEvents) UserRegistered(ctx context.Context, n *myMsg.MyMsg) (e *myMsg.Error, err error) {

	fmt.Println(n)

	return &myMsg.Error{}, err
}
