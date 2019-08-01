package msgbus

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"net"
	"sync"
	"time"
)

func NewMessageBusPublisher(address string) (*MassageBusServer, error) {
	mb := &MassageBusServer{
		Topics: map[string]Subscribers{},
	}
	mb.Server = grpc.NewServer()
	RegisterMessageBusServiceServer(mb.Server, mb)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	go mb.Server.Serve(listener)

	go func() {

		for {

			time.Sleep(time.Second * 10)

			mb.lock.Lock()

			for t, subscribers := range mb.Topics {

				var lostSubscribers Subscribers

				for _, subscriber := range subscribers {

					if subscriber.Conn.GetState() == connectivity.Ready {

						lostSubscribers = append(lostSubscribers, subscriber)

					} else {

						subscriber.Conn.Close()
					}

				}

				mb.Topics[t] = subscribers

			}

			mb.lock.Unlock()

		}
	}()

	return mb, err
}

type Subscribers []*Subscriber

type MassageBusServer struct {
	Topics map[string]Subscribers
	lock   sync.RWMutex
	Server *grpc.Server
	Addr   *grpc.Server
}

func (mb *MassageBusServer) Subscribe(ctx context.Context, n *NodeInfo) (*Error, error) {
	_, _ = mb.Unsubscribe(ctx, n)
	mb.lock.Lock()
	defer mb.lock.Unlock()
	cnn, err := grpc.Dial(n.GetAddr(), grpc.WithInsecure())
	if err != nil {
		return &Error{Message: err.Error()}, err
	}
	for _, topic := range n.GetTopics() {
		node := newSubscriberNode(n.GetAddr(), topic, cnn)
		newSubscribers := append(mb.Topics[topic], node)
		mb.Topics[topic] = newSubscribers
	}

	fmt.Println(n.GetAddr(), "registered on", n.GetTopics(), )
	return &Error{Message: ""}, err

}

func (mb *MassageBusServer) Unsubscribe(ctx context.Context, n *NodeInfo) (e *Error, err error) {

	mb.lock.Lock()

	defer mb.lock.Unlock()

	for _, topic := range n.GetTopics() {

		for _, subscribers := range mb.Topics {

			for _, subscriber := range subscribers {

				if subscriber.ID == fmt.Sprintf(`%s-%s`, topic, n.GetAddr()) {

					subscriber.Conn.Close()

				}
			}

		}

	}

	return
}

func (mb *MassageBusServer) Broadcast(topic string, fn func(*grpc.ClientConn)) {
	mb.lock.Lock()
	subscribers := append(Subscribers{}, mb.Topics[topic]...)
	mb.lock.Unlock()
	for _, subscriber := range subscribers {
		if subscriber.Conn.GetState() == connectivity.Ready {
			fn(subscriber.Conn)
		}
	}
}

type Subscriber struct {
	ID        string
	Addr      string
	Topic     string
	Conn      *grpc.ClientConn
	Listening bool
}

func newSubscriberNode(Addr string, topic string, Conn *grpc.ClientConn) (*Subscriber) {
	return &Subscriber{
		ID:    fmt.Sprintf(`%s-%s`, topic, Addr),
		Addr:  Addr,
		Conn:  Conn,
		Topic: topic,
	}
}
