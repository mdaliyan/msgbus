package msgbus

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"net"
	"sync"
	"time"
)

func NewMessageBusPublisher(address string) (*MassageBusServer, error) {
	mb := &MassageBusServer{
		Topics: map[string]*Topic{},
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

			time.Sleep(time.Second * 5)

			mb.lock.Lock()

			for _, topic := range mb.Topics {

				var Subscribers []*Subscriber

				for _, subscriber := range topic.Subscribers {

					if subscriber.Conn.GetState() != connectivity.Ready {

						subscriber.Conn.Close()

					} else {

						Subscribers = append(Subscribers, subscriber)

					}

				}

				topic.Subscribers = Subscribers

			}

			mb.lock.Unlock()

		}
	}()

	return mb, err
}

type Topic struct {
	Subscribers []*Subscriber
	turn        int
	lock        sync.RWMutex
}

func (t *Topic) getRoundRobinSubscriber() (s *Subscriber, err error) {
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
	l := len(t.Subscribers)
	if l == 0 {
		return nil, errors.New("no subscriber")
	}
	t.turn = (t.turn + 1) % l
	s = t.Subscribers[t.turn]
	return
}

type MassageBusServer struct {
	Topics map[string]*Topic
	lock   sync.RWMutex
	Server *grpc.Server
	Addr   *grpc.Server
}

func (mb *MassageBusServer) Subscribe(ctx context.Context, s *SubscriberInfo) (*Error, error) {
	_, _ = mb.Unsubscribe(ctx, s)
	mb.lock.Lock()
	defer mb.lock.Unlock()
	cnn, err := grpc.Dial(s.GetAddr(), grpc.WithInsecure())
	if err != nil {
		return &Error{Message: err.Error()}, err
	}
	for _, topic := range s.GetTopics() {
		subscriber := newSubscriberNode(s.GetAddr(), topic, cnn)
		t, ok := mb.Topics[topic]
		if ok {
			t.Subscribers = append(t.Subscribers, subscriber)
		} else {
			mb.Topics[topic] = &Topic{
				Subscribers: []*Subscriber{subscriber},
			}
		}
	}

	fmt.Println(s.GetAddr(), "registered on", s.GetTopics(), )

	return &Error{Message: ""}, err

}

func (mb *MassageBusServer) Unsubscribe(ctx context.Context, s *SubscriberInfo) (e *Error, err error) {

	mb.lock.Lock()

	defer mb.lock.Unlock()

	for _, topic := range s.GetTopics() {

		for _, t := range mb.Topics {

			for _, subscriber := range t.Subscribers {

				if subscriber.ID == fmt.Sprintf(`%s-%s`, topic, s.GetAddr()) {

					subscriber.Conn.Close()

				}

			}

		}

	}

	return
}

func (mb *MassageBusServer) CallRoundRobin(topic string, fn func(*grpc.ClientConn)) {
	mb.lock.Lock()
	t, ok := mb.Topics[topic]
	mb.lock.Unlock()
	if ! ok {
		return
	}
	c, err := t.getRoundRobinSubscriber()
	if err == nil && c != nil {
		if c.Conn.GetState() == connectivity.Ready {
			fn(c.Conn)
		} else {
			mb.CallRoundRobin(topic, fn)
		}
	}
}

func (mb *MassageBusServer) Broadcast(topic string, fn func(*grpc.ClientConn)) {
	mb.lock.Lock()
	t, ok := mb.Topics[topic]
	mb.lock.Unlock()
	if ! ok {
		return
	}
	subscribers := append([]*Subscriber{}, t.Subscribers...)
	for _, subscriber := range subscribers {
		if subscriber.Conn.GetState() == connectivity.Ready {
			fn(subscriber.Conn)
		}
	}
}

type Subscriber struct {
	ID    string
	Addr  string
	Topic string
	Conn  *grpc.ClientConn
}

func newSubscriberNode(Addr string, topic string, Conn *grpc.ClientConn) (*Subscriber) {
	return &Subscriber{
		ID:    fmt.Sprintf(`%s-%s`, topic, Addr),
		Addr:  Addr,
		Conn:  Conn,
		Topic: topic,
	}
}
