package msgbus

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"net"
	"time"
)

func NewSubscriber(broadcastAddr string, keepalive bool) (l Listener) {

	l.broadcastAddr = broadcastAddr
	l.reconnectOnDisconnection = keepalive
	l.server = grpc.NewServer()

	return
}

type Listener struct {
	conn                     *grpc.ClientConn
	server                   *grpc.Server
	publisherAddr            string
	broadcastAddr            string
	topics                   []string
	reconnectOnDisconnection bool
}

func (l *Listener) ListenForBroadcasts(publisherAddr string, topics []string) (err error) {

	l.publisherAddr = publisherAddr
	l.topics = topics

	err = l.connect()

	if err == nil {
		err = l.subscribe()
		go l.keepAlive()
	}

	listener, err := net.Listen("tcp", l.broadcastAddr)
	if err != nil {
		return
	}
	err = l.server.Serve(listener)
	return
}

func (l *Listener) SubscriberConnection() *grpc.Server {
	return l.server
}

func (l *Listener) connect() (err error) {
	if l.conn != nil {
		l.conn.Close()
	}
	l.conn, err = grpc.Dial(l.publisherAddr,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Millisecond * 100, // send pings every 10 seconds if there is no activity
			Timeout:             1 * time.Second,        // wait 20 second for ping ack before considering the connection dead
			PermitWithoutStream: true,                   // send pings even without active streams
		}))
	if err != nil {
		return
	}
	return
}

func (l *Listener) subscribe() (err error) {
	clusterManager := NewMessageBusServiceClient(l.conn)
	_, err = clusterManager.Subscribe(context.Background(),
		&SubscriberInfo{
			Topics: l.topics,
			Addr:   l.broadcastAddr,
		},
	)
	return
}

func (l *Listener) keepAlive() (err error) {

	for {
		for state := l.conn.GetState(); state != connectivity.TransientFailure && state != connectivity.Shutdown; {
			l.conn.WaitForStateChange(context.Background(), l.conn.GetState())
			state = l.conn.GetState()
		}
		fmt.Println("state changed:", l.conn.GetState())
		if ! l.reconnectOnDisconnection {
			return
		}
		time.Sleep(time.Millisecond * 100)
		if l.conn != nil {
			l.conn.Close()
		}
		err = l.connect()
		if err == nil {
			l.subscribe()
		}
	}

}
