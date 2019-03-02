// Copyright 2018 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stanpubsub

import (
	"context"
	"errors"
	"reflect"
	"sync"

	stan "github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/ugorji/go/codec"
	"gocloud.dev/gcerrors"
	"gocloud.dev/internal/gcerr"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
	"google.golang.org/grpc/status"
)

type topic struct {
	conn    stan.Conn
	subject string
}

// For encoding we use msgpack from github.com/ugorji/go.
// It was already imported in go-cloud and is reasonably performant.
// However the more recent version has better resource handling with
// the addition of the following:
// 	mh.ExplicitRelease = true
// 	defer enc.Release()
// However this is not compatible with etcd at the moment.
// https://github.com/etcd-io/etcd/pull/10337
var mh codec.MsgpackHandle

func init() {
	// driver.Message.Metadata type
	dm := driver.Message{}
	mh.MapType = reflect.TypeOf(dm.Metadata)
}

// We define our own version of message here for encoding that
// only encodes Body and Metadata. Otherwise we would have to
// add codec decorations to driver.Message.
type encMsg struct {
	Body     []byte            `codec:",omitempty"`
	Metadata map[string]string `codec:",omitempty"`
}

var errNotInitialized = errors.New("stanpubsub: topic not initialized")

// As implements driver.Topic.As.
func (t *topic) As(i interface{}) bool {
	c, ok := i.(*stan.Conn)
	if !ok {
		return false
	}
	*c = t.conn
	return true
}

func (t *topic) ErrorAs(err error, target interface{}) bool {
	return errorAs(err, target)
}

func errorAs(err error, target interface{}) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	p, ok := target.(**status.Status)
	if !ok {
		return false
	}
	*p = s
	return true
}

func (*topic) ErrorCode(err error) gcerrors.ErrorCode {
	return gcerr.GRPCCode(err)
}

// IsRetryable implements driver.Topic.IsRetryable.
func (t *topic) IsRetryable(error) bool {
	// The client handles retries.
	return false
}

// SendBatch implements driver.Topic.SendBatch.
func (t *topic) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	if t == nil || t.conn == nil {
		return errNotInitialized
	}

	// Reuse if possible.
	var em encMsg
	var raw [1024]byte
	b := raw[:0]
	enc := codec.NewEncoderBytes(&b, &mh)

	for _, m := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		enc.ResetBytes(&b)
		em.Body, em.Metadata = m.Body, m.Metadata
		if err := enc.Encode(em); err != nil {
			return err
		}
		if err := t.conn.Publish(t.subject, b); err != nil {
			return err
		}
	}
	// Per specification this is supposed to only return after
	// a message has been sent. Normally NATS is very efficient
	// at sending messages in batches on its own and also handles
	// disconnected buffering during a reconnect event. We will
	// let NATS handle this for now. If needed we could add a
	// FlushWithContext() call which ensures the connected server
	// has processed all the messages.
	return nil
}

// CreateTopic returns a *pubsub.Topic for use with NATS.
// We delay checking for the proper syntax here.
func CreateTopic(nc stan.Conn, topicName string) *pubsub.Topic {
	return pubsub.NewTopic(createTopic(nc, topicName), nil)
}

// createTopic returns the driver for CreateTopic. This function exists so the test
// harness can get the driver interface implementation if it needs to.
func createTopic(nc stan.Conn, topicName string) driver.Topic {
	return &topic{nc, topicName}
}

type subscription struct {
	nc          stan.Conn
	nsub        stan.Subscription
	msgCh       <-chan *stan.Msg
	mu          sync.Mutex
	pendingMsgs map[driver.AckID]*stan.Msg
	err         error
}

// CreateSubscription returns a *pubsub.Subscription representing a NATS Streaming subscription.
// TODO(dlc) - Options for queue groups?
func CreateSubscription(nc stan.Conn, subject string) *pubsub.Subscription {
	return pubsub.NewSubscription(createSubscription(nc, subject), nil)
}

func createSubscription(nc stan.Conn, subject string) driver.Subscription {
	msgCh := make(chan *stan.Msg)
	sub, err := nc.Subscribe(subject, func(msg *stan.Msg) {
		// forward handled messages on channel
		msgCh <- msg
	}, stan.SetManualAckMode())
	return &subscription{
		nc:          nc,
		nsub:        sub,
		msgCh:       msgCh,
		pendingMsgs: make(map[driver.AckID]*stan.Msg),
		err:         err,
	}
}

// ReceiveBatch implements driver.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	if s == nil || s.nsub == nil {
		return nil, stan.ErrBadSubscription
	}

	// return right away if the ctx has an error.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var ms []*driver.Message
	for {
		select {
		case msg := <-s.msgCh:
			dm, err := decode(msg)
			if err != nil {
				return nil, err
			}
			ms = append(ms, dm)
			s.mu.Lock()
			s.pendingMsgs[dm.AckID] = msg
			s.mu.Unlock()
			if len(ms) >= maxMessages {
				break
			}
		case <-ctx.Done():
			break
		}
	}

	return ms, ctx.Err()
}

// Convert NATS msgs to *driver.Message.
func decode(msg *stan.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, stan.ErrNilMsg
	}
	var dm driver.Message
	// Everything is in the msg.Data
	dec := codec.NewDecoderBytes(msg.Data, &mh)
	dec.Decode(&dm)
	dm.AckID = &pb.Ack{Subject: msg.Subject, Sequence: msg.Sequence}
	dm.AsFunc = messageAsFunc(msg)

	return &dm, nil
}

func messageAsFunc(msg *stan.Msg) func(interface{}) bool {
	return func(i interface{}) bool {
		p, ok := i.(**stan.Msg)
		if !ok {
			return false
		}
		*p = msg
		return true
	}
}

// SendAcks implements driver.Subscription.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ids []driver.AckID) error {
	var ids2 []string
	for _, id := range ids {
		ids2 = append(ids2, id.(string))
	}
	// get the msgs from the AckIds and Acknowledge them
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if msg, ok := s.pendingMsgs[id]; ok {
			if err := msg.Ack(); err != nil {
				return err
			}
		}
	}
	return nil
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (s *subscription) IsRetryable(error) bool { return false }

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {
	c, ok := i.(*stan.Subscription)
	if !ok {
		return false
	}
	*c = s.nsub
	return true
}

// ErrorAs implements driver.Subscription.ErrorAs
func (*subscription) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Subscription.ErrorCode
func (*subscription) ErrorCode(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case errNotInitialized, stan.ErrBadAck, stan.ErrBadSubscription:
		return gcerrors.FailedPrecondition
	case stan.ErrBadConnection:
		return gcerrors.PermissionDenied
	case stan.ErrMaxPings:
		return gcerrors.ResourceExhausted
	case stan.ErrTimeout:
		return gcerrors.DeadlineExceeded
	}
	return gcerrors.Unknown
}
