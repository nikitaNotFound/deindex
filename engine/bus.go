package engine

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type MessageHandlingStatus int

const (
	MessageHandlingStatusPending MessageHandlingStatus = iota
	MessageHandlingStatusInProgress
	MessageHandlingStatusCompleted
	MessageHandlingStatusDeadLetter
)

type MessageHandling struct {
	ID         string
	TopicID    TopicID
	MessageID  string
	ReceiverID ActorID
	Status     MessageHandlingStatus
	Retries    int
	CreatedAt  time.Time
}

type Message interface {
	Topic() TopicID
	Args() any
	ID() string
}

type TopicID string

type receiver struct {
	impl Receiver
	ch   chan Message
}

type Topic struct {
	id        TopicID
	msgChan   chan Message
	receivers map[ActorID]*receiver
	engineCtx EngineCtx

	mu sync.RWMutex
}

func createTopic(id TopicID, engineCtx EngineCtx) *Topic {
	return &Topic{
		id:        id,
		msgChan:   make(chan Message, 64),
		receivers: make(map[ActorID]*receiver),
		engineCtx: engineCtx,
	}
}

func (t *Topic) subscribe(actorID ActorID, rcv Receiver) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.receivers[actorID] = &receiver{
		impl: rcv,
		ch:   make(chan Message, 64),
	}
}

func (t *Topic) startBusLoop(ctx context.Context) {
	wg := sync.WaitGroup{}

	wg.Go(func() {
		t.startBroadcastLoop(ctx)
	})

	t.mu.RLock()
	for actorID, rcv := range t.receivers {
		wg.Go(func() {
			t.startReceiverLoop(ctx, actorID, rcv)
		})
	}
	t.mu.RUnlock()

	wg.Wait()
}

func (t *Topic) startBroadcastLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-t.msgChan:
			t.broadcast(ctx, msg)
		}
	}
}

func (t *Topic) broadcast(ctx context.Context, msg Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, rcv := range t.receivers {
		select {
		case rcv.ch <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (t *Topic) startReceiverLoop(ctx context.Context, actorID ActorID, rcv *receiver) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-rcv.ch:
			if err := rcv.impl.Receive(t.engineCtx, msg); err != nil {
				// TODO: update task status, retry/dead-letter via PersistenceDB
				continue
			}
			// TODO: mark task completed via PersistenceDB
		}
	}
}

func (t *Topic) publish(ctx context.Context, msg Message) error {
	// TODO: persist message + create tasks via PersistenceDB

	select {
	case t.msgChan <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

type engineBus struct {
	topics map[TopicID]*Topic
}

func (eb *engineBus) linkReceiverWithTopics(engineCtx EngineCtx, actorID ActorID, r Receiver, topics ...TopicID) {
	for _, topic := range topics {
		if _, ok := eb.topics[topic]; !ok {
			eb.topics[topic] = createTopic(topic, engineCtx)
		}

		eb.topics[topic].subscribe(actorID, r)
	}
}

func (eb *engineBus) publishMsg(ctx context.Context, msg Message) error {
	topic := msg.Topic()
	if _, ok := eb.topics[topic]; !ok {
		return fmt.Errorf("topic not found: %s", topic)
	}

	return eb.topics[topic].publish(ctx, msg)
}

func (eb *engineBus) start(ctx context.Context) {
	for _, topic := range eb.topics {
		go topic.startBusLoop(ctx)
	}
}
