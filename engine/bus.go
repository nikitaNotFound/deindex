package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type MessageHandlingStatus int

const (
	MessageHandlingStatusNew MessageHandlingStatus = iota
	MessageHandlingStatusProcessing
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
	id            TopicID
	msgChan       chan Message
	receivers     map[ActorID]*receiver
	engineCtx     EngineCtx
	persistenceDB PersistenceDB

	mu sync.RWMutex
}

func createTopic(id TopicID, engineCtx EngineCtx, persistenceDB PersistenceDB) *Topic {
	return &Topic{
		id:            id,
		msgChan:       make(chan Message, 64),
		receivers:     make(map[ActorID]*receiver),
		engineCtx:     engineCtx,
		persistenceDB: persistenceDB,
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

const maxRetries = 5

func (t *Topic) startReceiverLoop(ctx context.Context, actorID ActorID, rcv *receiver) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-rcv.ch:
			t.handleMessage(ctx, actorID, rcv, msg)
		}
	}
}

func (t *Topic) handleMessage(ctx context.Context, actorID ActorID, rcv *receiver, msg Message) {
	db := t.persistenceDB

	_ = db.UpdateHandlingStatus(ctx, t.id, msg.ID(), actorID, MessageHandlingStatusProcessing, 0)

	if err := rcv.impl.Receive(t.engineCtx, msg); err != nil {
		handling, hErr := t.getHandling(ctx, actorID, msg.ID())
		if hErr != nil {
			log.Printf("failed to get handling for %s/%s: %v", actorID, msg.ID(), hErr)
			return
		}

		retries := handling.Retries + 1
		if retries >= maxRetries {
			_ = db.UpdateHandlingStatus(ctx, t.id, msg.ID(), actorID, MessageHandlingStatusDeadLetter, retries)
			log.Printf("dead-lettered message %s for receiver %s after %d retries", msg.ID(), actorID, retries)
			return
		}

		_ = db.UpdateHandlingStatus(ctx, t.id, msg.ID(), actorID, MessageHandlingStatusProcessing, retries)
		select {
		case rcv.ch <- msg:
		case <-ctx.Done():
		}
		return
	}

	_ = db.UpdateHandlingStatus(ctx, t.id, msg.ID(), actorID, MessageHandlingStatusCompleted, 0)
}

func (t *Topic) getHandling(ctx context.Context, actorID ActorID, msgID string) (*MessageHandling, error) {
	handlings, err := t.persistenceDB.GetUnfinishedHandlings(ctx, actorID)
	if err != nil {
		return nil, err
	}
	for i := range handlings {
		if handlings[i].MessageID == msgID && handlings[i].TopicID == t.id {
			return &handlings[i], nil
		}
	}
	return nil, fmt.Errorf("handling not found for topic=%s msg=%s receiver=%s", t.id, msgID, actorID)
}

func (t *Topic) receiverIDs() []ActorID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ids := make([]ActorID, 0, len(t.receivers))
	for id := range t.receivers {
		ids = append(ids, id)
	}
	return ids
}

func (t *Topic) publish(ctx context.Context, msg Message) error {
	if err := t.persistenceDB.PublishMessage(ctx, t.id, msg, t.receiverIDs()); err != nil {
		return fmt.Errorf("persist message: %w", err)
	}

	select {
	case t.msgChan <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

type engineBus struct {
	topics        map[TopicID]*Topic
	persistenceDB PersistenceDB
}

func (eb *engineBus) linkReceiverWithTopics(engineCtx EngineCtx, actorID ActorID, r Receiver, topics ...TopicID) {
	for _, topic := range topics {
		if _, ok := eb.topics[topic]; !ok {
			eb.topics[topic] = createTopic(topic, engineCtx, eb.persistenceDB)
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
