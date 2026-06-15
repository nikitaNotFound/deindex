package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
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
	MessageID  MessageID
	ReceiverID ActorID
	Status     MessageHandlingStatus
	Retries    int
	CreatedAt  time.Time
}

type MessageID uuid.UUID

type Message struct {
	payload         any
	id              MessageID
	topic           TopicID
	retryIntervalFn func(retries int) time.Duration
}

func (m *Message) ID() MessageID {
	return m.id
}

func (m *Message) Topic() TopicID {
	return m.topic
}

func (m *Message) Payload() any {
	return m.payload
}

func (m *Message) GetRetryInterval(retries int) time.Duration {
	if m.retryIntervalFn == nil {
		return time.Duration(retries) * time.Second * 30
	}

	return m.retryIntervalFn(retries)
}

type TopicProvider interface {
	Topic() TopicID
}

type RetryIntervalProvider interface {
	RetryInterval(retries int) time.Duration
}

func CreateMessage(message TopicProvider) Message {
	msg := Message{}
	if retryIntervalProvider, ok := message.(RetryIntervalProvider); ok {
		msg.retryIntervalFn = retryIntervalProvider.RetryInterval
	}

	id := uuid.New()
	msg.id = MessageID(id)
	msg.topic = message.Topic()
	msg.payload = message

	return msg
}

type TopicID string

type receiver struct {
	impl rawReceiver
	ch   chan Message
}

func (r *receiver) send(ctx context.Context, msg Message) {
	select {
	case r.ch <- msg:
	case <-ctx.Done():
	}
}

type Topic struct {
	cfg       EngineConfig
	id        TopicID
	receivers map[ActorID]*receiver
	engineCtx EngineCtx
	db        EngineDB

	mu sync.RWMutex
}

func createTopic(id TopicID, cfg EngineConfig, engineCtx EngineCtx, db EngineDB) *Topic {
	return &Topic{
		cfg:       cfg,
		id:        id,
		receivers: make(map[ActorID]*receiver),
		engineCtx: engineCtx,
		db:        db,
	}
}

func (t *Topic) subscribe(actorID ActorID, rcv rawReceiver) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.receivers[actorID] = &receiver{
		impl: rcv,
		ch:   make(chan Message, t.cfg.ReceiverBufferSize),
	}
}

func (t *Topic) startBusLoop(ctx context.Context) {
	wg := sync.WaitGroup{}

	t.mu.RLock()
	for actorID, rcv := range t.receivers {
		wg.Go(func() {
			t.startReceiverLoop(ctx, actorID, rcv)
		})
	}
	t.mu.RUnlock()

	wg.Wait()
}

func (t *Topic) broadcast(ctx context.Context, msg Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, rcv := range t.receivers {
		rcv.send(ctx, msg)
	}
}

func (t *Topic) startReceiverLoop(ctx context.Context, actorID ActorID, rcv *receiver) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-rcv.ch:
			if err := t.handleMessage(ctx, actorID, rcv, msg); err != nil {
				log.Printf("failed to handle message %s for receiver %s: %v", msg.ID(), actorID, err)
			}
		}
	}
}

func (t *Topic) handleMessage(ctx context.Context, actorID ActorID, rcv *receiver, msg Message) error {
	handling, err := t.getHandling(ctx, actorID, msg.ID())
	if err != nil {
		log.Printf("failed to get handling for %s/%s: %v", actorID, msg.ID(), err)
		return err
	}

	if handling == nil {
		return fmt.Errorf("handling not found for %s/%s", actorID, msg.ID())
	}

	if err := rcv.impl.receive(t.engineCtx, msg); err != nil {
		retries := handling.Retries + 1
		if retries >= t.cfg.MaxRetries {
			return t.db.UpdateHandlingStatus(ctx, UpdateHandlingParams{
				TopicID:    t.id,
				MessageID:  msg.ID(),
				ReceiverID: actorID,
				Status:     MessageHandlingStatusDeadLetter,
				Retries:    retries,
			})
		}

		executeAt := time.Now().Add(msg.GetRetryInterval(retries))
		if err := t.db.UpdateHandlingStatus(ctx, UpdateHandlingParams{
			TopicID:    t.id,
			MessageID:  msg.ID(),
			ReceiverID: actorID,
			Status:     MessageHandlingStatusProcessing,
			Retries:    retries,
			ExecuteAt:  &executeAt,
		}); err != nil {
			return err
		}

		return nil
	}

	return t.db.UpdateHandlingStatus(ctx, UpdateHandlingParams{
		TopicID:    t.id,
		MessageID:  msg.ID(),
		ReceiverID: actorID,
		Status:     MessageHandlingStatusCompleted,
		Retries:    handling.Retries,
	})
}

func (t *Topic) getHandling(ctx context.Context, actorID ActorID, msgID MessageID) (*MessageHandling, error) {
	return t.db.GetHandling(ctx, t.id, msgID, actorID)
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

type engineBus struct {
	cfg    EngineConfig
	topics map[TopicID]*Topic
	db     EngineDB
}

func (eb *engineBus) linkReceiverWithTopic(engineCtx EngineCtx, actorID ActorID, r rawReceiver, topic TopicID) {
	if _, ok := eb.topics[topic]; !ok {
		eb.topics[topic] = createTopic(topic, eb.cfg, engineCtx, eb.db)
	}

	eb.topics[topic].subscribe(actorID, r)
}

func (eb *engineBus) publishMsg(ctx context.Context, msg Message) error {
	topicID := msg.Topic()
	topic, ok := eb.topics[topicID]
	if !ok {
		return fmt.Errorf("topic %s not found", topicID)
	}

	if err := eb.db.WrapTx(ctx, func(ctx context.Context) error {
		if err := eb.db.CreateMessage(ctx, topic.id, msg, topic.receiverIDs()); err != nil {
			return err
		}

		for _, rcvID := range topic.receiverIDs() {
			if err := eb.db.CreateHandling(ctx, topic.id, msg.ID(), rcvID); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("publish message to topic %s: %w", topic.id, err)
	}

	topic.broadcast(ctx, msg)

	return nil
}

func (eb *engineBus) start(ctx context.Context) {
	wg := sync.WaitGroup{}
	for _, topic := range eb.topics {
		wg.Go(func() {
			topic.startBusLoop(ctx)
		})
	}

	wg.Wait()
}
