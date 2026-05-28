package engine

import (
	"github.com/google/uuid"
)

type ActorID string

type Producer interface {
	Start(engineCtx EngineCtx) error
}

type Receiver interface {
	Receive(engineCtx EngineCtx, msg Message) error
}

type ProducerReceiver interface {
	Producer
	Receiver
}

type Actor struct {
	id       ActorID
	producer Producer
	receiver Receiver

	listenedTopics []TopicID
}

func (a *Actor) ID() ActorID {
	return a.id
}

func (a *Actor) GetProducer() Producer {
	return a.producer
}

func (a *Actor) GetReceiver() Receiver {
	return a.receiver
}

func (a *Actor) HasProducer() bool {
	return a.producer != nil
}

func (a *Actor) HasReceiver() bool {
	return a.receiver != nil
}

func (a *Actor) GetListenedTopics() []TopicID {
	return a.listenedTopics
}

type CreateActorParams struct {
	customID ActorID
}
type CreateActorOpt func(*CreateActorParams)

type CreateReceiverActorParams struct {
	customID       ActorID
	listenedTopics []TopicID
}
type CreateReceiverActorOpt func(*CreateReceiverActorParams)

func WithCustomID(id ActorID) CreateActorOpt {
	return func(params *CreateActorParams) {
		params.customID = id
	}
}

func WithListenedTopics(topics ...TopicID) CreateReceiverActorOpt {
	return func(params *CreateReceiverActorParams) {
		params.listenedTopics = topics
	}
}

func CreateActor(impl ProducerReceiver, opts ...CreateReceiverActorOpt) *Actor {
	params := handleCreateReceiverParams(opts...)

	return &Actor{
		id:             params.customID,
		producer:       impl,
		receiver:       impl,
		listenedTopics: params.listenedTopics,
	}
}

func CreateProducerActor(impl Producer, opts ...CreateActorOpt) *Actor {
	params := handleCreateParams(opts...)

	return &Actor{
		id:       params.customID,
		producer: impl,
		receiver: nil,
	}
}

func CreateReceiverActor(impl Receiver, opts ...CreateReceiverActorOpt) *Actor {
	params := handleCreateReceiverParams(opts...)

	return &Actor{
		id:             params.customID,
		producer:       nil,
		receiver:       impl,
		listenedTopics: params.listenedTopics,
	}
}

func handleCreateParams(opts ...CreateActorOpt) *CreateActorParams {
	id := ActorID(uuid.New().String())
	params := &CreateActorParams{
		customID: id,
	}
	for _, opt := range opts {
		opt(params)
	}

	return params
}

func handleCreateReceiverParams(opts ...CreateReceiverActorOpt) *CreateReceiverActorParams {
	id := ActorID(uuid.New().String())
	params := &CreateReceiverActorParams{
		customID:       id,
		listenedTopics: []TopicID{},
	}
	for _, opt := range opts {
		opt(params)
	}

	return params

}
