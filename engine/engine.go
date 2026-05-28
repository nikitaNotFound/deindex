package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type EngineCtx struct {
	ctx context.Context
	bus *engineBus
}

func (e *EngineCtx) Context() context.Context {
	return e.ctx
}

func (e *EngineCtx) PublishMsg(ctx context.Context, msg Message) error {
	return e.bus.publishMsg(ctx, msg)
}

type Engine struct {
	engineCtx     EngineCtx
	executionCtx  context.Context
	bus           *engineBus
	persistenceDB PersistenceDB

	actors map[ActorID]*Actor
}

func NewEngine(persistenceDB PersistenceDB) *Engine {
	bus := &engineBus{
		topics:        make(map[TopicID]*Topic),
		persistenceDB: persistenceDB,
	}

	return &Engine{
		persistenceDB: persistenceDB,
		actors:        make(map[ActorID]*Actor),
		bus:           bus,
	}
}

func (e *Engine) RegisterActor(actor *Actor) error {
	if actor.HasProducer() {
		e.actors[actor.ID()] = actor
	}
	if actor.HasReceiver() {
		e.actors[actor.ID()] = actor
	}

	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	engineCtx := EngineCtx{
		ctx: ctx,
		bus: e.bus,
	}
	e.engineCtx = engineCtx

	for _, actor := range e.actors {
		if actor.HasReceiver() {
			e.bus.linkReceiverWithTopics(e.engineCtx, actor.ID(), actor.GetReceiver(), actor.GetListenedTopics()...)
		}
	}

	if err := e.recoverUnfinishedHandlings(ctx); err != nil {
		return fmt.Errorf("recover unfinished handlings: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, actor := range e.actors {
		if actor.HasProducer() {
			wg.Go(func() {
				if err := actor.GetProducer().Start(e.engineCtx); err != nil {
					log.Printf("failed to start producer: %v", err)
				}
			})
		}
	}
	wg.Go(func() {
		e.bus.start(ctx)
	})

	wg.Wait()

	return nil
}

func (e *Engine) recoverUnfinishedHandlings(ctx context.Context) error {
	for _, actor := range e.actors {
		if !actor.HasReceiver() {
			continue
		}

		handlings, err := e.persistenceDB.GetUnfinishedHandlings(ctx, actor.ID())
		if err != nil {
			return fmt.Errorf("get unfinished handlings for %s: %w", actor.ID(), err)
		}

		if len(handlings) == 0 {
			continue
		}

		log.Printf("recovering %d unfinished handlings for receiver %s", len(handlings), actor.ID())

		for _, h := range handlings {
			topic, ok := e.bus.topics[h.TopicID]
			if !ok {
				log.Printf("skipping handling %s: topic %s not found", h.ID, h.TopicID)
				continue
			}

			msg, err := e.persistenceDB.GetMessage(ctx, h.TopicID, h.MessageID)
			if err != nil {
				log.Printf("skipping handling %s: failed to get message %s: %v", h.ID, h.MessageID, err)
				continue
			}

			topic.mu.RLock()
			rcv, ok := topic.receivers[actor.ID()]
			topic.mu.RUnlock()
			if !ok {
				log.Printf("skipping handling %s: receiver %s not subscribed to topic %s", h.ID, actor.ID(), h.TopicID)
				continue
			}

			select {
			case rcv.ch <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}
