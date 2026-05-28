package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type EngineCtx struct {
	ctx context.Context
	bus *engineBus
}

func (e *EngineCtx) Context() context.Context {
	return e.ctx
}

func (e *EngineCtx) Publish(ctx context.Context, msg TopicProvider) error {
	return e.bus.publishMsg(ctx, CreateMessage(msg))
}

type Engine struct {
	engineCtx     EngineCtx
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
	if actor.HasProducer() || actor.HasReceiver() {
		e.actors[actor.ID()] = actor
		return nil
	}

	return fmt.Errorf("actor %s has no producer or receiver", actor.ID())
}

func (e *Engine) RegisterActors(actors ...*Actor) error {
	for _, actor := range actors {
		if err := e.RegisterActor(actor); err != nil {
			return fmt.Errorf("register actor %s: %w", actor.ID(), err)
		}
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
	wg.Go(func() {
		e.runCleanupLoop(ctx)
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

			rcv.send(ctx, msg)
		}
	}

	return nil
}

const cleanupInterval = 10 * time.Minute

func (e *Engine) runCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.persistenceDB.DeleteExpiredMessages(ctx); err != nil {
				log.Printf("failed to delete expired messages: %v", err)
			}
		}
	}
}
