package threadgroup

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestThreadgroup(t *testing.T) {
	tg := New()

	for i := 0; i < 10; i++ {
		done, err := tg.Add()
		if err != nil {
			t.Fatal(err)
		}
		time.AfterFunc(100*time.Millisecond, done)
	}
	start := time.Now()
	tg.Stop()
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("expected stop to wait for all threads to complete")
	}

	_, err := tg.Add()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestThreadgroupContext(t *testing.T) {
	tg := New()

	t.Run("context cancel", func(t *testing.T) {
		ctx, cancel, err := tg.AddContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		time.AfterFunc(100*time.Millisecond, cancel)

		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("expected Canceled, got %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("expected context to be cancelled")
		}
	})

	t.Run("parent cancel", func(t *testing.T) {
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer parentCancel()

		ctx, cancel, err := tg.AddContext(parentCtx)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected DeadlineExceeded, got %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("expected context to be cancelled")
		}
	})

	t.Run("stop", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			_, cancel, err := tg.AddContext(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			time.AfterFunc(100*time.Millisecond, cancel)
		}

		start := time.Now()
		tg.Stop()
		if time.Since(start) < 100*time.Millisecond {
			t.Fatal("expected threadgroup to wait until all threads complete")
		}
	})
}
