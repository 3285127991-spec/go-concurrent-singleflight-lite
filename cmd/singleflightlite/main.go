// Command singleflightlite is a minimal demo of the singleflightlite
// library: same-key call merging, follower cancellation and Forget.
package main

import (
	"context"
	"fmt"
	"time"

	sf "singleflightlite"
)

func main() {
	demoMerge()
	demoCancel()
	demoForget()
}

// demoMerge: three concurrent Do calls on the same key run fn only once.
func demoMerge() {
	fmt.Println("== same-key merge ==")
	var g sf.Group
	fn := func(ctx context.Context) (any, error) {
		time.Sleep(50 * time.Millisecond) // pretend to be expensive
		return "result", nil
	}
	done := make(chan struct{})
	for i := 0; i < 3; i++ {
		go func(id int) {
			v, shared, err := g.Do(context.Background(), "key", fn)
			fmt.Printf("  caller %d: value=%v shared=%v err=%v\n", id, v, shared, err)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 3; i++ {
		<-done
	}
	fmt.Println("  fn executed exactly once; two callers shared the result")
}

// demoCancel: a cancelled follower exits alone; the leader still finishes.
func demoCancel() {
	fmt.Println("== follower cancel ==")
	var g sf.Group
	fn := func(ctx context.Context) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return "leader-result", nil
	}
	leaderDone := make(chan struct{})
	go func() {
		v, shared, err := g.Do(context.Background(), "key", fn)
		fmt.Printf("  leader:   value=%v shared=%v err=%v\n", v, shared, err)
		close(leaderDone)
	}()
	time.Sleep(10 * time.Millisecond) // let the leader start the flight
	ctx, cancel := context.WithCancel(context.Background())
	followerDone := make(chan struct{})
	go func() {
		_, _, err := g.Do(ctx, "key", fn)
		fmt.Printf("  follower: err=%v (exited alone, flight untouched)\n", err)
		close(followerDone)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-followerDone
	<-leaderDone
}

// demoForget: Forget makes the next Do start a new generation immediately.
func demoForget() {
	fmt.Println("== forget ==")
	var g sf.Group
	slow := func(ctx context.Context) (any, error) {
		time.Sleep(80 * time.Millisecond)
		return "old-generation", nil
	}
	go func() {
		v, _, _ := g.Do(context.Background(), "key", slow)
		fmt.Printf("  first Do finished later: value=%v\n", v)
	}()
	time.Sleep(10 * time.Millisecond) // first flight is now running
	fmt.Printf("  in-flight before Forget: %v\n", g.Snapshot())
	g.Forget("key")
	fmt.Printf("  in-flight after Forget:  %v (Len=%d)\n", g.Snapshot(), g.Len())
	// This Do does not wait for the old flight: it starts a new generation.
	v, shared, err := g.Do(context.Background(), "key", func(ctx context.Context) (any, error) {
		return "new-generation", nil
	})
	fmt.Printf("  Do after Forget: value=%v shared=%v err=%v\n", v, shared, err)
	time.Sleep(100 * time.Millisecond) // let the old generation print
}
