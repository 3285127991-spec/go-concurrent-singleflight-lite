// Command sfdemo demonstrates the singleflightlite Group: same-key call
// merging, follower cancellation, and Forget. All coordination uses
// channels, so the output is deterministic.
package main

import (
	"context"
	"fmt"
	"runtime"

	"singleflightlite"
)

func waitFor(cond func() bool) {
	for !cond() {
		runtime.Gosched()
	}
}

func demoMerge() {
	fmt.Println("== demo 1: same-key calls merge into one execution ==")
	g := singleflightlite.NewGroup()
	entered := make(chan struct{})
	release := make(chan struct{})
	executions := make(chan int, 8)

	fn := func(ctx context.Context) (any, error) {
		executions <- 1
		close(entered)
		<-release
		return "user:42 profile", nil
	}

	type result struct {
		id     int
		val    any
		shared bool
		err    error
	}
	results := make(chan result, 4)
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			v, shared, err := g.Do(context.Background(), "user:42", fn)
			results <- result{i, v, shared, err}
		}()
	}
	<-entered
	waitFor(func() bool {
		s := g.Snapshot()
		return len(s) == 1 && s[0].Waiters == 4
	})
	fmt.Printf("in-flight: %+v\n", g.Snapshot())
	close(release)
	for i := 0; i < 4; i++ {
		r := <-results
		fmt.Printf("caller %d: value=%q shared=%v err=%v\n", r.id, r.val, r.shared, r.err)
	}
	fmt.Printf("fn executions: %d (want 1)\n\n", len(executions))
}

func demoCancel() {
	fmt.Println("== demo 2: follower cancel leaves the leader running ==")
	g := singleflightlite.NewGroup()
	entered := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		close(entered)
		<-release
		return "leader result", nil
	}

	leader := make(chan any, 1)
	go func() {
		v, _, err := g.Do(context.Background(), "report", fn)
		if err != nil {
			leader <- err
		} else {
			leader <- v
		}
	}()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	follower := make(chan error, 1)
	go func() {
		_, _, err := g.Do(ctx, "report", fn)
		follower <- err
	}()
	waitFor(func() bool {
		s := g.Snapshot()
		return len(s) == 1 && s[0].Waiters == 2
	})

	cancel()
	fmt.Printf("follower returned early with: %v\n", <-follower)
	fmt.Printf("leader still in flight: %+v\n", g.Snapshot())

	close(release)
	fmt.Printf("leader finished with: %v\n\n", <-leader)
}

func demoForget() {
	fmt.Println("== demo 3: Forget starts a new generation immediately ==")
	g := singleflightlite.NewGroup()
	entered1, release1 := make(chan struct{}), make(chan struct{})
	old := make(chan any, 1)
	go func() {
		v, _, _ := g.Do(context.Background(), "config", func(ctx context.Context) (any, error) {
			close(entered1)
			<-release1
			return "config v1", nil
		})
		old <- v
	}()
	<-entered1
	fmt.Printf("before Forget: %+v\n", g.Snapshot())

	g.Forget("k") // unknown key: no-op
	g.Forget("config")
	fmt.Printf("after Forget:  %+v (Len=%d)\n", g.Snapshot(), g.Len())

	entered2, release2 := make(chan struct{}), make(chan struct{})
	fresh := make(chan any, 1)
	go func() {
		v, _, _ := g.Do(context.Background(), "config", func(ctx context.Context) (any, error) {
			close(entered2)
			<-release2
			return "config v2", nil
		})
		fresh <- v
	}()
	<-entered2 // new generation started while the old one is still blocked
	fmt.Printf("new generation: %+v\n", g.Snapshot())

	close(release1)
	fmt.Printf("old flight finished with: %v (Len still %d)\n", <-old, g.Len())
	close(release2)
	fmt.Printf("new flight finished with: %v (Len=%d)\n", <-fresh, g.Len())
}

func main() {
	demoMerge()
	demoCancel()
	demoForget()
}
