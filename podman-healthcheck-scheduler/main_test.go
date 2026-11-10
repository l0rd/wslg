// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newContainer(id string, running bool, regular, startup *healthConfig, status string) *container {
	ctr := &container{ID: id}
	ctr.State.Running = running
	if status != "" {
		ctr.State.Health = &struct{ Status string }{Status: status}
	}
	ctr.Config.Healthcheck = regular
	ctr.Config.StartupHealthCheck = startup
	return ctr
}

func TestInterval(t *testing.T) {
	regular := &healthConfig{Test: []string{"CMD-SHELL", "true"}, Interval: 30 * time.Second}
	startup := &healthConfig{Test: []string{"CMD-SHELL", "true"}, Interval: time.Second}
	paused := newContainer("p", true, regular, nil, "starting")
	paused.State.Paused = true

	tests := []struct {
		name string
		ctr  *container
		want time.Duration
	}{
		{"regular check", newContainer("a", true, regular, nil, "starting"), 30 * time.Second},
		{"not running", newContainer("a", false, regular, nil, ""), 0},
		{"paused", paused, 0},
		{"no check", newContainer("a", true, nil, nil, ""), 0},
		{"empty test", newContainer("a", true, &healthConfig{Interval: time.Second}, nil, ""), 0},
		{"NONE", newContainer("a", true, &healthConfig{Test: []string{"NONE"}, Interval: time.Second}, nil, ""), 0},
		{"interval disabled", newContainer("a", true, &healthConfig{Test: []string{"CMD", "true"}}, nil, "starting"), 0},
		{"startup check pending", newContainer("a", true, regular, startup, "starting"), time.Second},
		{"startup check passed", newContainer("a", true, regular, startup, "healthy"), 30 * time.Second},
		{"startup check only", newContainer("a", true, nil, startup, ""), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := interval(tt.ctr); got != tt.want {
				t.Errorf("interval() = %v, want %v", got, tt.want)
			}
		})
	}
}

// fakePodman serves the parts of the libpod API the scheduler uses.
type fakePodman struct {
	mu         sync.Mutex
	containers map[string]*container
	checks     map[string]int
	events     chan event
}

func (f *fakePodman) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v5.0.0/libpod")
	switch {
	case path == "/events":
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		enc := json.NewEncoder(w)
		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-f.events:
				_ = enc.Encode(ev)
				w.(http.Flusher).Flush()
			}
		}
	case path == "/containers/json":
		f.mu.Lock()
		var list []map[string]string
		for id, ctr := range f.containers {
			if ctr.State.Running {
				list = append(list, map[string]string{"Id": id})
			}
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(list)
	case strings.HasSuffix(path, "/json"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/json")
		f.mu.Lock()
		ctr, ok := f.containers[id]
		var body []byte
		if ok {
			body, _ = json.Marshal(ctr)
		}
		f.mu.Unlock()
		if !ok {
			http.Error(w, "no such container", http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	case strings.HasSuffix(path, "/healthcheck"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/healthcheck")
		f.mu.Lock()
		defer f.mu.Unlock()
		ctr, ok := f.containers[id]
		if !ok || !ctr.State.Running {
			http.Error(w, "container is stopped", http.StatusConflict)
			return
		}
		f.checks[id]++
		// Like Podman, the first regular check moves the status out of "starting".
		ctr.State.Health = &struct{ Status string }{Status: "healthy"}
		_, _ = w.Write([]byte(`{"Status":"healthy"}`))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakePodman) setRunning(id string, running bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[id].State.Running = running
}

func (f *fakePodman) checkCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checks[id]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestScheduler(t *testing.T) {
	check := &healthConfig{Test: []string{"CMD-SHELL", "true"}, Interval: 50 * time.Millisecond}
	fake := &fakePodman{
		containers: map[string]*container{
			"running": newContainer("running", true, check, nil, "starting"),
			"later":   newContainer("later", false, check, nil, ""),
			"nocheck": newContainer("nocheck", true, nil, nil, ""),
		},
		checks: map[string]int{},
		events: make(chan event),
	}

	socket := filepath.Join(t.TempDir(), "podman.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: fake}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newScheduler(newClient(socket))
	go s.watch(ctx)

	// A container that was already running is checked repeatedly.
	waitFor(t, "repeated checks of the running container", func() bool { return fake.checkCount("running") >= 3 })

	// A container that starts later is checked once its start event arrives.
	if n := fake.checkCount("later"); n != 0 {
		t.Fatalf("container checked %d times before it started", n)
	}
	fake.setRunning("later", true)
	start := event{Type: "container", Action: "start"}
	start.Actor.ID = "later"
	fake.events <- start
	waitFor(t, "checks of the started container", func() bool { return fake.checkCount("later") >= 2 })

	// A container that dies is no longer checked. The fake still reports it as
	// running, so only the event can stop the checks.
	died := event{Type: "container", Action: "died"}
	died.Actor.ID = "running"
	fake.events <- died
	time.Sleep(100 * time.Millisecond)
	stopped := fake.checkCount("running")
	time.Sleep(200 * time.Millisecond)
	if n := fake.checkCount("running"); n != stopped {
		t.Errorf("container still checked after it died: %d checks, then %d", stopped, n)
	}

	if n := fake.checkCount("nocheck"); n != 0 {
		t.Errorf("container without a health check was checked %d times", n)
	}
}
