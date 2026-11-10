// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// podman-healthcheck-scheduler runs the health checks of Podman containers on
// systems without systemd.
//
// Podman schedules health checks with systemd timers that run
// "podman healthcheck run <id>" every interval. Built without systemd, as in
// the WSLC VM, Podman never runs them and containers stay "starting".
//
// This program takes the place of those timers for a Podman API service: for
// each running container with a health check, it asks the service to run the
// check every interval. Podman still runs the check, picks the startup or the
// regular check, records the result, emits the health_status event and applies
// the on-failure action.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// apiBase is the libpod API prefix. The host part is ignored: requests go to the Unix socket.
const apiBase = "http://podman/v5.0.0/libpod"

type client struct {
	http *http.Client
}

func newClient(socket string) *client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}
	return &client{http: &http.Client{Transport: transport}}
}

// statusError is returned for a response that isn't 200.
type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.code, e.body)
}

func (c *client) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, &statusError{code: resp.StatusCode, body: string(body)}
	}
	return resp, nil
}

func (c *client) getJSON(ctx context.Context, path string, v any) error {
	resp, err := c.get(ctx, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

type healthConfig struct {
	Test     []string
	Interval time.Duration // nanoseconds in JSON, like time.Duration
}

// container holds the parts of the libpod inspect output used here.
type container struct {
	ID    string `json:"Id"`
	State struct {
		Running bool
		Paused  bool
		Health  *struct {
			Status string
		}
	}
	Config struct {
		Healthcheck        *healthConfig
		StartupHealthCheck *healthConfig
	}
}

func (c *client) inspect(ctx context.Context, id string) (*container, error) {
	var ctr container
	if err := c.getJSON(ctx, "/containers/"+url.PathEscape(id)+"/json", &ctr); err != nil {
		return nil, err
	}
	return &ctr, nil
}

func (c *client) runHealthCheck(ctx context.Context, id string) error {
	resp, err := c.get(ctx, "/containers/"+url.PathEscape(id)+"/healthcheck")
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// hasCheck mirrors Podman's HasHealthCheck: a test that isn't empty or NONE.
func hasCheck(hc *healthConfig) bool {
	return hc != nil && len(hc.Test) > 0 && hc.Test[0] != "NONE"
}

// interval returns how long to wait before the next check, or 0 if the
// container needs no checks. Podman only starts the timer for a container with
// a regular health check, and not for an interval of 0.
//
// Podman uses the startup check's interval until the startup check passes.
// That isn't visible through the API, but the status stays "starting" until
// the first regular check runs, so this uses the startup interval until then:
// at most one regular check runs at the startup interval.
func interval(ctr *container) time.Duration {
	if !ctr.State.Running || ctr.State.Paused || !hasCheck(ctr.Config.Healthcheck) {
		return 0
	}
	if hasCheck(ctr.Config.StartupHealthCheck) && ctr.State.Health != nil && ctr.State.Health.Status == "starting" {
		return ctr.Config.StartupHealthCheck.Interval
	}
	return ctr.Config.Healthcheck.Interval
}

type scheduler struct {
	client *client
	mu     sync.Mutex
	jobs   map[string]context.CancelFunc
}

func newScheduler(c *client) *scheduler {
	return &scheduler{client: c, jobs: map[string]context.CancelFunc{}}
}

// schedule (re)starts the checks of a container, like Podman (re)creates its
// timer when the container starts or is unpaused.
func (s *scheduler) schedule(ctx context.Context, id string) {
	s.cancel(id)

	ctr, err := s.client.inspect(ctx, id)
	if err != nil {
		var se *statusError
		if !errors.As(err, &se) || se.code != http.StatusNotFound {
			log.Printf("inspecting container %s: %v", id, err)
		}
		return
	}
	every := interval(ctr)
	if every <= 0 {
		return
	}

	jobCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.jobs[ctr.ID] = cancel
	s.mu.Unlock()
	go s.run(jobCtx, ctr.ID, every)
}

func (s *scheduler) cancel(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel, ok := s.jobs[id]; ok {
		cancel()
		delete(s.jobs, id)
	}
}

func (s *scheduler) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, cancel := range s.jobs {
		cancel()
		delete(s.jobs, id)
	}
}

// run checks the container every interval, counted from the end of the previous
// check like the systemd timer (OnUnitInactiveSec), until it's cancelled or the
// container no longer needs checks.
func (s *scheduler) run(ctx context.Context, id string, every time.Duration) {
	timer := time.NewTimer(every)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		if err := s.client.runHealthCheck(ctx, id); err != nil {
			if ctx.Err() != nil {
				return
			}
			var se *statusError
			if errors.As(err, &se) && (se.code == http.StatusNotFound || se.code == http.StatusConflict) {
				// The container is gone or stopped: its events cancel this job.
				return
			}
			log.Printf("health check of container %s: %v", id, err)
		}

		ctr, err := s.client.inspect(ctx, id)
		if err != nil {
			return
		}
		if every = interval(ctr); every <= 0 {
			return
		}
		timer.Reset(every)
	}
}

type event struct {
	Type   string
	Action string
	Actor  struct {
		ID string
	}
}

// handle updates the jobs for a container event.
func (s *scheduler) handle(ctx context.Context, ev event) {
	if ev.Type != "container" || ev.Actor.ID == "" {
		return
	}
	switch ev.Action {
	case "start", "restart", "unpause":
		s.schedule(ctx, ev.Actor.ID)
	case "died", "stop", "pause", "remove":
		s.cancel(ev.Actor.ID)
	}
}

// watch follows the container events until the stream ends, and reports
// whether it got as far as subscribing. It subscribes first and then schedules
// the containers that are already running, so no start is missed in between.
func (s *scheduler) watch(ctx context.Context) (bool, error) {
	filters := url.QueryEscape(`{"type":["container"]}`)
	resp, err := s.client.get(ctx, "/events?stream=true&filters="+filters)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var running []struct {
		ID string `json:"Id"`
	}
	if err := s.client.getJSON(ctx, "/containers/json", &running); err != nil {
		return true, err
	}
	for _, ctr := range running {
		s.schedule(ctx, ctr.ID)
	}

	dec := json.NewDecoder(resp.Body)
	for {
		var ev event
		if err := dec.Decode(&ev); err != nil {
			return true, err
		}
		s.handle(ctx, ev)
	}
}

func main() {
	socket := flag.String("socket", "/run/podman/podman.sock", "Podman API socket")
	flag.Parse()
	log.SetFlags(0)
	log.SetPrefix("podman-healthcheck-scheduler: ")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := newScheduler(newClient(*socket))
	logFailure := true
	for {
		subscribed, err := s.watch(ctx)
		s.cancelAll()
		if ctx.Err() != nil {
			return
		}
		// The service may not be listening yet, or may have restarted: retry,
		// logging only the first failure until a subscription works again.
		if subscribed {
			logFailure = true
		}
		if logFailure {
			log.Printf("watching %s: %v (retrying)", *socket, err)
			logFailure = false
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
