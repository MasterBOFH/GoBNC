// Command gobnc-modlog is an example GoBNC module: it follows every network and prints each
// message and notice as "<time> <network> <buffer> <raw>". Configure it in gobnc.json:
//
//	"modules": {"list": [{"name": "modlog", "exec": "/path/to/gobnc-modlog", "features": ["networks", "events"]}]}
//
// Run by hand, it needs GOBNC_MODULE_SOCKET and GOBNC_MODULE_NAME (or -socket / -name).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func main() {
	socket := flag.String("socket", "", "module socket (default $GOBNC_MODULE_SOCKET)")
	name := flag.String("name", "", "module name (default $GOBNC_MODULE_NAME)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, moduleapi.Options{Socket: *socket, Module: *name}, os.Stdout); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "gobnc-modlog:", err)
		os.Exit(1)
	}
}

// logger prints line events, naming networks from networks.list / network events.
type logger struct {
	out   io.Writer
	mu    sync.Mutex
	names map[int64]string
}

func run(ctx context.Context, opts moduleapi.Options, out io.Writer) error {
	opts.Features = []string{wire.FeatureNetworks, wire.FeatureEvents}
	c, err := moduleapi.Dial(ctx, opts)
	if err != nil {
		return err
	}
	defer c.Close()
	l := &logger{out: out, names: map[int64]string{}}
	go l.consume(ctx, c)
	return c.Run(ctx, l.subscribeAll)
}

// subscribeAll (re)subscribes to every network; Run calls it after each (re)connect.
func (l *logger) subscribeAll(ctx context.Context, c *moduleapi.Client) error {
	var nets wire.NetworksListResult
	if err := c.Call(ctx, wire.MethodNetworksList, wire.Empty{}, &nets); err != nil {
		return err
	}
	for _, n := range nets.Networks {
		if err := l.subscribe(ctx, c, n.ID, n.Name); err != nil {
			return err
		}
	}
	return nil
}

func (l *logger) subscribe(ctx context.Context, c *moduleapi.Client, id int64, name string) error {
	l.mu.Lock()
	l.names[id] = name
	l.mu.Unlock()
	return c.Call(ctx, wire.MethodEventsSubscribe, wire.SubscribeParams{Network: id,
		Kinds: []string{wire.LineMessage, wire.LineNotice}}, nil)
}

func (l *logger) consume(ctx context.Context, c *moduleapi.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-c.Events():
			switch ev.Event {
			case wire.EventLine:
				var line wire.LineEvent
				if json.Unmarshal(ev.Data, &line) == nil {
					l.print(line)
				}
			case wire.EventNetwork:
				var n wire.NetworkEvent
				if json.Unmarshal(ev.Data, &n) == nil && n.Change == wire.ChangeAdded {
					go func() { _ = l.subscribe(ctx, c, n.Network, n.Name) }()
				}
			}
		}
	}
}

func (l *logger) print(line wire.LineEvent) {
	buffer := ""
	if len(line.Buffers) > 0 {
		buffer = line.Buffers[0].Name
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, "%s %s %s %s\n", line.Time, l.names[line.Network], buffer, line.Raw)
}
