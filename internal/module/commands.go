package module

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodCommandsRegister, handleCommandsRegister, wire.FeatureCommands)
	registerMethod(wire.MethodCommandsReply, handleCommandsReply, wire.FeatureCommands)
}

// CommandTimeout is how long BNC/CLI callers wait for a module's commands.reply.
var CommandTimeout = 10 * time.Second

var commandNameRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// builtinCommands are BNC and gobnc CLI verbs a module may not take over.
var builtinCommands = map[string]bool{
	"help": true, "status": true, "network": true, "reconnect": true, "disconnect": true,
	"rehash": true, "reload": true, "die": true, "serve": true, "auth": true, "stop": true,
	"can-upgrade": true, "debug": true,
}

type commandEntry struct {
	conn *Conn
	help string
}

// commandRegistry tracks module commands and invocations waiting for a reply.
type commandRegistry struct {
	mu      sync.Mutex
	byName  map[string]commandEntry
	waiting map[string]chan []string
	seq     uint64
}

func handleCommandsRegister(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.CommandRegisterParams](params)
	if err != nil {
		return nil, err
	}
	if !commandNameRE.MatchString(p.Name) {
		return nil, wire.Errorf(wire.CodeBadParams, "command name must match [a-z][a-z0-9-]{0,31}")
	}
	if builtinCommands[p.Name] {
		return nil, wire.Errorf(wire.CodeBadParams, "%q is a built-in BNC command", p.Name)
	}
	r := &c.srv.commands
	r.mu.Lock()
	if e, taken := r.byName[p.Name]; taken && e.conn != c {
		r.mu.Unlock()
		return nil, wire.Errorf(wire.CodeBadParams, "command %q is registered by module %s", p.Name, e.conn.Module())
	}
	if r.byName == nil {
		r.byName = map[string]commandEntry{}
	}
	r.byName[p.Name] = commandEntry{conn: c, help: p.Help}
	r.mu.Unlock()
	c.OnClose(func() {
		r.mu.Lock()
		if e := r.byName[p.Name]; e.conn == c {
			delete(r.byName, p.Name)
		}
		r.mu.Unlock()
	})
	return nil, nil
}

func handleCommandsReply(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.CommandReplyParams](params)
	if err != nil {
		return nil, err
	}
	r := &c.srv.commands
	r.mu.Lock()
	ch := r.waiting[p.InvocationID]
	delete(r.waiting, p.InvocationID)
	r.mu.Unlock()
	if ch == nil {
		return nil, wire.Errorf(wire.CodeNotFound, "no pending invocation %q", p.InvocationID)
	}
	ch <- p.Lines
	return nil, nil
}

// RunCommand runs a module command for a BNC or CLI caller (origin wire.OriginBNC/OriginCLI).
// handled is false when no module registered args[0]; otherwise the module's reply lines, or
// an error once CommandTimeout passes without one.
func (s *Server) RunCommand(name string, args []string, origin string) (lines []string, handled bool, err error) {
	r := &s.commands
	r.mu.Lock()
	e, ok := r.byName[name]
	if !ok {
		r.mu.Unlock()
		return nil, false, nil
	}
	r.seq++
	id := "cmd-" + strconv.FormatUint(r.seq, 10)
	ch := make(chan []string, 1)
	if r.waiting == nil {
		r.waiting = map[string]chan []string{}
	}
	r.waiting[id] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.waiting, id)
		r.mu.Unlock()
	}()
	if args == nil {
		args = []string{}
	}
	notAnswered := fmt.Errorf("module %s did not answer", e.conn.Module())
	if !e.conn.EnqueueEvent(wire.EventCommand, wire.CommandEvent{InvocationID: id, Args: args, Origin: origin}) {
		return nil, true, notAnswered
	}
	select {
	case lines := <-ch:
		return lines, true, nil
	case <-time.After(CommandTimeout):
		return nil, true, notAnswered
	}
}
