package admin

import (
	"encoding/json"
	"fmt"

	"github.com/MasterBOFH/GoBNC/internal/control"
)

// ControlRuntime talks to a running daemon over the Unix control socket.
type ControlRuntime struct {
	Socket string
}

func (r ControlRuntime) StartNetwork(name string) (bool, error) {
	return control.TryNotify(r.Socket, control.CmdStartNetwork+" "+name)
}

func (r ControlRuntime) StopNetwork(name string) (bool, error) {
	return control.TryNotify(r.Socket, control.CmdStopNetwork+" "+name)
}

func (r ControlRuntime) ReloadNetwork(name string) (bool, error) {
	return control.TryNotify(r.Socket, control.CmdReloadNetwork+" "+name)
}

func (r ControlRuntime) ReconnectNetwork(name string) error {
	ok, err := control.TryNotify(r.Socket, control.CmdReconnectNetwork+" "+name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("daemon not running (no control socket at %s)", r.Socket)
	}
	return nil
}

func (r ControlRuntime) Rehash() error {
	ok, err := control.TryNotify(r.Socket, control.CmdRehash)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("daemon not running (no control socket at %s)", r.Socket)
	}
	return nil
}

func (r ControlRuntime) Status() (Status, bool, error) {
	payload, ok, err := control.TryQuery(r.Socket, control.CmdStatus)
	if err != nil {
		return Status{}, ok, err
	}
	if !ok {
		return Status{}, false, nil
	}
	var st Status
	if payload == "" {
		return Status{Running: true}, true, nil
	}
	if err := json.Unmarshal([]byte(payload), &st); err != nil {
		return Status{}, true, fmt.Errorf("status decode: %w", err)
	}
	st.Running = true
	return st, true, nil
}
