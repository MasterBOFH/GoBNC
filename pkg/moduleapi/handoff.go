package moduleapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Hot reload: on a reload event a module starts its replacement itself and passes it the files
// that must survive (its listening sockets), its module lock and a confirmation pipe as
// inherited file descriptors. The replacement adopts them, connects to the brain, and confirms;
// only then does the old process stop serving and exit. See docs/module-api-design.md.

// EnvHandoff names the files a replacement inherited: "lock=3,confirm=4,<name>=5,...", fd
// numbers as the replacement sees them.
const EnvHandoff = "GOBNC_MODULE_HANDOFF"

// HandoffTimeout is how long Handoff waits for the replacement to confirm.
var HandoffTimeout = 15 * time.Second

// HandoffFile is a file the old process passes to its replacement under Name.
type HandoffFile struct {
	Name string
	File *os.File
}

var handoffNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// Handoff starts the replacement ev describes (its exec, args and config, from the brain's
// current gobnc.json entry) with the module lock, a confirmation pipe and files, and waits for
// it to confirm (ConfirmHandoff). It returns the replacement's pid; the caller then stops
// serving and calls Release, never Close. On any failure the replacement is killed and the
// caller keeps serving.
func (c *Client) Handoff(ctx context.Context, ev wire.ReloadEvent, files []HandoffFile) (int, error) {
	if c.lock == nil {
		return 0, errors.New("moduleapi: handoff needs the module lock")
	}
	if ev.Exec == "" {
		return 0, errors.New("moduleapi: handoff needs an exec")
	}
	names := []string{"lock=3", "confirm=4"}
	extra := []*os.File{c.lock, nil}
	for i, f := range files {
		if !handoffNameRE.MatchString(f.Name) || f.Name == "lock" || f.Name == "confirm" {
			return 0, fmt.Errorf("moduleapi: bad handoff file name %q", f.Name)
		}
		names = append(names, f.Name+"="+strconv.Itoa(5+i))
		extra = append(extra, f.File)
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		return 0, err
	}
	defer pr.Close()
	extra[1] = pw
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		pw.Close()
		return 0, err
	}
	defer null.Close()
	cfg := string(ev.Config)
	if cfg == "" {
		cfg = "{}"
	}
	cmd := exec.Command(ev.Exec, ev.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = null, null, null
	cmd.ExtraFiles = extra
	cmd.Env = append(moduleFreeEnv(os.Environ()),
		"GOBNC_MODULE_SOCKET="+c.opts.Socket,
		"GOBNC_MODULE_NAME="+c.opts.Module,
		"GOBNC_MODULE_CONFIG="+cfg,
		EnvHandoff+"="+strings.Join(names, ","))
	err = cmd.Start()
	pw.Close() // the child holds the write end now
	if err != nil {
		return 0, fmt.Errorf("moduleapi: start replacement: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	confirmed := make(chan bool, 1)
	go func() {
		line, _ := bufio.NewReader(pr).ReadString('\n')
		confirmed <- line == "ok\n"
	}()
	timer := time.NewTimer(HandoffTimeout)
	defer timer.Stop()
	fail := func(err error) (int, error) {
		_ = cmd.Process.Kill()
		return 0, err
	}
	for {
		select {
		case ok := <-confirmed:
			if ok {
				return cmd.Process.Pid, nil
			}
			// EOF without ok: the replacement closed the pipe or exited; let exited say which.
			confirmed = nil
		case err := <-exited:
			return 0, fmt.Errorf("moduleapi: replacement exited before confirming: %v", err)
		case <-timer.C:
			return fail(fmt.Errorf("moduleapi: replacement did not confirm within %s", HandoffTimeout))
		case <-ctx.Done():
			return fail(ctx.Err())
		}
	}
}

// moduleFreeEnv is env without any GOBNC_MODULE_* variable.
func moduleFreeEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if !strings.HasPrefix(kv, "GOBNC_MODULE_") {
			out = append(out, kv)
		}
	}
	return out
}

// Release drops the brain connection and closes the module lock file without unlocking it. A
// flock belongs to the open file description, which a replacement started by Handoff shares:
// unlocking here (as Close does) would release the replacement's lock too.
func (c *Client) Release() {
	c.dropConn()
	if c.lock != nil {
		_ = c.lock.Close()
		c.lock = nil
	}
}

var (
	inheritOnce sync.Once
	inheritMu   sync.Mutex
	inherited   map[string]*os.File
)

// Inherited returns the file the previous process passed under name, or nil when this process
// is not a handoff replacement (or has no such file).
func Inherited(name string) *os.File {
	inheritOnce.Do(parseInherited)
	inheritMu.Lock()
	defer inheritMu.Unlock()
	return inherited[name]
}

// parseInherited reads EnvHandoff. The inherited descriptors are marked close-on-exec so they
// never leak into a later replacement except through its own ExtraFiles.
func parseInherited() {
	inherited = map[string]*os.File{}
	v := os.Getenv(EnvHandoff)
	if v == "" {
		return
	}
	for _, part := range strings.Split(v, ",") {
		name, num, ok := strings.Cut(part, "=")
		fd, err := strconv.Atoi(num)
		if !ok || err != nil || fd < 3 {
			continue
		}
		syscall.CloseOnExec(fd)
		inherited[name] = os.NewFile(uintptr(fd), name)
	}
}

// take removes name from the inherited set and returns it.
func takeInherited(name string) *os.File {
	inheritOnce.Do(parseInherited)
	inheritMu.Lock()
	defer inheritMu.Unlock()
	f := inherited[name]
	delete(inherited, name)
	return f
}

// InheritedListener returns the listener the previous process passed under name, or (nil, nil)
// when there is none.
func InheritedListener(name string) (net.Listener, error) {
	f := takeInherited(name)
	if f == nil {
		return nil, nil
	}
	defer f.Close()
	ln, err := net.FileListener(f)
	if err != nil {
		return nil, fmt.Errorf("moduleapi: inherited listener %q: %w", name, err)
	}
	return ln, nil
}

// ConfirmHandoff tells the previous process this replacement is serving, so it stops and exits.
// It is a no-op when this process is not a handoff replacement or already confirmed.
func (c *Client) ConfirmHandoff() error {
	f := takeInherited("confirm")
	if f == nil {
		return nil
	}
	defer f.Close()
	if _, err := f.WriteString("ok\n"); err != nil {
		return fmt.Errorf("moduleapi: confirm handoff: %w", err)
	}
	c.confirmed.Store(true)
	return nil
}
