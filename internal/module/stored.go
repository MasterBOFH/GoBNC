package module

import (
	"context"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// bufferView is what the read-only history and read-marker methods need from a network: how
// it folds and classifies names, our nick, and the channels we are in. A running network's
// Session is one; a stopped network is answered by storedNetwork.
type bufferView interface {
	Fold(name string) string
	CaseMapping() irc.CaseMapping
	Nick() string
	JoinedChannels() []string
	IsChannel(name string) bool
}

// storedNetwork stands in for the Session of a network the brain runs none for (stopped, or
// never started): its history, read markers and device rows are still in the store. With no
// uplink there is no ISUPPORT and no self-nick, so it folds and classifies names the way a
// Session does before the server's 005 (rfc1459, channel prefixes #&+!) and takes the nick
// the network last had (store.LastNick: the alternate after a collision, say) as ours, or its
// configured nick when none was recorded. Its channels are the network's stored channel list (the
// ones it joins on connect), standing in for the joined channels a Session reports.
type storedNetwork struct {
	nick     string
	cm       irc.CaseMapping
	channels []string
}

func (n storedNetwork) Fold(name string) string      { return n.cm.Canonical(name) }
func (n storedNetwork) CaseMapping() irc.CaseMapping { return n.cm }
func (n storedNetwork) Nick() string                 { return n.nick }
func (n storedNetwork) JoinedChannels() []string     { return n.channels }
func (n storedNetwork) IsChannel(name string) bool {
	return name != "" && strings.IndexByte("#&+!", name[0]) >= 0
}

// view returns a network for a read-only method: its Session when one runs, else the stored
// network. not_ready until the brain is ready, not_found for a network the store does not have.
func (s *Server) view(ctx context.Context, network int64) (bufferView, error) {
	sess, err := s.session(network)
	if err == nil {
		return sess, nil
	}
	if we, ok := err.(*wire.Error); !ok || we.Code != wire.CodeNotFound {
		return nil, err
	}
	db := s.backend.Store()
	if db == nil {
		return nil, err
	}
	nets, lerr := db.ListNetworks(ctx)
	if lerr != nil {
		return nil, lerr
	}
	for _, n := range nets {
		if n.ID != network {
			continue
		}
		chs, err := db.ListChannels(ctx, network)
		if err != nil {
			return nil, err
		}
		nick, err := db.LastNick(ctx, network)
		if err != nil {
			return nil, err
		}
		if nick == "" {
			nick = n.Nick
		}
		v := storedNetwork{nick: nick, cm: irc.NewISUPPORT().CaseMapping}
		for _, ch := range chs {
			v.channels = append(v.channels, ch.Name)
		}
		return v, nil
	}
	return nil, err
}
