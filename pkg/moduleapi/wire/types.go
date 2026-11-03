package wire

import "encoding/json"

// Params, results and event data of every method and event. JSON names are the wire contract;
// a network is GoBNC's network id, a buffer a display name and a key its CASEMAPPING fold.

// --- hello ---

type HelloParams struct {
	API      int      `json:"api"`
	Module   string   `json:"module"`
	Features []string `json:"features"`
	// PID is the module's process id (the brain stops or restarts the module by it).
	PID int `json:"pid,omitempty"`
	// Handoff says the module replaces itself on a reload event.
	Handoff bool `json:"handoff,omitempty"`
}

type HelloResult struct {
	API          int      `json:"api"`
	BrainVersion string   `json:"brain_version"`
	Granted      []string `json:"granted"`
	Denied       []string `json:"denied"`
	Ready        bool     `json:"ready"`
}

// --- networks ---

// Network states.
const (
	NetStopped      = "stopped"
	NetConnecting   = "connecting"
	NetRegistered   = "registered"
	NetDisconnected = "disconnected"
)

type NetworkInfo struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
	Nick  string `json:"nick"`
}

type NetworksListResult struct {
	Networks []NetworkInfo `json:"networks"`
}

// NetworkParams names one network (networks.connect/disconnect/reconnect, events.unsubscribe).
type NetworkParams struct {
	Network int64 `json:"network"`
}

// Network event changes.
const (
	ChangeAdded   = "added"
	ChangeRemoved = "removed"
	ChangeConfig  = "config"
	ChangeState   = "state"
)

type NetworkEvent struct {
	Network int64  `json:"network"`
	Name    string `json:"name"`
	State   string `json:"state"`
	Nick    string `json:"nick"`
	Change  string `json:"change"`
}

// --- events ---

type SubscribeParams struct {
	Network int64    `json:"network"`
	Kinds   []string `json:"kinds,omitempty"`
}

type SubscribeResult struct {
	Snapshot Snapshot `json:"snapshot"`
}

type Snapshot struct {
	Network int64  `json:"network"`
	State   string `json:"state"`
	LastID  int64  `json:"last_id"`
	// Seq is the seq of the last line event emitted before the snapshot: line events with a
	// higher seq follow it. A module re-subscribing for a fresh snapshot uses it to place the
	// snapshot in the stream it is already consuming.
	Seq      uint64            `json:"seq"`
	Self     Self              `json:"self"`
	ISupport map[string]string `json:"isupport"`
	Caps     []string          `json:"caps"`
	Channels []Channel         `json:"channels"`
}

type Self struct {
	Nick    string `json:"nick"`
	User    string `json:"user"`
	Host    string `json:"host"`
	Account string `json:"account"`
	UModes  string `json:"umodes"`
	Away    bool   `json:"away"`
}

type Channel struct {
	Name            string   `json:"name"`
	Key             string   `json:"key"`
	Topic           Topic    `json:"topic"`
	Modes           string   `json:"modes"`
	ChanKey         string   `json:"chankey"`
	MembersComplete bool     `json:"members_complete"`
	Members         []Member `json:"members"`
}

type Topic struct {
	Text   string `json:"text"`
	Setter string `json:"setter"`
	Time   string `json:"time"` // RFC 3339, "" when unknown
}

type Member struct {
	Nick     string `json:"nick"`
	Prefixes string `json:"prefixes"`
	User     string `json:"user"`
	Host     string `json:"host"`
	Account  string `json:"account"`
	Away     bool   `json:"away"`
}

// Line kinds.
const (
	LineMessage   = "message" // PRIVMSG, incl. CTCP ACTION
	LineNotice    = "notice"
	LineCTCP      = "ctcp"       // PRIVMSG CTCP other than ACTION
	LineCTCPReply = "ctcp_reply" // NOTICE CTCP
	LineTagmsg    = "tagmsg"
	LineJoin      = "join"
	LinePart      = "part"
	LineQuit      = "quit"
	LineKick      = "kick"
	LineNick      = "nick"
	LineMode      = "mode"
	LineTopic     = "topic"
	LineInvite    = "invite"
	LineAway      = "away"
	LineAccount   = "account"
	LineChghost   = "chghost"
	LineSetname   = "setname"
	LineNumeric   = "numeric"
	LineError     = "error"
	LineOther     = "other"
)

// ServerBuffer is the buffer of server-sourced and $mask lines.
const ServerBuffer = "Server"

type LineEvent struct {
	Network int64        `json:"network"`
	Seq     uint64       `json:"seq"`
	Kind    string       `json:"kind"`
	Raw     string       `json:"raw"`
	MsgID   string       `json:"msgid"`
	Time    string       `json:"time"`
	Source  string       `json:"source"`
	Nick    string       `json:"nick"`
	Self    bool         `json:"self"`
	Label   string       `json:"label,omitempty"`
	Buffers []LineBuffer `json:"buffers"`
}

type LineBuffer struct {
	Name string `json:"name"`
	Key  string `json:"key"`
	ID   int64  `json:"id,omitempty"` // set when that buffer stored the line
}

type ClientLineEvent struct {
	Network int64  `json:"network"`
	Client  string `json:"client"`
	Raw     string `json:"raw"`
}

// State event kinds.
const StateMembers = "members"

// StateEvent: kind "members" replaces a channel's member list (Complete true once NAMES landed).
type StateEvent struct {
	Network  int64    `json:"network"`
	Kind     string   `json:"kind"`
	Buffer   string   `json:"buffer"`
	Key      string   `json:"key"`
	Members  []Member `json:"members"`
	Complete bool     `json:"complete"`
}

// --- history ---

// History anchors.
const (
	AnchorFirstUnread = "first_unread"
	AnchorLatest      = "latest"
)

type HistoryMessagesParams struct {
	Network  int64  `json:"network"`
	Buffer   string `json:"buffer"`
	BeforeID *int64 `json:"before_id,omitempty"`
	AfterID  *int64 `json:"after_id,omitempty"`
	AroundID *int64 `json:"around_id,omitempty"`
	Anchor   string `json:"anchor,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Device   string `json:"device,omitempty"`
}

type HistoryMessagesResult struct {
	Messages  []HistoryMessage `json:"messages"`
	HasBefore bool             `json:"has_before"`
	HasAfter  bool             `json:"has_after"`
}

type HistoryMessage struct {
	ID      int64  `json:"id"`
	Time    string `json:"time"` // RFC 3339 nano, UTC
	MsgID   string `json:"msgid"`
	Command string `json:"command"`
	Source  string `json:"source"`
	Buffer  string `json:"buffer"`
	Raw     string `json:"raw"`
	Text    string `json:"text"`
}

type HistoryActivityParams struct {
	Network int64  `json:"network"`
	Device  string `json:"device"`
}

type HistoryActivityResult struct {
	Buffers []BufferActivity `json:"buffers"`
}

type BufferActivity struct {
	Buffer     string `json:"buffer"`
	Key        string `json:"key"`
	LastID     int64  `json:"last_id"`
	LastTime   string `json:"last_time"`
	Unread     int    `json:"unread"`
	Highlights int    `json:"highlights"`
}

// --- read markers ---

type ReadMarkerGetParams struct {
	Network int64  `json:"network"`
	Buffer  string `json:"buffer"`
}

type ReadMarkerGetResult struct {
	ID   int64  `json:"id"`
	Time string `json:"time"`
}

type ReadMarkerSetParams struct {
	Network int64  `json:"network"`
	Buffer  string `json:"buffer"`
	ID      int64  `json:"id"`
}

type ReadMarkerSetResult struct {
	StoredID int64 `json:"stored_id"`
	Updated  bool  `json:"updated"`
}

type ReadMarkerEvent struct {
	Network int64  `json:"network"`
	Buffer  string `json:"buffer"`
	Key     string `json:"key"`
	ID      int64  `json:"id"`
	Time    string `json:"time"`
}

// --- clients / send ---

type ClientParams struct {
	Network int64  `json:"network"`
	Client  string `json:"client"`
}

type ClientsAttachResult struct {
	ClientID string `json:"client_id"`
}

type SendLineParams struct {
	Network int64  `json:"network"`
	Client  string `json:"client"`
	Line    string `json:"line"`
	Label   string `json:"label,omitempty"`
}

// --- devices ---

type DeviceTouchParams struct {
	ClientID string `json:"client_id"`
	Name     string `json:"name,omitempty"`
}

type DeviceTouchResult struct {
	Created bool `json:"created"`
}

type DeviceParams struct {
	ClientID string `json:"client_id"`
}

type DeviceNetworksResult struct {
	Networks []DeviceNetwork `json:"networks"`
}

type DeviceNetwork struct {
	Network    int64 `json:"network"`
	Subscribed bool  `json:"subscribed"`
}

type DeviceSetNetworkParams struct {
	ClientID   string `json:"client_id"`
	Network    int64  `json:"network"`
	Subscribed bool   `json:"subscribed"`
}

type DeviceNetworkParams struct {
	ClientID string `json:"client_id"`
	Network  int64  `json:"network"`
}

type DeviceBuffersResult struct {
	Buffers []DeviceBuffer `json:"buffers"`
}

type DeviceBuffer struct {
	Buffer     string `json:"buffer"`
	Key        string `json:"key"`
	Open       bool   `json:"open"`
	ClosedAtID int64  `json:"closed_at_id"`
}

type DeviceBufferParams struct {
	ClientID string `json:"client_id"`
	Network  int64  `json:"network"`
	Buffer   string `json:"buffer"`
}

type DeviceCloseResult struct {
	ClosedAtID int64 `json:"closed_at_id"`
}

type DeviceSeedResult struct {
	Seeded []string `json:"seeded"`
}

// buffer_open reasons.
const (
	OpenMessage = "message"
	OpenUser    = "user"
	OpenSeed    = "seed"
)

type BufferOpenEvent struct {
	Network  int64  `json:"network"`
	ClientID string `json:"client_id"`
	Buffer   string `json:"buffer"`
	Key      string `json:"key"`
	Reason   string `json:"reason"`
}

type BufferCloseEvent struct {
	Network    int64  `json:"network"`
	ClientID   string `json:"client_id"`
	Buffer     string `json:"buffer"`
	Key        string `json:"key"`
	ClosedAtID int64  `json:"closed_at_id"`
}

type DeviceNetworkEvent struct {
	ClientID   string `json:"client_id"`
	Network    int64  `json:"network"`
	Subscribed bool   `json:"subscribed"`
}

// --- auth ---

type AuthCheckParams struct {
	Password   string `json:"password,omitempty"`
	CertSHA256 string `json:"cert_sha256,omitempty"`
	RemoteIP   string `json:"remote_ip"`
}

// auth.check refusal reasons (AuthCheckResult.Reason; empty when OK).
const (
	AuthInvalid     = "invalid"      // wrong credential, auth method disabled, or address not allowed
	AuthRateLimited = "rate_limited" // too many failures from this address; see RetryAfter
)

type AuthCheckResult struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
	// RetryAfter is the whole seconds until a rate-limited address may try again.
	RetryAfter int `json:"retry_after,omitempty"`
}

// --- storage ---

type StorageKeyParams struct {
	Key string `json:"key"`
}

type StorageGetResult struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type StorageSetParams struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type StorageListParams struct {
	Prefix string `json:"prefix"`
}

type StorageListResult struct {
	Keys []string `json:"keys"`
}

// --- commands ---

type CommandRegisterParams struct {
	Name string `json:"name"`
	Help string `json:"help"`
}

// Command origins.
const (
	OriginBNC = "bnc"
	OriginCLI = "cli"
)

type CommandEvent struct {
	InvocationID string   `json:"invocation_id"`
	Args         []string `json:"args"`
	Origin       string   `json:"origin"`
}

type CommandReplyParams struct {
	InvocationID string   `json:"invocation_id"`
	Lines        []string `json:"lines"`
}

// Empty is the result of methods that return {}.
type Empty struct{}

// --- reload ---

// ReloadEvent asks a module that said handoff in hello to start its replacement from its
// current gobnc.json entry and hand over to it.
type ReloadEvent struct {
	Exec   string          `json:"exec"`
	Args   []string        `json:"args,omitempty"`
	Config json.RawMessage `json:"config,omitempty"`
}
