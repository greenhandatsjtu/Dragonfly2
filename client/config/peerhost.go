/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package config holds all options of peerhost.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	commonv1 "d7y.io/api/pkg/apis/common/v1"

	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	netip "d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/unit"
)

type DaemonConfig = DaemonOption
type DaemonOption struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	// AliveTime indicates alive duration for which daemon keeps no accessing by any uploading and download requests,
	// after this period daemon will automatically exit
	// when AliveTime == 0, will run infinitely
	AliveTime  util.Duration `mapstructure:"aliveTime" yaml:"aliveTime"`
	GCInterval util.Duration `mapstructure:"gcInterval" yaml:"gcInterval"`
	Metrics    string        `yaml:"metrics" mapstructure:"metrics"`

	WorkHome    string `mapstructure:"workHome" yaml:"workHome"`
	CacheDir    string `mapstructure:"cacheDir" yaml:"cacheDir"`
	LogDir      string `mapstructure:"logDir" yaml:"logDir"`
	DataDir     string `mapstructure:"dataDir" yaml:"dataDir"`
	KeepStorage bool   `mapstructure:"keepStorage" yaml:"keepStorage"`

	Scheduler     SchedulerOption     `mapstructure:"scheduler" yaml:"scheduler"`
	Host          HostOption          `mapstructure:"host" yaml:"host"`
	Download      DownloadOption      `mapstructure:"download" yaml:"download"`
	Proxy         *ProxyOption        `mapstructure:"proxy" yaml:"proxy"`
	Upload        UploadOption        `mapstructure:"upload" yaml:"upload"`
	ObjectStorage ObjectStorageOption `mapstructure:"objectStorage" yaml:"objectStorage"`
	Storage       StorageOption       `mapstructure:"storage" yaml:"storage"`
	Health        *HealthOption       `mapstructure:"health" yaml:"health"`
	Reload        ReloadOption        `mapstructure:"reload" yaml:"reload"`
}

func NewDaemonConfig() *DaemonOption {
	return peerHostConfig()
}

func (p *DaemonOption) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("unable to load peer host configuration from %q [%v]", path, err)
	}

	switch filepath.Ext(path) {
	case ".json":
		err := json.Unmarshal(data, p)
		if err != nil {
			return err
		}
	case ".yml", ".yaml":
		err := yaml.Unmarshal(data, p)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("extension of %s is not in 'yml/yaml/json'", path)
	}

	return nil
}

func (p *DaemonOption) Convert() error {
	// AdvertiseIP
	ip := net.ParseIP(p.Host.AdvertiseIP)
	if ip == nil || net.IPv4zero.Equal(ip) {
		p.Host.AdvertiseIP = netip.IPv4
	} else {
		p.Host.AdvertiseIP = ip.String()
	}

	// ScheduleTimeout should not great then AliveTime
	if p.AliveTime.Duration > 0 && p.Scheduler.ScheduleTimeout.Duration > p.AliveTime.Duration {
		p.Scheduler.ScheduleTimeout.Duration = p.AliveTime.Duration - time.Second
	}

	return nil
}

func (p *DaemonOption) Validate() error {
	if p.Scheduler.Manager.Enable {
		if len(p.Scheduler.Manager.NetAddrs) == 0 {
			return errors.New("manager addr is not specified")
		}

		if p.Scheduler.Manager.RefreshInterval == 0 {
			return errors.New("manager refreshInterval is not specified")
		}
		return nil
	}

	if len(p.Scheduler.NetAddrs) == 0 {
		return errors.New("empty schedulers and config server is not specified")
	}

	if int64(p.Download.TotalRateLimit.Limit) < DefaultMinRate.ToNumber() {
		return fmt.Errorf("rate limit must be greater than %s", DefaultMinRate.String())
	}

	if int64(p.Upload.RateLimit.Limit) < DefaultMinRate.ToNumber() {
		return fmt.Errorf("rate limit must be greater than %s", DefaultMinRate.String())
	}

	if p.ObjectStorage.Enable {
		if p.ObjectStorage.MaxReplicas <= 0 {
			return errors.New("max replicas must be greater than 0")
		}
	}

	if p.Reload.Interval.Duration > 0 && p.Reload.Interval.Duration < time.Second {
		return errors.New("reload interval too short, must great than 1 second")
	}

	switch p.Download.DefaultPattern {
	case PatternP2P, PatternSeedPeer, PatternSource:
	default:
		return errors.New("available pattern: p2p, seed-peer, source")
	}
	return nil
}

func ConvertPattern(p string, defaultPattern commonv1.Pattern) commonv1.Pattern {
	switch p {
	case PatternP2P:
		return commonv1.Pattern_P2P
	case PatternSeedPeer:
		return commonv1.Pattern_SEED_PEER
	case PatternSource:
		return commonv1.Pattern_SOURCE
	case "":
		return defaultPattern
	}
	logger.Warnf("unknown pattern, use default pattern: %s", commonv1.Pattern_name[int32(defaultPattern)])
	return defaultPattern
}

type SchedulerOption struct {
	// Manager is to get the scheduler configuration remotely.
	Manager ManagerOption `mapstructure:"manager" yaml:"manager"`
	// NetAddrs is scheduler addresses.
	NetAddrs []dfnet.NetAddr `mapstructure:"netAddrs" yaml:"netAddrs"`
	// ScheduleTimeout is request timeout.
	ScheduleTimeout util.Duration `mapstructure:"scheduleTimeout" yaml:"scheduleTimeout"`
	// DisableAutoBackSource indicates not back source normally, only scheduler says back source.
	DisableAutoBackSource bool `mapstructure:"disableAutoBackSource" yaml:"disableAutoBackSource"`
}

type ManagerOption struct {
	// Enable get configuration from manager.
	Enable bool `mapstructure:"enable" yaml:"enable"`
	// NetAddrs is manager addresses.
	NetAddrs []dfnet.NetAddr `mapstructure:"netAddrs" yaml:"netAddrs"`
	// RefreshInterval is the refresh interval.
	RefreshInterval time.Duration `mapstructure:"refreshInterval" yaml:"refreshInterval"`
	// SeedPeer configuration.
	SeedPeer SeedPeerOption `mapstructure:"seedPeer" yaml:"seedPeer"`
}

type SeedPeerOption struct {
	// Enable seed peer mode.
	Enable bool `mapstructure:"enable" yaml:"enable"`
	// Type is seed peer type.
	Type string `mapstructure:"type" yaml:"type"`
	// ClusterID is seed peer cluster id.
	ClusterID uint `mapstructure:"clusterID" yaml:"clusterID"`
	// KeepAlive configuration.
	KeepAlive KeepAliveOption `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type KeepAliveOption struct {
	// Keep alive interval.
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

type HostOption struct {
	// SecurityDomain is the security domain
	SecurityDomain string `mapstructure:"securityDomain" yaml:"securityDomain"`
	// IDC for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`
	// Peerhost net topology for scheduler
	NetTopology string `mapstructure:"netTopology" yaml:"netTopology"`
	// Location for scheduler
	Location string `mapstructure:"location" yaml:"location"`
	// Hostname is daemon host name
	Hostname string `mapstructure:"hostname" yaml:"hostname"`
	// The listen ip for all tcp services of daemon
	ListenIP string `mapstructure:"listenIP" yaml:"listenIP"`
	// The ip report to scheduler, normal same with listen ip
	AdvertiseIP string `mapstructure:"advertiseIP" yaml:"advertiseIP"`
}

type DownloadOption struct {
	DefaultPattern       string            `mapstructure:"defaultPattern" yaml:"defaultPattern"`
	TotalRateLimit       util.RateLimit    `mapstructure:"totalRateLimit" yaml:"totalRateLimit"`
	PerPeerRateLimit     util.RateLimit    `mapstructure:"perPeerRateLimit" yaml:"perPeerRateLimit"`
	EnableTrafficShaper  bool              `mapstructure:"enableTrafficShaper" yaml:"enableTrafficShaper"`
	PieceDownloadTimeout time.Duration     `mapstructure:"pieceDownloadTimeout" yaml:"pieceDownloadTimeout"`
	DownloadGRPC         ListenOption      `mapstructure:"downloadGRPC" yaml:"downloadGRPC"`
	PeerGRPC             ListenOption      `mapstructure:"peerGRPC" yaml:"peerGRPC"`
	CalculateDigest      bool              `mapstructure:"calculateDigest" yaml:"calculateDigest"`
	Transport            *TransportOption  `mapstructure:"transportOption" yaml:"transportOption"`
	GetPiecesMaxRetry    int               `mapstructure:"getPiecesMaxRetry" yaml:"getPiecesMaxRetry"`
	Prefetch             bool              `mapstructure:"prefetch" yaml:"prefetch"`
	WatchdogTimeout      time.Duration     `mapstructure:"watchdogTimeout" yaml:"watchdogTimeout"`
	Concurrent           *ConcurrentOption `mapstructure:"concurrent" yaml:"concurrent"`
}

type TransportOption struct {
	DialTimeout           time.Duration `mapstructure:"dialTimeout" yaml:"dialTimeout"`
	KeepAlive             time.Duration `mapstructure:"keepAlive" yaml:"keepAlive"`
	MaxIdleConns          int           `mapstructure:"maxIdleConns" yaml:"maxIdleConns"`
	IdleConnTimeout       time.Duration `mapstructure:"idleConnTimeout" yaml:"idleConnTimeout"`
	ResponseHeaderTimeout time.Duration `mapstructure:"responseHeaderTimeout" yaml:"responseHeaderTimeout"`
	TLSHandshakeTimeout   time.Duration `mapstructure:"tlsHandshakeTimeout" yaml:"tlsHandshakeTimeout"`
	ExpectContinueTimeout time.Duration `mapstructure:"expectContinueTimeout" yaml:"expectContinueTimeout"`
}

type ConcurrentOption struct {
	// ThresholdSize indicates the threshold to download pieces concurrently
	ThresholdSize util.Size `mapstructure:"thresholdSize" yaml:"thresholdSize"`
	// ThresholdSpeed indicates the threshold download speed to download pieces concurrently
	ThresholdSpeed unit.Bytes `mapstructure:"thresholdSpeed" yaml:"thresholdSpeed"`
	// GoroutineCount indicates the concurrent goroutine count for every task
	GoroutineCount int `mapstructure:"goroutineCount" yaml:"goroutineCount"`
	// InitBackoff second for every piece failed, default: 0.5
	InitBackoff float64 `mapstructure:"initBackoff" yaml:"initBackoff"`
	// MaxBackoff second for every piece failed, default: 3
	MaxBackoff float64 `mapstructure:"maxBackoff" yaml:"maxBackoff"`
	// MaxAttempts for every piece failed,default: 3
	MaxAttempts int `mapstructure:"maxAttempts" yaml:"maxAttempts"`
}

type ProxyOption struct {
	// WARNING: when add more option, please update ProxyOption.unmarshal function
	ListenOption    `mapstructure:",squash" yaml:",inline"`
	BasicAuth       *BasicAuth      `mapstructure:"basicAuth" yaml:"basicAuth"`
	DefaultFilter   string          `mapstructure:"defaultFilter" yaml:"defaultFilter"`
	DefaultTag      string          `mapstructure:"defaultTag" yaml:"defaultTag"`
	MaxConcurrency  int64           `mapstructure:"maxConcurrency" yaml:"maxConcurrency"`
	RegistryMirror  *RegistryMirror `mapstructure:"registryMirror" yaml:"registryMirror"`
	WhiteList       []*WhiteList    `mapstructure:"whiteList" yaml:"whiteList"`
	ProxyRules      []*ProxyRule    `mapstructure:"proxies" yaml:"proxies"`
	HijackHTTPS     *HijackConfig   `mapstructure:"hijackHTTPS" yaml:"hijackHTTPS"`
	DumpHTTPContent bool            `mapstructure:"dumpHTTPContent" yaml:"dumpHTTPContent"`
	// ExtraRegistryMirrors add more mirror for different ports
	ExtraRegistryMirrors []*RegistryMirror `mapstructure:"extraRegistryMirrors" yaml:"extraRegistryMirrors"`
}

func (p *ProxyOption) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		file, err := os.ReadFile(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(file, p); err != nil {
			return err
		}
		return nil
	case map[string]any:
		if err := p.unmarshal(json.Unmarshal, b); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy option")
	}
}

func (p *ProxyOption) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		var path string
		if err := node.Decode(&path); err != nil {
			return err
		}

		file, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(file, p); err != nil {
			return err
		}
		return nil
	case yaml.MappingNode:
		var m = make(map[string]any)
		for i := 0; i < len(node.Content); i += 2 {
			var (
				key   string
				value any
			)
			if err := node.Content[i].Decode(&key); err != nil {
				return err
			}
			if err := node.Content[i+1].Decode(&value); err != nil {
				return err
			}
			m[key] = value
		}

		b, err := yaml.Marshal(m)
		if err != nil {
			return err
		}

		if err := p.unmarshal(yaml.Unmarshal, b); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy")
	}
}

func (p *ProxyOption) unmarshal(unmarshal func(in []byte, out any) (err error), b []byte) error {
	pt := struct {
		ListenOption         `mapstructure:",squash" yaml:",inline"`
		BasicAuth            *BasicAuth        `mapstructure:"basicAuth" yaml:"basicAuth"`
		DefaultFilter        string            `mapstructure:"defaultFilter" yaml:"defaultFilter"`
		DefaultTag           string            `mapstructure:"defaultTag" yaml:"defaultTag"`
		MaxConcurrency       int64             `mapstructure:"maxConcurrency" yaml:"maxConcurrency"`
		RegistryMirror       *RegistryMirror   `mapstructure:"registryMirror" yaml:"registryMirror"`
		WhiteList            []*WhiteList      `mapstructure:"whiteList" yaml:"whiteList"`
		Proxies              []*ProxyRule      `mapstructure:"proxies" yaml:"proxies"`
		HijackHTTPS          *HijackConfig     `mapstructure:"hijackHTTPS" yaml:"hijackHTTPS"`
		DumpHTTPContent      bool              `mapstructure:"dumpHTTPContent" yaml:"dumpHTTPContent"`
		ExtraRegistryMirrors []*RegistryMirror `mapstructure:"extraRegistryMirrors" yaml:"extraRegistryMirrors"`
	}{}

	if err := unmarshal(b, &pt); err != nil {
		return err
	}
	p.ExtraRegistryMirrors = pt.ExtraRegistryMirrors
	p.ListenOption = pt.ListenOption
	p.RegistryMirror = pt.RegistryMirror
	p.ProxyRules = pt.Proxies
	p.HijackHTTPS = pt.HijackHTTPS
	p.WhiteList = pt.WhiteList
	p.MaxConcurrency = pt.MaxConcurrency
	p.DefaultFilter = pt.DefaultFilter
	p.DefaultTag = pt.DefaultTag
	p.BasicAuth = pt.BasicAuth
	p.DumpHTTPContent = pt.DumpHTTPContent

	return nil
}

type UploadOption struct {
	ListenOption `yaml:",inline" mapstructure:",squash"`
	RateLimit    util.RateLimit `mapstructure:"rateLimit" yaml:"rateLimit"`
}

type ObjectStorageOption struct {
	// Enable object storage.
	Enable bool `mapstructure:"enable" yaml:"enable"`
	// Filter is used to generate a unique Task ID by
	// filtering unnecessary query params in the URL,
	// it is separated by & character.
	Filter string `mapstructure:"filter" yaml:"filter"`
	// MaxReplicas is the maximum number of replicas of an object cache in seed peers.
	MaxReplicas int `mapstructure:"maxReplicas" yaml:"maxReplicas"`
	// ListenOption is object storage service listener.
	ListenOption `yaml:",inline" mapstructure:",squash"`
}

type ListenOption struct {
	Security   SecurityOption    `mapstructure:"security" yaml:"security"`
	TCPListen  *TCPListenOption  `mapstructure:"tcpListen,omitempty" yaml:"tcpListen,omitempty"`
	UnixListen *UnixListenOption `mapstructure:"unixListen,omitempty" yaml:"unixListen,omitempty"`
}

type TCPListenOption struct {
	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1
	Listen string `mapstructure:"listen" yaml:"listen"`

	// PortRange stands listen port
	// yaml example 1:
	//   port: 12345
	// yaml example 2:
	//   port:
	//     start: 12345
	//     end: 12346
	PortRange TCPListenPortRange `mapstructure:"port" yaml:"port"`

	// Namespace stands the linux net namespace, like /proc/1/ns/net
	// It's useful for running daemon in pod with ip allocated and listen in host
	Namespace string `mapstructure:"namespace" yaml:"namespace"`
}

type TCPListenPortRange struct {
	Start int
	End   int
}

func (t *TCPListenPortRange) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	return t.unmarshal(v)
}

func (t *TCPListenPortRange) UnmarshalYAML(node *yaml.Node) error {
	var v any
	switch node.Kind {
	case yaml.MappingNode:
		var m = make(map[string]any)
		for i := 0; i < len(node.Content); i += 2 {
			var (
				key   string
				value int
			)
			if err := node.Content[i].Decode(&key); err != nil {
				return err
			}
			if err := node.Content[i+1].Decode(&value); err != nil {
				return err
			}
			m[key] = value
		}
		v = m
	case yaml.ScalarNode:
		var i int
		if err := node.Decode(&i); err != nil {
			return err
		}
		v = i
	}
	return t.unmarshal(v)
}

func (t *TCPListenPortRange) unmarshal(v any) error {
	switch value := v.(type) {
	case int:
		t.Start = value
		return nil
	case float64:
		t.Start = int(value)
		return nil
	case map[string]any:
		if s, ok := value["start"]; ok {
			switch start := s.(type) {
			case float64:
				t.Start = int(start)
			case int:
				t.Start = start
			default:
				return errors.New("invalid start port")
			}
		} else {
			return errors.New("empty start port")
		}
		if e, ok := value["end"]; ok {
			switch end := e.(type) {
			case float64:
				t.End = int(end)
			case int:
				t.End = end
			default:
				return errors.New("invalid end port")
			}
		}
		return nil
	default:
		return errors.New("invalid port")
	}
}

type UnixListenOption struct {
	Socket string `mapstructure:"socket" yaml:"socket"`
}

type SecurityOption struct {
	// Insecure indicate enable tls or not
	Insecure  bool        `mapstructure:"insecure" yaml:"insecure"`
	CACert    string      `mapstructure:"caCert" yaml:"caCert"`
	Cert      string      `mapstructure:"cert" yaml:"cert"`
	Key       string      `mapstructure:"key" yaml:"key"`
	TLSVerify bool        `mapstructure:"tlsVerify" yaml:"tlsVerify"`
	TLSConfig *tls.Config `mapstructure:"tlsConfig" yaml:"tlsConfig"`
}

type StorageOption struct {
	// DataPath indicates directory which stores temporary files for p2p uploading
	DataPath string `mapstructure:"dataPath" yaml:"dataPath"`
	// TaskExpireTime indicates caching duration for which cached file keeps no accessed by any process,
	// after this period cache file will be gc
	TaskExpireTime util.Duration `mapstructure:"taskExpireTime" yaml:"taskExpireTime"`
	// DiskGCThreshold indicates the threshold to gc the oldest tasks
	DiskGCThreshold unit.Bytes `mapstructure:"diskGCThreshold" yaml:"diskGCThreshold"`
	// DiskGCThresholdPercent indicates the threshold to gc the oldest tasks according the disk usage
	// Eg, DiskGCThresholdPercent=80, when the disk usage is above 80%, start to gc the oldest tasks
	DiskGCThresholdPercent float64 `mapstructure:"diskGCThresholdPercent" yaml:"diskGCThresholdPercent"`
	// Multiplex indicates reusing underlying storage for same task id
	Multiplex     bool          `mapstructure:"multiplex" yaml:"multiplex"`
	StoreStrategy StoreStrategy `mapstructure:"strategy" yaml:"strategy"`
}

type StoreStrategy string

type HealthOption struct {
	ListenOption `yaml:",inline" mapstructure:",squash"`
	Path         string `mapstructure:"path" yaml:"path"`
}

type ReloadOption struct {
	Interval util.Duration `mapstructure:"interval" yaml:"interval"`
}

type FileString string

func (f *FileString) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	file, err := os.ReadFile(s)
	if err != nil {
		return err
	}
	val := strings.TrimSpace(string(file))
	*f = FileString(val)
	return nil
}

func (f *FileString) UnmarshalYAML(node *yaml.Node) error {
	var s string
	switch node.Kind {
	case yaml.ScalarNode:
		if err := node.Decode(&s); err != nil {
			return err
		}
	default:
		return errors.New("invalid filestring")
	}

	file, err := os.ReadFile(s)
	if err != nil {
		return err
	}
	val := strings.TrimSpace(string(file))
	*f = FileString(val)
	return nil
}

type tlsConfigFiles struct {
	Cert   string     `json:"cert"`
	Key    string     `json:"key"`
	CACert FileString `json:"caCert"`
}

type TLSConfig struct {
	tls.Config
}

func (t *TLSConfig) UnmarshalJSON(b []byte) error {
	var cf tlsConfigFiles
	err := json.Unmarshal(b, &cf)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(cf.CACert)) {
		return errors.New("invalid CA Cert")
	}
	cert, err := tls.LoadX509KeyPair(cf.Cert, cf.Key)
	if err != nil {
		return err
	}
	t.Config = tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}
	return nil
}

// RegistryMirror configures the mirror of the official docker registry
type RegistryMirror struct {
	// Remote url for the registry mirror, default is https://index.docker.io
	Remote *URL `yaml:"url" mapstructure:"url"`

	// DynamicRemote indicates using header "X-Dragonfly-Registry" for remote instead of Remote
	// if header "X-Dragonfly-Registry" does not exist, use Remote by default
	DynamicRemote bool `yaml:"dynamic" mapstructure:"dynamic"`

	// Optional certificates if the mirror uses self-signed certificates
	Certs *CertPool `yaml:"certs" mapstructure:"certs"`

	// Whether to ignore certificates errors for the registry
	Insecure bool `yaml:"insecure" mapstructure:"insecure"`

	// Request the remote registry directly.
	Direct bool `yaml:"direct" mapstructure:"direct"`

	// Whether to use proxies to decide when to use dragonfly
	UseProxies bool `yaml:"useProxies" mapstructure:"useProxies"`
}

// TLSConfig returns the tls.Config used to communicate with the mirror.
func (r *RegistryMirror) TLSConfig() *tls.Config {
	if r == nil {
		return nil
	}
	cfg := &tls.Config{
		InsecureSkipVerify: r.Insecure,
	}
	if r.Certs != nil {
		cfg.RootCAs = r.Certs.CertPool
	}
	return cfg
}

// URL is simple wrapper around url.URL to make it unmarshallable from a string.
type URL struct {
	*url.URL
}

// UnmarshalJSON implements json.Unmarshaler.
func (u *URL) UnmarshalJSON(b []byte) error {
	return u.unmarshal(func(v any) error { return json.Unmarshal(b, v) })
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (u *URL) UnmarshalYAML(unmarshal func(any) error) error {
	return u.unmarshal(unmarshal)
}

// MarshalJSON implements json.Marshaller to print the url.
func (u *URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// MarshalYAML implements yaml.Marshaller to print the url.
func (u *URL) MarshalYAML() (any, error) {
	return u.String(), nil
}

func (u *URL) unmarshal(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	parsed, err := url.Parse(s)
	if err != nil {
		return err
	}

	u.URL = parsed
	return nil
}

// CertPool is a wrapper around x509.CertPool, which can be unmarshalled and
// constructed from a list of filenames.
type CertPool struct {
	Files []string
	*x509.CertPool
}

// UnmarshalJSON implements json.Unmarshaler.
func (cp *CertPool) UnmarshalJSON(b []byte) error {
	return cp.unmarshal(func(v any) error { return json.Unmarshal(b, v) })
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (cp *CertPool) UnmarshalYAML(unmarshal func(any) error) error {
	return cp.unmarshal(unmarshal)
}

// MarshalJSON implements json.Marshaller to print the cert pool.
func (cp *CertPool) MarshalJSON() ([]byte, error) {
	return json.Marshal(cp.Files)
}

// MarshalYAML implements yaml.Marshaller to print the cert pool.
func (cp *CertPool) MarshalYAML() (any, error) {
	return cp.Files, nil
}

func (cp *CertPool) unmarshal(unmarshal func(any) error) error {
	if err := unmarshal(&cp.Files); err != nil {
		return err
	}

	pool, err := certPoolFromFiles(cp.Files...)
	if err != nil {
		return err
	}

	cp.CertPool = pool
	return nil
}

// certPoolFromFiles returns an *x509.CertPool constructed from the given files.
// If no files are given, (nil, nil) will be returned.
func certPoolFromFiles(files ...string) (*x509.CertPool, error) {
	if len(files) == 0 {
		return nil, nil
	}

	roots := x509.NewCertPool()
	for _, f := range files {
		cert, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read cert file %s: %w", f, err)
		}
		if !roots.AppendCertsFromPEM(cert) {
			return nil, fmt.Errorf("invalid cert: %s", f)
		}
	}
	return roots, nil
}

// ProxyRule describes a regular expression matching rule for how to proxy a request.
type ProxyRule struct {
	Regx     *Regexp `yaml:"regx" mapstructure:"regx"`
	UseHTTPS bool    `yaml:"useHTTPS" mapstructure:"useHTTPS"`
	Direct   bool    `yaml:"direct" mapstructure:"direct"`

	// Redirect is the host to redirect to, if not empty
	Redirect string `yaml:"redirect" mapstructure:"redirect"`
}

func NewProxyRule(regx string, useHTTPS bool, direct bool, redirect string) (*ProxyRule, error) {
	exp, err := NewRegexp(regx)
	if err != nil {
		return nil, fmt.Errorf("invalid regexp: %w", err)
	}

	return &ProxyRule{
		Regx:     exp,
		UseHTTPS: useHTTPS,
		Direct:   direct,
		Redirect: redirect,
	}, nil
}

// Match checks if the given url matches the rule.
func (r *ProxyRule) Match(url string) bool {
	return r.Regx != nil && r.Regx.MatchString(url)
}

// Regexp is a simple wrapper around regexp. Regexp to make it unmarshallable from a string.
type Regexp struct {
	*regexp.Regexp
}

// NewRegexp returns a new Regexp instance compiled from the given string.
func NewRegexp(exp string) (*Regexp, error) {
	r, err := regexp.Compile(exp)
	if err != nil {
		return nil, err
	}
	return &Regexp{r}, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (r *Regexp) UnmarshalYAML(unmarshal func(any) error) error {
	return r.unmarshal(unmarshal)
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *Regexp) UnmarshalJSON(b []byte) error {
	return r.unmarshal(func(v any) error { return json.Unmarshal(b, v) })
}

func (r *Regexp) unmarshal(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	exp, err := regexp.Compile(s)
	if err == nil {
		r.Regexp = exp
	}
	return err
}

// MarshalJSON implements json.Marshaller to print the regexp.
func (r *Regexp) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// MarshalYAML implements yaml.Marshaller to print the regexp.
func (r *Regexp) MarshalYAML() (any, error) {
	return r.String(), nil
}

// HijackConfig represents how dfdaemon hijacks http requests.
type HijackConfig struct {
	Cert  string             `yaml:"cert" mapstructure:"cert"`
	Key   string             `yaml:"key" mapstructure:"key"`
	Hosts []*HijackHost      `yaml:"hosts" mapstructure:"hosts"`
	SNI   []*TCPListenOption `yaml:"sni" mapstructure:"sni"`
}

// HijackHost is a hijack rule for the hosts that matches Regx.
type HijackHost struct {
	Regx     *Regexp   `yaml:"regx" mapstructure:"regx"`
	Insecure bool      `yaml:"insecure" mapstructure:"insecure"`
	Certs    *CertPool `yaml:"certs" mapstructure:"certs"`
}

// TelemetryOption is the option for telemetry
type TelemetryOption struct {
	Jaeger string `yaml:"jaeger" mapstructure:"jaeger"`
}

type WhiteList struct {
	Host  string   `yaml:"host" mapstructure:"host"`
	Regx  *Regexp  `yaml:"regx" mapstructure:"regx"`
	Ports []string `yaml:"ports" mapstructure:"ports"`
}

type BasicAuth struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}
