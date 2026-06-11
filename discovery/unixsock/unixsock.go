package unixsock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/xyzj/miniapp/discovery"
	"github.com/xyzj/miniapp/framework"
)

// type peerState struct {
// 	Infos    map[string]*discovery.Info
// 	LastSeen time.Time
// }

type packet struct {
	Type string `json:"type"`
	// PID    int               `json:"pid,omitempty"`
	Infos  []*discovery.Info `json:"infos,omitempty"`
	Active []*discovery.Info `json:"active,omitempty"`
}

type Module struct {
	name string
	cfg  discovery.Config

	serverAddr *net.UnixAddr
	clientAddr *net.UnixAddr
	serverConn *net.UnixConn
	clientConn *net.UnixConn
	// isServer   bool

	infoCache *discovery.ServiceInfo
	servCache *discovery.ServiceInfo
	localRegs *discovery.ServiceInfo
	// peers     map[string]*peerState

	// mu     sync.RWMutex
	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New() *Module {
	return &Module{name: discovery.DefaultModuleName, cfg: discovery.Config{}}
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.clientConn == nil {
		return fmt.Errorf("unixsock discovery client is not initialized")
	}
	return nil
}

func (m *Module) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.ServiceGroup == "" {
		m.cfg.ServiceGroup = discovery.DefaultServiceGroup
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	var err error
	m.serverAddr, err = net.ResolveUnixAddr("unixgram", "@"+m.cfg.ServiceGroup)
	if err != nil {
		return fmt.Errorf("failed to resolve unix address: %w", err)
	}
	m.clientAddr, err = net.ResolveUnixAddr("unixgram", "@"+m.cfg.ServiceGroup+"_"+strconv.Itoa(os.Getpid()))
	if err != nil {
		return fmt.Errorf("failed to resolve unix address: %w", err)
	}
	m.infoCache = discovery.NewServiceInfo(discovery.DiscoveryInterval() + time.Second*10)
	m.servCache = discovery.NewServiceInfo(discovery.DiscoveryInterval()*2 + time.Second*10)
	m.localRegs = discovery.NewServiceInfo(0)
	// m.peers = make(map[string]*peerState)
	return nil
}

func (m *Module) Start(ctx framework.Context) error {
	m.runCtx, m.cancel = context.WithCancel(ctx)

	serverConn, err := net.ListenUnixgram("unixgram", m.serverAddr)
	if err == nil {
		// m.isServer = true
		m.serverConn = serverConn
		m.wg.Go(func() {
			m.serverLoop(ctx)
		})
		// m.wg.Add(1)
		// go m.serverLoop(ctx)
	} else if !isAddrInUse(err) {
		return fmt.Errorf("unixsock server listen failed: %w", err)
	}
	clientConn, err := net.DialUnix("unixgram", m.clientAddr, m.serverAddr)
	if err != nil {
		if m.serverConn != nil {
			_ = m.serverConn.Close()
			m.serverConn = nil
		}
		return fmt.Errorf("unixsock client listen failed: %w", err)
	}
	m.clientConn = clientConn
	// m.wg.Add(1)
	// go m.clientReadLoop(ctx)
	m.wg.Go(func() {
		m.clientReadLoop(ctx)
	})
	m.wg.Go(func() {
		m.clientRegisterLoop(ctx)
	})
	// m.wg.Add(1)
	// go m.clientRegisterLoop(ctx)
	ctx.Provide(m.name, m)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	_ = m.clientConn.Close()
	_ = m.serverConn.Close()
	m.wg.Wait()
	if m.clientConn != nil {
		m.clientConn = nil
	}
	if m.serverConn != nil {
		m.serverConn = nil
	}
	m.infoCache = nil
	m.servCache = nil
	m.localRegs = nil
	return nil
}

func (m *Module) Register(info *discovery.Info) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	info.EnsureData()
	_ = m.localRegs.Store(info)
	return nil
}

func (m *Module) Find(name string) (*discovery.Info, error) {
	if info, ok := m.infoCache.Load(name); ok {
		return info, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}

func (m *Module) clientRegisterLoop(ctx framework.Context) {
	// defer m.wg.Done()
	ticker := time.NewTicker(discovery.DiscoveryInterval())
	defer ticker.Stop()
	m.sendRegisterPacket(ctx)
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.sendRegisterPacket(ctx)
		}
	}
}

func (m *Module) clientReadLoop(ctx framework.Context) {
	// defer m.wg.Done()
	buf := make([]byte, 64*1024)
	for {
		n, err := m.clientConn.Read(buf)
		if err != nil {
			if m.runCtx.Err() != nil {
				return
			}
			ctx.Logger().Error("[DISCOVERY-UNIX] client read failed: " + err.Error())
			continue
		}
		var pkt packet
		if err := json.Unmarshal(buf[:n], &pkt); err != nil {
			ctx.Logger().Error("[DISCOVERY-UNIX] client decode failed: " + err.Error())
			continue
		}
		if pkt.Type != "active" {
			continue
		}
		for _, info := range pkt.Active {
			if info == nil {
				continue
			}
			_ = m.infoCache.Store(info)
		}
	}
}

func (m *Module) serverLoop(ctx framework.Context) {
	// defer m.wg.Done()
	buf := make([]byte, 64*1024)
	for {
		n, conn, err := m.serverConn.ReadFromUnix(buf)
		if err != nil {
			if m.runCtx.Err() != nil {
				return
			}
			ctx.Logger().Error("[DISCOVERY-UNIX] server read failed: " + err.Error())
			continue
		}
		func(conn *net.UnixAddr, b []byte) {
			var pkt packet
			if err := json.Unmarshal(b, &pkt); err != nil {
				ctx.Logger().Error("[DISCOVERY-UNIX] server decode failed: " + err.Error())
				return
			}
			if pkt.Type != "register" {
				return
			}
			for _, info := range pkt.Infos {
				if info == nil {
					continue
				}
				_ = m.servCache.Store(info)
			}
			resp, err := json.Marshal(packet{Type: "active", Active: m.servCache.All()})
			if err != nil {
				ctx.Logger().Error("[DISCOVERY-UNIX] server encode failed: " + err.Error())
				return
			}
			_, err = m.serverConn.WriteToUnix(resp, conn)
			if err != nil {
				ctx.Logger().Error("[DISCOVERY-UNIX] server write failed: " + err.Error())
			}
		}(conn, buf[:n])
	}
}

func (m *Module) sendRegisterPacket(ctx framework.Context) {
	if m.clientConn == nil {
		return
	}
	body, err := json.Marshal(packet{Type: "register", Infos: m.localRegs.All()})
	if err != nil {
		ctx.Logger().Error("[DISCOVERY-UNIX] client encode failed: " + err.Error())
		return
	}
	if _, err := m.clientConn.WriteToUnix(body, m.serverAddr); err != nil {
		ctx.Logger().Error("[DISCOVERY-UNIX] client write failed: " + err.Error())
	}
}

// func (m *Module) localInfos() []*discovery.Info {
// 	m.mu.RLock()
// 	defer m.mu.RUnlock()
// 	infos := make([]*discovery.Info, 0, m.localRegs.Len())
// 	for _, info := range m.localRegs.All() {
// 		infos = append(infos, cloneInfo(info))
// 	}
// 	return infos
// }

// func (m *Module) updatePeer(peer string, infos []*discovery.Info) {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	state, ok := m.peers[peer]
// 	if !ok {
// 		state = &peerState{Infos: make(map[string]*discovery.Info)}
// 		m.peers[peer] = state
// 	}
// 	state.LastSeen = time.Now()
// 	state.Infos = make(map[string]*discovery.Info, len(infos))
// 	for _, info := range infos {
// 		state.Infos[info.Instance] = cloneInfo(info)
// 	}
// }

// func (m *Module) activeInfos() []*discovery.Info {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	aliveWindow := discovery.DiscoveryInterval() * 2
// 	now := time.Now()
// 	res := make([]*discovery.Info, 0)
// 	for peer, state := range m.peers {
// 		if now.Sub(state.LastSeen) > aliveWindow {
// 			delete(m.peers, peer)
// 			continue
// 		}
// 		for _, info := range state.Infos {
// 			res = append(res, cloneInfo(info))
// 		}
// 	}
// 	return res
// }

// func cloneInfo(in *discovery.Info) *discovery.Info {
// 	if in == nil {
// 		return nil
// 	}
// 	out := *in
// 	if len(in.SourceIP) > 0 {
// 		out.SourceIP = append([]string(nil), in.SourceIP...)
// 	}
// 	return &out
// }

func isAddrInUse(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return errors.Is(opErr.Err, syscall.EADDRINUSE)
	}
	return errors.Is(err, syscall.EADDRINUSE)
}
