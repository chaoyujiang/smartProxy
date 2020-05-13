package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const FSM_TIMEOUT int64 = 10 * 60
const QUELEN int = 1024
const DATALEN int32 = 65535
const INVAILD_INT = 0x7fffffff
const EVENTCHAN_LEN = 64
const PORTNUMMAX = 65535

type DstBindPort struct {
	portFlag    [PORTNUMMAX]bool
	currentPort int32
	lock        sync.Mutex
}

var gDstBindUdpPort DstBindPort
var gDstBindTcpPort DstBindPort

/*********************************UDP******************************************/
type UdpDialState int32

func getDstBindUdpPort() int32 {
	gDstBindUdpPort.lock.Lock()
	defer gDstBindUdpPort.lock.Unlock()

	var i int32
	if gDstBindUdpPort.currentPort < 1025 || gDstBindUdpPort.currentPort >= PORTNUMMAX {
		gDstBindUdpPort.currentPort = 1025
	}
	for i = gDstBindUdpPort.currentPort; i <= PORTNUMMAX; i++ {
		if gDstBindUdpPort.portFlag[i] == false {
			break
		}
	}
	gDstBindUdpPort.currentPort = i
	gDstBindUdpPort.portFlag[i] = true
	return gDstBindUdpPort.currentPort
}
func resetDstBindUdpPort(port int32) {
	gDstBindUdpPort.lock.Lock()
	defer gDstBindUdpPort.lock.Unlock()
	if port < PORTNUMMAX {
		gDstBindUdpPort.portFlag[port] = false
	}
	return
}
func (this UdpDialState) String() string {
	switch this {
	case DAILING_STATUS_DAILING:
		return "Dailing"
	case DAILING_STATUS_FWDING:
		return "FWDING"
	case DAILING_STATUS_STOP:
		return "DailStop"
	default:
		return "Unknow"
	}
}

type UdpDialEvent int32

func (this UdpDialEvent) String() string {
	switch this {
	case DAILING_EVENT_START:
		return "DailStart"
	case DAILING_EVENT_OK:
		return "DailOK"
	case DAILING_EVENT_RETRY:
		return "DailRetry"
	case FWDING_EVENT_START:
		return "FWDINGStart"
	case DAILING_EVENT_STOP:
		return "DailStop"

	default:
		return "Unknow"
	}
}
func startTimer(f func(), timeout time.Duration) chan struct{} {
	done := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			f()
		case <-done:
		}
	}()
	return done
}

func closeTimer(id chan struct{}) {
	if id != nil {
		close(id)
	}
	return
}

// fwd object fsm
const (
	DAILING_STATUS_DAILING = iota
	DAILING_STATUS_FWDING
	DAILING_STATUS_STOP

	DAILING_STATUS_COUNT
)
const (
	DAILING_EVENT_START = iota
	DAILING_EVENT_OK
	DAILING_EVENT_RETRY

	FWDING_EVENT_START
	DAILING_EVENT_STOP
	DAILING_EVENT_COUNT
)

type DialFsm struct {
	Fwd         ForwardMap
	SrcInfo     *ReadSrcConn
	Event       chan UdpDialEvent
	State       UdpDialState
	TimerHandle chan struct{}
	Interval    time.Duration
	DialConn    *net.UDPConn
	ListenConn  *net.UDPConn
	KeepLive    int64
	BindPort    int32
}
type ReadSrcConn struct {
	IP   net.IP
	Port int
	Data []byte
}
type DialFwdHandle struct {
	FsmTable    map[string]*DialFsm
	ListenConn  *net.UDPConn
	ReadSrcInfo chan *ReadSrcConn
}

var gDailingFsmTable [DAILING_STATUS_COUNT][DAILING_EVENT_COUNT]UdpDialState

func initDailingFsmTbale() {
	for i := 0; i < DAILING_STATUS_COUNT; i++ {
		for j := 0; j < DAILING_EVENT_COUNT; j++ {
			gDailingFsmTable[i][j] = INVAILD_INT
		}
	}
	gDailingFsmTable[DAILING_STATUS_DAILING][DAILING_EVENT_START] = DAILING_STATUS_DAILING
	gDailingFsmTable[DAILING_STATUS_DAILING][DAILING_EVENT_OK] = DAILING_STATUS_FWDING
	gDailingFsmTable[DAILING_STATUS_DAILING][DAILING_EVENT_RETRY] = DAILING_STATUS_DAILING
	gDailingFsmTable[DAILING_STATUS_DAILING][DAILING_EVENT_STOP] = DAILING_STATUS_STOP

	gDailingFsmTable[DAILING_STATUS_FWDING][DAILING_EVENT_RETRY] = DAILING_STATUS_DAILING
	gDailingFsmTable[DAILING_STATUS_FWDING][FWDING_EVENT_START] = DAILING_STATUS_FWDING
	gDailingFsmTable[DAILING_STATUS_FWDING][DAILING_EVENT_STOP] = DAILING_STATUS_STOP
	return
}

func (self *DialFsm) couldChangeState(event UdpDialEvent) bool {
	state := gDailingFsmTable[self.State][event]
	if state != INVAILD_INT {
		return true
	}
	return false
}
func (self *DialFsm) changeState(event UdpDialEvent) {
	state := gDailingFsmTable[self.State][event]
	log.Printf("Change dialing state from [%s] to [%s] on event [%s]\n", self.State, state, event)
	self.State = state
	return
}
func (self *DialFsm) pushEvent(event UdpDialEvent) {
	self.Event <- event
}

func (self *DialFsm) connFwd() {
	name := self.Fwd.Name
	n, err := self.DialConn.Write(self.SrcInfo.Data)
	log.Printf("[%s]Write to dst. len %d  err %v", name, n, err)
	return
}
func (self *DialFsm) readDial2SrcUser() {
	name := self.Fwd.Name
	go func() {
		data := make([]byte, DATALEN)
		for {
			n, raddr, err := self.DialConn.ReadFromUDP(data)
			if err != nil {
				log.Printf("[%s]Failed to ReadFrom dst %v err %v.", name, raddr, err)
				self.pushEvent(DAILING_EVENT_RETRY)
				return
			}
			uaddr := net.UDPAddr{self.SrcInfo.IP, self.SrcInfo.Port, ""}
			if _, err := self.ListenConn.WriteToUDP(data[:n], &uaddr); err != nil {
				log.Printf("[%s]Failed to WriteToUDP user %v err %v.", name, uaddr, err)
			}
			log.Printf("[%s]Write from dst to user. uaddr = %v, n = %d", name, uaddr, n)
		}
	}()
	return
}
func (self *DialFsm) Stop() {
	if self.DialConn != nil {
		self.DialConn.Close()
	}
	closeTimer(self.TimerHandle)
	self.TimerHandle = nil
	if self.BindPort != 0 {
		resetDstBindUdpPort(self.BindPort)
	}
	return
}
func (self *DialFsm) DialFwdUserFsmRun() {
	initDailingFsmTbale()
	name := self.Fwd.Name
	go func() {
		defer log.Printf("[%s]Dial-FWD FSM stop", name)
	loop:
		for {
			event := <-self.Event
			log.Printf("[%s]DialToFwd udp event %s", name, event)
			if !self.couldChangeState(event) {
				log.Printf("[%s]Ignore dial invalid Event %s, state(%s) not changed.\n", name, event, self.State)
				continue
			}
			switch event {
			case DAILING_EVENT_START:
				rAddr, _ := net.ResolveUDPAddr("udp", self.Fwd.DstAddr)
				var lAddr *net.UDPAddr
				var port int32
				if self.Fwd.DstBindIp != DEFAULT_BIND_IP {
					port = getDstBindUdpPort()
					ipport := fmt.Sprintf("%s:%d", self.Fwd.DstBindIp, port)
					lAddr, _ = net.ResolveUDPAddr("udp", ipport)
					self.BindPort = port
				}
				if dialConn, err := stackDialUDPTo("udp", lAddr, rAddr); err != nil {
					log.Printf("[%s]Failed to dialudp host %v", name, rAddr)
					resetDstBindUdpPort(port)
					self.pushEvent(DAILING_EVENT_RETRY)
				} else {
					self.DialConn = dialConn
					self.pushEvent(DAILING_EVENT_OK)
				}
			case DAILING_EVENT_OK:
				self.readDial2SrcUser()
				self.pushEvent(FWDING_EVENT_START)
			case DAILING_EVENT_RETRY:
				self.TimerHandle = startTimer(func() { self.pushEvent(DAILING_EVENT_START) }, self.Interval*time.Second)
			case FWDING_EVENT_START:
				self.connFwd()
			case DAILING_EVENT_STOP:
				self.Stop()
				break loop
			default:
				log.Printf("[%s]Dial-FWD FSM unknow event", name)
			}
			self.changeState(event)
		}
	}()
	return
}

func (self *DialFwdHandle) Stop() {
	close(self.ReadSrcInfo)
	for key, dialFsm := range self.FsmTable {
		dialFsm.pushEvent(DAILING_EVENT_STOP)
		delete(self.FsmTable, key)
		log.Printf("Stop dailFsm ,key = %s", key)
	}
	return
}
func fsmTableTimeout(fsmTable map[string]*DialFsm) {

	for key, dialFsm := range fsmTable {
		if dialFsm.KeepLive < time.Now().Unix() {
			dialFsm.pushEvent(DAILING_EVENT_STOP)
			delete(fsmTable, key)
			log.Printf("Timeout to delete fsmTable,key = %s", key)
		}
	}
	return
}
func (self *DialFwdHandle) UdpDialRun(fwd ForwardMap) {
	timer := time.NewTimer(1 * time.Minute)
	go func() {
		defer timer.Stop()
		for {
			select {
			case srcInfo, ok := <-self.ReadSrcInfo:
				if !ok {
					log.Printf("[%s]UDP ReadSrcInfo chan is closed", fwd.Name)
					return
				}
				key := srcInfo.IP.String() + ":" + fmt.Sprintf("%d", srcInfo.Port)
				if _, ok := self.FsmTable[key]; !ok {
					self.FsmTable[key] = &DialFsm{
						Fwd:        fwd,
						SrcInfo:    srcInfo,
						Event:      make(chan UdpDialEvent, EVENTCHAN_LEN),
						State:      DAILING_STATUS_DAILING,
						Interval:   time.Duration(5),
						ListenConn: self.ListenConn,
						KeepLive:   time.Now().Unix() + FSM_TIMEOUT}

					self.FsmTable[key].DialFwdUserFsmRun() 
					self.FsmTable[key].pushEvent(DAILING_EVENT_START)
				} else {
					self.FsmTable[key].SrcInfo = srcInfo
					self.FsmTable[key].KeepLive = time.Now().Unix() + FSM_TIMEOUT
					self.FsmTable[key].pushEvent(FWDING_EVENT_START)
				}
			case <-timer.C:
				fsmTableTimeout(self.FsmTable)
				timer.Reset(time.Minute * 1)
			}
		}
	}()
	return
}

// listen object fsm
type UdpListenState int32

func (this UdpListenState) String() string {
	switch this {
	case LISTEN_STATUS_LISTEN:
		return "Listen"
	case LISTEN_STATUS_ACCEPT:
		return "Accept"
	case LISTEN_STATUS_STOPED:
		return "ListenStop"
	default:
		return "Unknow"
	}
}

type UdpListenEvent int32

func (this UdpListenEvent) String() string {
	switch this {
	case LISTEN_EVENT_START:
		return "ListenStart"
	case LISTEN_EVENT_OK:
		return "ListenOK"
	case LISTEN_EVENT_RETRY:
		return "ListenRetry"
	case LISTEN_EVENT_STOP:
		return "ListenStop"
	case ACCEPT_EVENT_START:
		return "AcceptStart"
	default:
		return "Unknow"
	}
}

const (
	LISTEN_STATUS_LISTEN = iota
	LISTEN_STATUS_ACCEPT
	LISTEN_STATUS_STOPED

	LISTEN_STATUS_COUNT
)
const (
	LISTEN_EVENT_START = iota
	LISTEN_EVENT_OK
	LISTEN_EVENT_RETRY

	ACCEPT_EVENT_START

	LISTEN_EVENT_STOP
	LISTEN_EVENT_COUNT
)

var gListenFsmTable [LISTEN_STATUS_COUNT][LISTEN_EVENT_COUNT]UdpListenState

type L2DUDPFWDHandle struct {
	Event       chan UdpListenEvent
	State       UdpListenState
	ListenConn  *net.UDPConn
	TimerHandle chan struct{}
	Interval    time.Duration
	DialFwd     *DialFwdHandle
	Lock        sync.Mutex
}

func (self *L2DUDPFWDHandle) couldChangeState(event UdpListenEvent) bool {
	state := gListenFsmTable[self.State][event]
	if state != INVAILD_INT {
		return true
	}
	return false
}
func (self *L2DUDPFWDHandle) changeState(event UdpListenEvent) {
	state := gListenFsmTable[self.State][event]
	log.Printf("Change state from [%s] to [%s] on event [%s]\n", self.State, state, event)
	self.State = state
	return
}
func initListenFsmTbale() {
	for i := 0; i < LISTEN_STATUS_COUNT; i++ {
		for j := 0; j < LISTEN_EVENT_COUNT; j++ {
			gListenFsmTable[i][j] = INVAILD_INT
		}
	}
	gListenFsmTable[LISTEN_STATUS_STOPED][LISTEN_EVENT_START] = LISTEN_STATUS_LISTEN

	gListenFsmTable[LISTEN_STATUS_LISTEN][LISTEN_EVENT_START] = LISTEN_STATUS_LISTEN
	gListenFsmTable[LISTEN_STATUS_LISTEN][LISTEN_EVENT_OK] = LISTEN_STATUS_ACCEPT
	gListenFsmTable[LISTEN_STATUS_LISTEN][LISTEN_EVENT_RETRY] = LISTEN_STATUS_LISTEN
	gListenFsmTable[LISTEN_STATUS_LISTEN][LISTEN_EVENT_STOP] = LISTEN_STATUS_STOPED

	gListenFsmTable[LISTEN_STATUS_ACCEPT][ACCEPT_EVENT_START] = LISTEN_STATUS_ACCEPT
	gListenFsmTable[LISTEN_STATUS_ACCEPT][LISTEN_EVENT_START] = LISTEN_STATUS_LISTEN
	gListenFsmTable[LISTEN_STATUS_ACCEPT][LISTEN_EVENT_STOP] = LISTEN_STATUS_STOPED
	return
}

func (self *L2DUDPFWDHandle) pushEvent(event UdpListenEvent) {
	self.Event <- event
}

func (fwdHandle *L2DUDPFWDHandle) acceptUDP(fwd ForwardMap) {

	data := make([]byte, DATALEN)
	go func() {
		for {
			n, rAddr, err := fwdHandle.ListenConn.ReadFromUDP(data)
			if err != nil || rAddr == nil {
				log.Printf("[%s]Failed to read udp %v.", fwd.Name, err)
				return
			}
			log.Printf("[%s]user %v connect to %s n = %d", fwd.Name, rAddr, fwd.DstAddr, n)
			port := strings.Split(fwd.SourceAddr, ":")[1]
			if flag := isAllowPass(fwd.Protocol, port, fwd.Name, rAddr.IP,fwd.AclRule); !flag {
				log.Printf("[%s]user %v refused connect to %s ", fwd.Name, rAddr, fwd.DstAddr)
				continue
			}
			fwdHandle.DialFwd.ReadSrcInfo <- &ReadSrcConn{
				IP:   rAddr.IP,
				Port: rAddr.Port,
				Data: data[:n]}
		}
	}()
	return
}
func (fwdHandle *L2DUDPFWDHandle) Stop() {
	if fwdHandle.ListenConn != nil {
		fwdHandle.ListenConn.Close()
	}
	closeTimer(fwdHandle.TimerHandle)
	fwdHandle.TimerHandle = nil
	if fwdHandle.DialFwd != nil {
		fwdHandle.DialFwd.Stop()
	}
	close(fwdHandle.Event)
	fwdHandle.Event = nil
	return
}
func (fwdCtl *ForwardCtl) UdpListenFsmRun() {
	fwdHandle := fwdCtl.fwdHandle.(*L2DUDPFWDHandle)
	fwdHandle.Lock.Lock()
	initListenFsmTbale()
	if fwdHandle.Event == nil {
		fwdHandle.Event = make(chan UdpListenEvent, EVENTCHAN_LEN)
	}
	fwd := fwdCtl.singleFwd
	name := fwd.Name
	go func() {
		defer log.Printf("[%s]Listen udp FSM stop", name)
		defer fwdHandle.Lock.Unlock()
	loop:
		for {
			event, ok := <-fwdHandle.Event
			if !ok {
				log.Printf("[%s]UDP Event chan is closed", name)
				break loop
			}
			log.Printf("[%s]Listen2Dial udp event %s", name, event)
			if !fwdHandle.couldChangeState(event) {
				log.Printf("[%s]Ignore Invalid Event %s, state(%s) not changed.\n", name, event, fwdHandle.State)
				continue
			}
			switch event {
			case LISTEN_EVENT_START:
				if listenConn, err := stackListenUDP(fwd.SourceAddr); err != nil {
					log.Printf("[%s]Failed to listen udp bindhost %s  err %v.", name, fwd.SourceAddr, err)
					fwdHandle.pushEvent(LISTEN_EVENT_RETRY)
				} else {
					fwdHandle.ListenConn = listenConn
					fwdHandle.pushEvent(LISTEN_EVENT_OK)
				}
			case LISTEN_EVENT_OK:
				fwdHandle.pushEvent(ACCEPT_EVENT_START)
			case LISTEN_EVENT_RETRY:
				fwdHandle.TimerHandle = startTimer(func() { fwdHandle.pushEvent(LISTEN_EVENT_START) }, fwdHandle.Interval*time.Second)
			case ACCEPT_EVENT_START:
				fwdHandle.DialFwd = &DialFwdHandle{
					FsmTable:    make(map[string]*DialFsm),
					ListenConn:  fwdHandle.ListenConn,
					ReadSrcInfo: make(chan *ReadSrcConn, QUELEN),
				}
				fwdHandle.DialFwd.UdpDialRun(fwd)
				fwdHandle.acceptUDP(fwd)
			case LISTEN_EVENT_STOP:
				fwdHandle.Stop()
				fwdHandle.changeState(event)
				break loop
			default:
				log.Printf("[%s]Listen FSM unknow event", name)
			}
			fwdHandle.changeState(event)
		}
	}()
	return
}

func (fwdCtl *ForwardCtl) listen2DialUdpProcess() {
	handle := fwdCtl.fwdHandle.(*L2DUDPFWDHandle)

	handle.pushEvent(LISTEN_EVENT_STOP)
	fwdCtl.UdpListenFsmRun() 
	handle.pushEvent(LISTEN_EVENT_START)
	return
}

/**********************************TCP*****************************************/
type FwdFsmCtl struct {
	Event    chan TcpFwdEvent
	State    TcpFwdState
	ConnCtl  ConnHandle
	FwdName  string
	BindPort int32
}
type TcpFwdState int32

func getDstBindTcpPort() int32 {
	gDstBindTcpPort.lock.Lock()
	defer gDstBindTcpPort.lock.Unlock()

	var i int32

	if gDstBindTcpPort.currentPort < 1025 || gDstBindTcpPort.currentPort >= PORTNUMMAX {
		gDstBindTcpPort.currentPort = 1025
	}
	for i = gDstBindTcpPort.currentPort; i <= PORTNUMMAX; i++ {
		if gDstBindTcpPort.portFlag[i] == false {
			break
		}
	}
	gDstBindTcpPort.currentPort = i
	gDstBindTcpPort.portFlag[i] = true
	return gDstBindTcpPort.currentPort
}
func resetDstBindTcpPort(port int32) {
	gDstBindTcpPort.lock.Lock()
	defer gDstBindTcpPort.lock.Unlock()
	if port < PORTNUMMAX {
		gDstBindTcpPort.portFlag[port] = false
	}
	return
}
func (this TcpFwdState) String() string {
	switch this {
	case TCP_FWD_STATUS_FWDING:
		return "TCP_FWDING"
	case TCP_FWD_STATUS_STOPED:
		return "TCP_FWD_STOP"
	default:
		return "TCP_FWD_Unknow"
	}
}

type TcpFwdEvent int32

func (this TcpFwdEvent) String() string {
	switch this {
	case TCP_FWD_EVENT_START:
		return "TCP_Fwd_Start"
	case TCP_FWD_EVENT_STOP:
		return "TCP_Fwd_Stop"
	default:
		return "TCP_Unknow"
	}
}

const (
	TCP_FWD_STATUS_FWDING = iota
	TCP_FWD_STATUS_STOPED

	TCP_FWD_STATUS_COUNT
)
const (
	TCP_FWD_EVENT_START = iota
	TCP_FWD_EVENT_STOP

	TCP_FWD_EVENT_COUNT
)

func (self *FwdFsmCtl) couldChangeState(event TcpFwdEvent) bool {
	state := gTcpFwdFsmTable[self.State][event]
	if state != INVAILD_INT {
		return true
	}
	return false
}
func (self *FwdFsmCtl) changeState(event TcpFwdEvent) {
	state := gTcpFwdFsmTable[self.State][event]
	log.Printf("Change fwd state from [%s] to [%s] on event [%s]\n", self.State, state, event)
	self.State = state
	return
}
func (self *FwdFsmCtl) pushEvent(event TcpFwdEvent) {
	self.Event <- event
}
func (self *FwdFsmCtl) safePushStopEvent() {
	select {
	case _, isClose := <-self.Event:
		if !isClose {
			log.Printf("channel closed!")
		} else {
			self.Event <- TCP_FWD_EVENT_STOP
		}
	default:
		self.Event <- TCP_FWD_EVENT_STOP
	}
	return
}

var gTcpFwdFsmTable [TCP_FWD_STATUS_COUNT][TCP_FWD_EVENT_COUNT]TcpFwdState

func initTcpFwdFsmTbale() {
	for i := 0; i < TCP_FWD_STATUS_COUNT; i++ {
		for j := 0; j < TCP_FWD_EVENT_COUNT; j++ {
			gTcpFwdFsmTable[i][j] = INVAILD_INT
		}
	}
	gTcpFwdFsmTable[TCP_FWD_STATUS_FWDING][TCP_FWD_EVENT_START] = TCP_FWD_STATUS_FWDING
	gTcpFwdFsmTable[TCP_FWD_STATUS_FWDING][TCP_FWD_EVENT_STOP] = TCP_FWD_STATUS_STOPED

	gTcpFwdFsmTable[TCP_FWD_STATUS_STOPED][TCP_FWD_EVENT_START] = TCP_FWD_STATUS_FWDING
	gTcpFwdFsmTable[TCP_FWD_STATUS_STOPED][TCP_FWD_EVENT_STOP] = TCP_FWD_STATUS_STOPED

	return
}
func (self *FwdFsmCtl) Stop() {

	self.ConnCtl.CloseConn()
	resetDstBindTcpPort(self.BindPort)
	close(self.Event)
	self.Event = nil
	return
}
func (self *FwdFsmCtl) TcpFwdFsmRun() {
	initTcpFwdFsmTbale()
	if self.Event == nil {
		self.Event = make(chan TcpFwdEvent, EVENTCHAN_LEN)
	}

	go func() {
		defer log.Printf("[%s]TCP FWD FSM stop", self.FwdName)
	loop:
		for {
			event, ok := <-self.Event
			if !ok {
				log.Printf("[%s]TCP Fwd Event chan is closed", self.FwdName)
				break loop
			}
			log.Printf("[%s]Fwd tcp event %s", self.FwdName, event)
			if !self.couldChangeState(event) {
				log.Printf("[%s]Ignore Invalid tcp fwd Event %s, state(%s) not changed.\n", self.FwdName, event, self.State)
				continue
			}
			switch event {
			case TCP_FWD_EVENT_START:
				go connFwd(self.ConnCtl.UpConn, self.ConnCtl.DownConn)
				log.Printf("[%s]TCP Fwd FSM start", self.FwdName)
			case TCP_FWD_EVENT_STOP:
				self.Stop()
				self.changeState(event)
				break loop
			default:
				log.Printf("[%s]TCP Fwd FSM unknow event", self.FwdName)
			}
			self.changeState(event)
		}
	}()
	return
}

type AcceptInfo struct {
	Lock        sync.Mutex
	Conn        []ConnHandle
	TcpBindPort []int32
}
type L2DTCPFWDHandle struct {
	Event       chan TcpListenEvent
	State       TcpListenState
	ListenConn  net.Listener
	TimerHandle chan struct{}
	Interval    time.Duration
	Lock        sync.Mutex
	AcceptCtl   AcceptInfo
	FwdFsmObj   []FwdFsmCtl
}

func (self *L2DTCPFWDHandle) Stop() {

	self.ListenConn.Close()
	for _, o := range self.FwdFsmObj {
		o.safePushStopEvent()
	}
	self.FwdFsmObj = nil
	closeTimer(self.TimerHandle)
	self.TimerHandle = nil
	close(self.Event)
	self.Event = nil
	return
}

func (fwdHandle *L2DTCPFWDHandle) listen2DialTcpBuildConnect(downConn net.Conn, fwd ForwardMap) (net.Conn, int32, error) {
	log.Printf("[%s]user %s connect to %s ", fwd.Name, downConn.RemoteAddr(), fwd.DstAddr)
	port := strings.Split(fwd.SourceAddr, ":")[1]
	addr, _ := net.ResolveTCPAddr("tcp", downConn.RemoteAddr().String())
	if flag := isAllowPass(fwd.Protocol, port, fwd.Name, addr.IP, fwd.AclRule); !flag {
		log.Printf("[%s]user %s refused tcp connect to %s ", fwd.Name, downConn.RemoteAddr(), fwd.DstAddr)
		return nil, 0, errors.New("ACL deny")
	}
	var lTcpAddr *net.TCPAddr
	var bindPort int32
	if fwd.DstBindIp != DEFAULT_BIND_IP {
		bindPort = getDstBindTcpPort()
		lAddr := fmt.Sprintf("%s:%d", fwd.DstBindIp, bindPort)
		lTcpAddr, _ = net.ResolveTCPAddr(PROTOCOL_TCP, lAddr)
	}
	rTcpAddr, _ := net.ResolveTCPAddr(PROTOCOL_TCP, fwd.DstAddr)
	up, err := stackDialTCPTo(PROTOCOL_TCP, lTcpAddr, rTcpAddr)
	if err != nil {
		log.Printf("[%s]connect to %s failed", fwd.Name, fwd.DstAddr)
		resetDstBindTcpPort(bindPort)
		return nil, 0, err
	}
	return up, bindPort, nil
}

type TcpListenState int32

func (this TcpListenState) String() string {
	switch this {
	case TCP_LISTEN_STATUS_LISTEN:
		return "TCP Listen"
	case TCP_LISTEN_STATUS_ACCEPT:
		return "TCP Accept"
	case TCP_LISTEN_STATUS_STOPED:
		return "TCP ListenStop"
	default:
		return "TCP_Unknow"
	}
}

type TcpListenEvent int32

func (this TcpListenEvent) String() string {
	switch this {
	case TCP_LISTEN_EVENT_START:
		return "TCP_ListenStart"
	case TCP_LISTEN_EVENT_OK:
		return "TCP_ListenOK"
	case TCP_LISTEN_EVENT_RETRY:
		return "TCP_ListenRetry"
	case TCP_LISTEN_EVENT_STOP:
		return "TCP_ListenStop"
	case TCP_ACCEPT_EVENT_START:
		return "TCP_AcceptStart"
	case TCP_ACCEPT_EVENT_FWD:
		return "TCP_AcceptFwding"
	default:
		return "TCP_Unknow"
	}
}

const (
	TCP_LISTEN_STATUS_LISTEN = iota
	TCP_LISTEN_STATUS_ACCEPT
	TCP_LISTEN_STATUS_STOPED

	TCP_LISTEN_STATUS_COUNT
)
const (
	TCP_LISTEN_EVENT_START = iota
	TCP_LISTEN_EVENT_OK
	TCP_LISTEN_EVENT_RETRY

	TCP_ACCEPT_EVENT_START
	TCP_ACCEPT_EVENT_FWD
	TCP_LISTEN_EVENT_STOP
	TCP_LISTEN_EVENT_COUNT
)

func (self *L2DTCPFWDHandle) couldChangeState(event TcpListenEvent) bool {
	state := gTcpListenFsmTable[self.State][event]
	if state != INVAILD_INT {
		return true
	}
	return false
}
func (self *L2DTCPFWDHandle) changeState(event TcpListenEvent) {
	state := gTcpListenFsmTable[self.State][event]
	log.Printf("Change state from [%s] to [%s] on event [%s]\n", self.State, state, event)
	self.State = state
	return
}
func (self *L2DTCPFWDHandle) pushEvent(event TcpListenEvent) {
	self.Event <- event
}

var gTcpListenFsmTable [TCP_LISTEN_STATUS_COUNT][TCP_LISTEN_EVENT_COUNT]TcpListenState

func initTcpListenFsmTbale() {
	for i := 0; i < TCP_LISTEN_STATUS_COUNT; i++ {
		for j := 0; j < TCP_LISTEN_EVENT_COUNT; j++ {
			gTcpListenFsmTable[i][j] = INVAILD_INT
		}
	}
	gTcpListenFsmTable[TCP_LISTEN_STATUS_STOPED][TCP_LISTEN_EVENT_START] = TCP_LISTEN_STATUS_LISTEN

	gTcpListenFsmTable[TCP_LISTEN_STATUS_LISTEN][TCP_LISTEN_EVENT_START] = TCP_LISTEN_STATUS_LISTEN
	gTcpListenFsmTable[TCP_LISTEN_STATUS_LISTEN][TCP_LISTEN_EVENT_OK] = TCP_LISTEN_STATUS_ACCEPT
	gTcpListenFsmTable[TCP_LISTEN_STATUS_LISTEN][TCP_LISTEN_EVENT_RETRY] = TCP_LISTEN_STATUS_LISTEN
	gTcpListenFsmTable[TCP_LISTEN_STATUS_LISTEN][TCP_LISTEN_EVENT_STOP] = TCP_LISTEN_STATUS_STOPED

	gTcpListenFsmTable[TCP_LISTEN_STATUS_ACCEPT][TCP_ACCEPT_EVENT_START] = TCP_LISTEN_STATUS_ACCEPT
	gTcpListenFsmTable[TCP_LISTEN_STATUS_ACCEPT][TCP_LISTEN_EVENT_START] = TCP_LISTEN_STATUS_LISTEN
	gTcpListenFsmTable[TCP_LISTEN_STATUS_ACCEPT][TCP_ACCEPT_EVENT_FWD] = TCP_LISTEN_STATUS_ACCEPT
	gTcpListenFsmTable[TCP_LISTEN_STATUS_ACCEPT][TCP_LISTEN_EVENT_STOP] = TCP_LISTEN_STATUS_STOPED
	return
}
func (self *AcceptInfo) saveAcceptInfo(down, up net.Conn, bindPort int32) {
	self.Lock.Lock()
	defer self.Lock.Unlock()
	self.Conn = append(self.Conn, ConnHandle{DownConn: down, UpConn: up})
	self.TcpBindPort = append(self.TcpBindPort, bindPort)
	return
}
func (self *AcceptInfo) popFirstAcceptInfo() (conn ConnHandle, bindport int32) {
	self.Lock.Lock()
	defer self.Lock.Unlock()
	conn = self.Conn[0]
	bindport = self.TcpBindPort[0]
	self.Conn = self.Conn[1:]
	self.TcpBindPort = self.TcpBindPort[1:]
	return
}

func (fwdHandle *L2DTCPFWDHandle) acceptTcp(fwd ForwardMap) {
	go func() {
		for {
			var downConn net.Conn
			var up net.Conn
			var bindPort int32
			var err error
			downConn, err = fwdHandle.ListenConn.Accept()
			if err != nil {
				log.Printf("TCP Accept failed:%v", err)
				return
			}
			if up, bindPort, err = fwdHandle.listen2DialTcpBuildConnect(downConn, fwd); err != nil {
				log.Printf("Failed to build connect:%v", err)
				downConn.Close()
				continue
			}
			fwdHandle.AcceptCtl.saveAcceptInfo(downConn, up, bindPort)
			fwdHandle.pushEvent(TCP_ACCEPT_EVENT_FWD)
		}
	}()
	return
}
func (fwdCtl *ForwardCtl) TcpListenFsmRun() {
	fwdHandle := fwdCtl.fwdHandle.(*L2DTCPFWDHandle)
	fwdHandle.Lock.Lock()
	name := fwdCtl.singleFwd.Name
	log.Printf("[%s]TCP listen fsm begin", name)
	initTcpListenFsmTbale()

	if fwdHandle.Event == nil {
		fwdHandle.Event = make(chan TcpListenEvent, EVENTCHAN_LEN)
	}
	fwd := fwdCtl.singleFwd

	go func() {
		defer log.Printf("[%s]TCP Listen FSM stop", name)
		defer fwdHandle.Lock.Unlock()
	loop:
		for {
			event, ok := <-fwdHandle.Event
			if !ok {
				log.Printf("[%s]TCP Event chan is closed", name)
				break loop
			}
			log.Printf("[%s]Listen2Dial tcp event %s", name, event)
			if !fwdHandle.couldChangeState(event) {
				log.Printf("[%s]Ignore Invalid tcp Event %s, state(%s) not changed.\n", name, event, fwdHandle.State)
				continue
			}
			switch event {
			case TCP_LISTEN_EVENT_START:
				if listenConn, err := StackListenTCP(fwd.SourceAddr); err != nil {
					log.Printf("[%s]Failed to listen tcp listenhost %s Stackt %s err %v.", name, fwd.SourceAddr, err)
					fwdHandle.pushEvent(TCP_LISTEN_EVENT_RETRY)
				} else {
					fwdHandle.ListenConn = listenConn
					fwdHandle.pushEvent(TCP_LISTEN_EVENT_OK)
				}
			case TCP_LISTEN_EVENT_OK:
				fwdHandle.pushEvent(TCP_ACCEPT_EVENT_START)
			case TCP_LISTEN_EVENT_RETRY:
				fwdHandle.TimerHandle = startTimer(func() { fwdHandle.pushEvent(TCP_LISTEN_EVENT_START) }, fwdHandle.Interval*time.Second)
			case TCP_ACCEPT_EVENT_START:
				fwdHandle.acceptTcp(fwd)
			case TCP_ACCEPT_EVENT_FWD:
				Conn, bindport := fwdHandle.AcceptCtl.popFirstAcceptInfo()
				fwdFsmObj := FwdFsmCtl{
					Event:    make(chan TcpFwdEvent, EVENTCHAN_LEN),
					State:    TCP_FWD_STATUS_STOPED,
					ConnCtl:  Conn,
					FwdName:  fwdCtl.singleFwd.Name,
					BindPort: bindport,
				}
				fwdFsmObj.TcpFwdFsmRun() //fwd fsm run
				fwdFsmObj.pushEvent(TCP_FWD_EVENT_START)
				fwdHandle.FwdFsmObj = append(fwdHandle.FwdFsmObj, fwdFsmObj) //save fwdfsm object
			case TCP_LISTEN_EVENT_STOP:
				fwdHandle.Stop()
				fwdHandle.changeState(event)
				break loop
			default:
				log.Printf("[%s]TCP Listen FSM unknow event", name)
			}
			fwdHandle.changeState(event)
		}
	}()
	return
}
func (fwdCtl *ForwardCtl) listen2DialTcpProcess() {
	handle := fwdCtl.fwdHandle.(*L2DTCPFWDHandle)
	fwdCtl.TcpListenFsmRun() 
	handle.pushEvent(TCP_LISTEN_EVENT_START)
	return
}

func (fwdCtl *ForwardCtl) listen2DialProcess() {

	log.Printf("Listen %s ----> Dial %s, protocol %s",
		fwdCtl.singleFwd.SourceAddr, fwdCtl.singleFwd.DstAddr, fwdCtl.singleFwd.Protocol)
	if fwdCtl.singleFwd.Protocol == PROTOCOL_TCP {
		fwdCtl.listen2DialTcpProcess()
	} else {
		fwdCtl.listen2DialUdpProcess()
	}
}
