package main

import (
	"log"
	"net"
	"sync"
	"time"
    "io"
    "io/ioutil"
    "os"
    "encoding/json"
    "flag"
)

type ForwardCtl struct {
	singleFwd ForwardMap
	fwdHandle interface{}
}
type ForwardAllMap struct {
	FwdMsg  map[string]*ForwardCtl
    fwdInfo map[string]ForwardMap
}
const (
    PROTOCOL_TCP = "tcp"
    PROTOCOL_UDP = "udp"
	DEFAULT_BIND_IP          = "0.0.0.0"
	CONF_FILE                = "smartProxy.conf"
	FWD_MAP_LEN              = 50
)
const (
    ACL_TYPE_SET int32 = iota
    ACL_TYPE_DEL
    ACL_TYPE_ADD
)
const (
    ACL_RULE_PERMIT int32 = iota
    ACL_RULE_DENY
)

type ACLNodes struct {
    IPNets []string
    Rule   int32
}
type ForwardMap struct {
	Name           string
	SourceAddr     string
    DstBindIp      string
	DstAddr        string
	Protocol       string
    AclRule        ACLNodes
}
func (self *ForwardAllMap)forwardMapInit(conf string) {
	log.Printf("read json file, conf = %s", conf)
	var jsonInfo map[string]ForwardMap
	cfg, err := ioutil.ReadFile(conf)
	if err != nil {
		log.Printf("Read %s file failed:%v",conf, err)
		os.Exit(1)
		return
	}

	err = json.Unmarshal(cfg, &jsonInfo)
    if err != nil {
		log.Printf("Parase %s file failed:%v", conf, err)
		os.Exit(1)
		return
	}
	for name, jsonMap := range jsonInfo {
		log.Printf("Add new forward map from json,%v", jsonMap)
        self.fwdInfo[name] = jsonMap
	}
}

type ConnHandle struct {
    DownConn net.Conn
    UpConn   net.Conn
    lock     sync.Mutex
}
func (self *ConnHandle) CloseConn() {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.DownConn != nil {
		self.DownConn.Close()
	}
	if self.UpConn != nil {
		self.UpConn.Close()
	}

	return
}
func (self *ConnHandle) SaveConn(downConn, up net.Conn) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.DownConn = downConn
	self.UpConn = up
	return
}
func checkRule(acl ACLNodes, ip net.IP) bool {
    if acl.Rule == ACL_RULE_PERMIT { 
        if nil == acl.IPNets {
            return true
        }   
        for _, ipNetString := range acl.IPNets {
            _,ipNet,err := net.ParseCIDR(ipNetString)
            if err != nil {
                log.Printf("parse acl cidr error %v ipNetString %s", err, ipNetString)
                continue
            }
            if true == ipNet.Contains(ip) {
                return true
            }   
        }   
        return false
    } else if acl.Rule == ACL_RULE_DENY { 
        if nil == acl.IPNets {
            return false
        }   
        for _, ipNetString := range acl.IPNets {
            _,ipNet,err := net.ParseCIDR(ipNetString)
            if err != nil {
                log.Printf("parse acl cidr error %v ipNetString %s", err, ipNetString)
                continue
            }
            if true == ipNet.Contains(ip) {
                return false
            }   
        }   
        return true
    } else {
        return true
    }   
}
func isAllowPass(protocol, port, fwdName string, ip net.IP,aclRule ACLNodes) bool {
    var configKey string = fwdName
    log.Printf("userinfo: configKey %s ip %v", configKey, ip) 
    return checkRule(aclRule, ip) 
}
func StackListenTCP(host string) (net.Listener, error) {
    var err error
    var ln net.Listener

    ln, err = net.Listen(PROTOCOL_TCP, host)
    log.Printf("StackListenTCP(host = %s,err = %v)", host, err)
    return ln, err 
}

func stackListenUDP(host string) (*net.UDPConn, error) {
    var conn *net.UDPConn
    var err error

    hostaddr, _ := net.ResolveUDPAddr("udp", host)
    conn, err = net.ListenUDP("udp", hostaddr)
    log.Printf("StackListenUDP(host = %s, err = %v)", host, err)
    return conn, err 
}
func stackDialUDPTo(protocol string, laddr, rAddr *net.UDPAddr ) (*net.UDPConn, error) {
    var conn *net.UDPConn
    var err error

    conn, err = net.DialUDP(protocol, laddr, rAddr)
    log.Printf("DialUDP laddr %v rAddr %v err %v\n", laddr, rAddr, err)
    if err != nil {
        return nil, err 
    }   
    return conn, nil 
}
func stackDialTCPTo(protocol string, lAddr, rAddr *net.TCPAddr) (*net.TCPConn, error) {
    var err error
    var conn *net.TCPConn

    conn, err = net.DialTCP(protocol, lAddr, rAddr)
    log.Printf("stackDialTCPTo(lAddr = %s, rAddr = %s)", lAddr, rAddr)

    return conn, err 
}
func StackConn(host string,protocol string) (net.Conn, error) {
    var err error
    var conn net.Conn

    conn, err = net.Dial(protocol, host)
    log.Printf("StackConn(host = %s, protocol = %s)", host, protocol)

    return conn, err 
}

func streamFwd(dst net.Conn, src net.Conn, waiter *sync.WaitGroup, quitFlag *int32) {

    sraddr := src.RemoteAddr()
    sladdr := src.LocalAddr()
    dladdr := dst.LocalAddr()
    draddr := dst.RemoteAddr()

    log.Printf("io.Copy data %s-->%s ---> %s-->%s", sraddr, sladdr, dladdr, draddr)
    _, err := io.Copy(dst, src)
    log.Printf("io.Copy quit %s-->%s ---> %s-->%s, err:%v", sraddr, sladdr, dladdr, draddr, err)
    log.Printf("close Conn(SRC) %s->%s", sladdr, sraddr)
    src.Close()

    log.Printf("close Conn(DST) %s->%s", dladdr, draddr)
    dst.Close()

    waiter.Done()
}
func connFwd(down net.Conn, up net.Conn) int32 {
    var waiter sync.WaitGroup
    waiter.Add(2)
    var quitFlag int32
    go streamFwd(down, up, &waiter, &quitFlag)
    go streamFwd(up, down, &waiter, &quitFlag)

    log.Printf("Wait streamFwd downStream <---> upStream (2)routines...")
    waiter.Wait()
    log.Printf("streamFwd go routines both quit...")
    return quitFlag
}
func creatNewFwdObject(fwd ForwardMap) *ForwardCtl {
	var fwdCtl ForwardCtl

	if fwd.Protocol == PROTOCOL_TCP {
		fwdCtl = ForwardCtl{singleFwd: fwd,
			fwdHandle: &L2DTCPFWDHandle{
				Event:    make(chan TcpListenEvent, EVENTCHAN_LEN),
				Interval: time.Duration(5),
				State:    TCP_LISTEN_STATUS_STOPED}}
   }else if fwd.Protocol == PROTOCOL_UDP {
            fwdCtl = ForwardCtl{
                singleFwd: fwd,
                fwdHandle: &L2DUDPFWDHandle{
                    Event:    make(chan UdpListenEvent, EVENTCHAN_LEN),
                    Interval: time.Duration(5),
                    State:    LISTEN_STATUS_STOPED}}
        }   
   return &fwdCtl
}
func (self *ForwardAllMap)forwardRun(){
    log.Printf("begin forwardRun...")
    for _,v := range self.fwdInfo {
        log.Printf("run %v",v)
        fwdCtl := creatNewFwdObject(v)
        fwdCtl.listen2DialProcess()
    }
}

func taskInit(conf string) {
	var allInfo ForwardAllMap
	allInfo.FwdMsg = make(map[string]*ForwardCtl)
	allInfo.fwdInfo = make(map[string]ForwardMap)
	allInfo.forwardMapInit(conf)
	allInfo.forwardRun()
}

func main() {
     
    log.SetPrefix("smartProxy: ")
    log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
    var confFile string
    var help string
    flag.StringVar(&confFile,"c","smartProxy.conf","must assign json profile")
    flag.StringVar(&help,"h","","must assign json profile")
    flag.Parse()
    taskInit(confFile)
    chs := make(chan int,1)
    <-chs
}
