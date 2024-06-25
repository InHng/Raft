package main

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// 日志条目结构
type LogEntry struct {
	Term    int         // 日志条目的任期
	Command interface{} // 客户端命令
}

// Raft节点状态结构
type Raft struct {
	mu               sync.Mutex    // 互斥锁，用于保护共享资源的访问
	peers            []string      // 所有节点的RPC端点
	me               int           // 当前节点的索引
	state            string        // 节点状态 (Follower, Candidate, Leader)
	currentTerm      int           // 当前任期
	votedFor         int           // 当前任期内投票给的候选人ID
	log              []LogEntry    // 日志条目
	commitIndex      int           // 已知的已提交的最大日志条目的索引
	lastApplied      int           // 最后被应用到状态机的日志条目的索引
	nextIndex        []int         // 对于每个服务器，发送到该服务器的下一个日志条目的索引
	matchIndex       []int         // 对于每个服务器，已知的该服务器的最大已复制日志条目的索引
	applyCh          chan ApplyMsg // 应用日志条目的通道
	electionTimeout  time.Duration // 选举超时时间
	heartbeatTimeout time.Duration // 心跳超时时间
	lastHeartbeat    time.Time     // 最后一次心跳时间
}

// RPC参数和相应结构
type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人最后日志条目的索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	Term        int  // 当前任期号
	VoteGranted bool // 是否投票给候选人
}

type AppendEntriesArgs struct {
	Term         int        // Leader的任期号
	LeaderId     int        // Leader的ID
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期号
	Entries      []LogEntry // 需要存储的日志条目
	LeaderCommit int        // Leader的已提交的日志的索引值
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号
	Success bool // 如果Follower包含了匹配上PrevLogIndex和PrevLogTerm的日志条目则为true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "Follower"
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		log.Printf("节点 %d 在任期 %d 中投票给节点 %d\n", rf.me, args.Term, args.CandidateId)
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	rf.currentTerm = args.Term
	rf.state = "Follower"
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	log.Printf("节点 %d 在任期 %d 中收到了来自节点 %d 的心跳\n", rf.me, args.Term, args.LeaderId)

	reply.Term = rf.currentTerm
	reply.Success = true
	return nil
}

func (rf *Raft) isUpToDate(lastLogIndex, lastLogTerm int) bool {
	if len(rf.log) == 0 {
		return true
	}
	lastTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm != lastTerm {
		return lastLogTerm > lastTerm
	}
	return lastLogIndex >= len(rf.log)-1
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client, err := rpc.Dial("tcp", rf.peers[server])
	if err != nil {
		log.Printf("Dialing error: %v", err)
		return false
	}
	defer client.Close()
	err = client.Call("Raft.RequestVote", args, reply)
	if err != nil {
		log.Printf("RPC error: %v", err)
		return false
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	client, err := rpc.Dial("tcp", rf.peers[server])
	if err != nil {
		log.Printf("Dialing error: %v", err)
		return false
	}
	defer client.Close()
	err = client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		log.Printf("RPC error: %v", err)
		return false
	}
	return true
}

func (rf *Raft) runElectionTimer() {
	for {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		if time.Since(rf.lastHeartbeat) >= rf.electionTimeout && rf.state != "Leader" {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = "Candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	log.Printf("节点 %d 开始任期 %d 的选举\n", rf.me, rf.currentTerm)

	var votes int32 = 1
	var wg sync.WaitGroup

	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				var reply RequestVoteReply
				if rf.sendRequestVote(i, &args, &reply) {
					if reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					} else if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = "Follower"
						rf.votedFor = -1
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}

	wg.Wait()
	if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
		rf.state = "Leader"
		log.Printf("节点 %d 成为任期 %d Leader\n", rf.me, rf.currentTerm)
		go rf.sendHeartbeats()
	} else {
		log.Printf("节点 %d 未能成为任期 %d 的 Leader\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) sendHeartbeats() {
	ticker := time.NewTicker(rf.heartbeatTimeout)
	defer ticker.Stop()
	for range ticker.C {
		if rf.state != "Leader" {
			return
		}
		for i := range rf.peers {
			if i != rf.me {
				go func(i int) {
					args := AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					var reply AppendEntriesReply
					if rf.sendAppendEntries(i, &args, &reply) {
						if !reply.Success && reply.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.state = "Follower"
							rf.votedFor = -1
							rf.mu.Unlock()
						}
					}
				}(i)
			}
		}
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(1000+rand.Intn(2000)) * time.Millisecond
}

func main() {
	rand.Seed(time.Now().UnixNano())
	applyCh := make(chan ApplyMsg)
	peers := []string{"localhost:12345", "localhost:12346", "localhost:12347"}

	rafts := make([]*Raft, len(peers))
	for i := range rafts {
		rafts[i] = &Raft{
			peers:            peers,
			me:               i,
			state:            "Follower",
			votedFor:         -1,
			log:              []LogEntry{{Term: 0}},
			electionTimeout:  randomElectionTimeout(),
			heartbeatTimeout: 1 * time.Second,
			applyCh:          applyCh,
		}
		rpc.Register(rafts[i])
		l, err := net.Listen("tcp", peers[i])
		if err != nil {
			log.Fatalf("Listen error: %v", err)
		}
		go rpc.Accept(l)
		go rafts[i].runElectionTimer()
	}

	select {} // Run indefinitely
}
