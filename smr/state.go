package smr

import (
	"sync"
)

type ServerState struct {
	sync.RWMutex
	myID     int
	leaderID int
	term     int
	votedFor bool
	logIndex int
	cmtIndex int
}

func NewServerState() ServerState {
	return ServerState{
		myID:     0,
		term:     0,
		votedFor: false,
		logIndex: 0,
		cmtIndex: 0,
	}
}

func (s *ServerState) SetMyServerID(id int) {
	s.Lock()
	defer s.Unlock()

	s.myID = id
}

func (s *ServerState) GetMyServerID() (myServerId int) {
	myServerId = s.myID
	return
}

func (s *ServerState) SetLeaderID(id int) {
	s.Lock()
	defer s.Unlock()

	s.leaderID = id
}

func (s *ServerState) GetLeaderID() (leaderId int) {
	leaderId = s.leaderID
	return
}

func (s *ServerState) SetTerm(term int) {
	s.Lock()
	defer s.Unlock()

	s.term = term
}

func (s *ServerState) GetTerm() (term int) {
	term = s.term
	return
}

func (s *ServerState) ResetVotedFor() (votedFor bool) {
	s.Lock()
	defer s.Unlock()

	s.votedFor = false
	votedFor = s.votedFor
	return
}

func (s *ServerState) CheckVotedFor() (votedFor bool) {
	votedFor = s.votedFor
	return
}

func (s *ServerState) SyncLogIndex(logIndex int) {
	s.Lock()
	defer s.Unlock()

	s.logIndex = logIndex
}

func (s *ServerState) GetLogIndex() (logIndex int) {
	logIndex = s.logIndex
	return
}

func (s *ServerState) AddLogIndex(n int) {
	s.Lock()
	defer s.Unlock()

	s.logIndex += n
}

func (s *ServerState) SyncCommitIndex(commitIndex int) {
	s.Lock()
	defer s.Unlock()

	s.cmtIndex = commitIndex
}

func (s *ServerState) GetCommitIndex() (commitIndex int) {
	commitIndex = s.cmtIndex
	return
}

func (s *ServerState) AddCommitIndex(n int) {
	s.Lock()
	defer s.Unlock()

	s.cmtIndex += n
}
