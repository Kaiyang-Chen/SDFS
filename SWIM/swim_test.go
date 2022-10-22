package SWIM

import (
	"sync"
	"testing"
)

func TestSwimShowPeer(t *testing.T) {
	swim := SWIM{
		MembershipList: map[string]Member{
			"127.0.0.1:8880": {WORKING},
			"127.0.0.1:8881": {MAYBEDEAD},
			"127.0.0.1:8882": {MAYBEDEAD},
			"127.0.0.1:8883": {WORKING},
			"127.0.0.1:8884": {DEAD},
		},
		PiggybackBuffer:  nil,
		MyIncarnationNum: 0,
		Mu:               sync.RWMutex{},
	}
	swim.SwimShowPeer()
}
