// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	//1. the Scheduler will select all suitable stores. Then sort them according to their region size.
	//Then the Scheduler tries to find regions to move from the store with the biggest region size.
	//In short, a suitable store should be up and the down time cannot be longer than MaxStoreDownTime of the cluster, which you can get through cluster.GetMaxStoreDownTime().
	suitable := make([]*core.StoreInfo, 0)
	stores := cluster.GetStores()
	for _, st := range stores {
		if st.IsUp() && st.DownTime() < cluster.GetMaxStoreDownTime() {
			suitable = append(suitable, st)
		}
	}

	//2. The scheduler will try to find the region most suitable for moving in the store.
	//First, it will try to select a pending region because pending may mean the disk is overloaded.
	//If there isn’t a pending region, it will try to find a follower region. If it still cannot pick out one region, it will try to pick leader regions.
	//Finally, it will select out the region to move, or the Scheduler will try the next store which has a smaller region size until all stores will have been tried.
	//手动写一个快排吧根据region size..

	//3. After you pick up one region to move, the Scheduler will select a store as the target.
	//Actually, the Scheduler will select the store with the smallest region size. Then the
	//Scheduler will judge whether this movement is valuable, by checking the difference between region sizes of the original store and the target store.
	//If the difference is big enough, the Scheduler should allocate a new peer on the target store and create a move peer operator.

	return operator.CreateMovePeerOperator()
}
