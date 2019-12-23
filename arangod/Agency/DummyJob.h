////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2018 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Lars Maier
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_CONSENSUS_DUMMY_JOB_H
#define ARANGOD_CONSENSUS_DUMMY_JOB_H 1

#include "Job.h"
#include "Supervision.h"

namespace arangodb::consensus {
struct DummyJob : public Job {
  DummyJob(Node const& snapshot, AgentInterface* agent, std::string const& jobId);
  DummyJob(Node const& snapshot, AgentInterface* agent, JOB_STATUS status,
           std::string const& jobId);

  virtual ~DummyJob();

  JOB_STATUS status() final;
  bool create(std::shared_ptr<VPackBuilder> envelope = nullptr) final;
  void run(bool&) final;
  bool start(bool&) final;
  Result abort(std::string const& reason) final;

 public:
  DummyJob& withPendingAfter(Supervision::Duration);
  DummyJob& withFailureAfter(Supervision::Duration);
  DummyJob& withFinishAfter(Supervision::Duration);

  DummyJob& withLockShard(std::string shard);
  DummyJob& withLockDBServer(std::string dbserver);

 private:
  void toVelocyPack(VPackBuilder& builder) const;
  void loadFromSlice(VPackSlice slice);

  std::string const& getDBServer() const;
  std::string const& getShardName() const;

  Supervision::TimePoint _timeCreated;
  std::optional<Supervision::Duration> _pendingAfter;
  std::optional<Supervision::Duration> _failureAfter;
  std::optional<Supervision::Duration> _finishAfter;
  std::optional<std::string> _shard;
  std::optional<std::string> _dbserver;
};
}  // namespace arangodb::consensus

#endif
