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

#include "Agency/DummyJob.h"
#include <Basics/StaticStrings.h>

#include "Agency/Job.h"
#include "Agency/JobContext.h"

using namespace arangodb::consensus;

DummyJob::DummyJob(Node const& snapshot, AgentInterface* agent, std::string const& jobId)
    : Job(NOTFOUND, snapshot, agent, jobId) {}

DummyJob::DummyJob(Node const& snapshot, AgentInterface* agent,
                   JOB_STATUS status, std::string const& jobId)
    : Job(status, snapshot, agent, jobId) {
  // Get job details from agency:
  std::string path = pos[status] + _jobId + "/";
  auto job = snapshot.hasAsSlice(path);
  if (job.second) {
    loadFromSlice(job.first);
  } else {
    std::stringstream err;
    err << "Failed to find job " << _jobId << " in agency.";
    LOG_TOPIC("defd5", ERR, Logger::SUPERVISION) << err.str();
    finish("", "", false, err.str());
    _status = FAILED;
  }
}

DummyJob::~DummyJob() = default;

void DummyJob::run(bool& aborts) { runHelper("", "", aborts); }

DummyJob& DummyJob::withPendingAfter(Supervision::Duration delta) {
  _pendingAfter = delta;
  return *this;
}

DummyJob& DummyJob::withFailureAfter(Supervision::Duration delta) {
  _failureAfter = delta;
  return *this;
}

DummyJob& DummyJob::withFinishAfter(Supervision::Duration delta) {
  _finishAfter = delta;
  return *this;
}

DummyJob& DummyJob::withLockShard(std::string shard) {
  _shard = std::move(shard);
  return *this;
}

DummyJob& DummyJob::withLockDBServer(std::string dbserver) {
  _dbserver = std::move(dbserver);
  return *this;
}

bool DummyJob::create(std::shared_ptr<VPackBuilder> envelope) {
  bool selfCreate = (envelope == nullptr);  // Do we create ourselves?

  if (selfCreate) {
    _jb = std::make_shared<Builder>();
  } else {
    _jb = envelope;
  }

  std::string path = toDoPrefix + _jobId;

  {
    VPackArrayBuilder guard(_jb.get());
    VPackObjectBuilder guard2(_jb.get());
    _jb->add(VPackValue(path));
    { toVelocyPack(*_jb); }
  }

  _status = TODO;
  if (!selfCreate) {
    return true;
  }

  write_ret_t res = singleWriteTransaction(_agent, *_jb, false);
  if (res.accepted && res.indices.size() == 1 && res.indices[0]) {
    return true;
  }

  _status = NOTFOUND;
  return false;
}

bool DummyJob::start(bool&) {
  auto now = Supervision::clock::now();

  if (_failureAfter) {
    if (now > (_timeCreated + *_failureAfter)) {
      finish(getDBServer(), getShardName(), false, "job failed pending (timeout)");
      return false;
    }
  }




  if (_finishAfter) {
    if (now > (_timeCreated + *_finishAfter)) {
      finish(getDBServer(), getShardName(), true, "job finished (timeout)");
      return false;
    }
  }

  if (_pendingAfter) {
    if (now < (_timeCreated + *_pendingAfter)) {
      // to early for start
      return false;
    }
  }

  VPackBuilder job;
  toVelocyPack(job);
  VPackBuilder trxBuilder;
  {
    VPackArrayBuilder env(&trxBuilder);
    {
      VPackArrayBuilder trx(&trxBuilder);
      {
        VPackObjectBuilder ops(&trxBuilder);
        addPutJobIntoSomewhere(trxBuilder, "Pending", job.slice());
        addRemoveJobFromSomewhere(trxBuilder, "ToDo", _jobId);

        if (_dbserver) {
          addBlockServer(trxBuilder, *_dbserver, _jobId);
        }

        if (_shard) {
          addBlockShard(trxBuilder, *_shard, _jobId);
        }
      }
      {
        VPackObjectBuilder precs(&trxBuilder);
        if (_dbserver) {
          addPreconditionServerNotBlocked(trxBuilder, *_dbserver);
          addPreconditionServerHealth(trxBuilder, *_dbserver, "GOOD");
        }
        if (_shard) {
          addPreconditionShardNotBlocked(trxBuilder, *_shard);
        }
      }
    }
  }

  // Transact to agency
  write_ret_t res = singleWriteTransaction(_agent, trxBuilder, false);

  if (res.accepted && res.indices.size() == 1 && res.indices[0]) {
    LOG_TOPIC("defdd", DEBUG, Logger::SUPERVISION) << "Pending: Dummy job";

    return true;
  }

  LOG_TOPIC("defde", INFO, Logger::SUPERVISION)
  << "Precondition failed for starting Dummy job " + _jobId;

  return false;
}

std::string const& DummyJob::getDBServer() const {
  if (_dbserver) {
    return *_dbserver;
  }

  return StaticStrings::Empty;
}

std::string const& DummyJob::getShardName() const {
  if (_shard) {
    return *_shard;
  }

  return StaticStrings::Empty;
}

arangodb::Result DummyJob::abort(std::string const& reason) {
  finish(getDBServer(), getShardName(), false, "job aborted: " + reason);
  return {};
}

JOB_STATUS DummyJob::status() {
  if (_status != PENDING) {
    return _status;
  }

  auto now = Supervision::clock::now();

  if (_failureAfter) {
    if (now > (_timeCreated + *_failureAfter)) {
      finish(getDBServer(), getShardName(), false, "job intentionally failed");
      return FAILED;
    }
  }

  if (_finishAfter) {
    if (now < (_timeCreated + *_finishAfter)) {
      return PENDING;
    }
  }

  finish(getDBServer(), getShardName(), true, "job intentionally finished");
  return FINISHED;
}

namespace {
template <typename T>
void addOptional(VPackBuilder& b, std::string_view attr, std::optional<T> const& opt) {
  if (opt) {
    b.add(VPackStringRef(attr.data(), attr.length()), VPackValue(*opt));
  }
}

void addOptional(VPackBuilder& b, std::string_view attr,
                 std::optional<Supervision::Duration> const& opt) {
  if (opt) {
    b.add(VPackStringRef(attr.data(), attr.length()), VPackValue(opt->count()));
  }
}

auto getOptionalString(VPackSlice slice, std::string_view attr)
    -> std::optional<std::string> {
  Slice value = slice.get(VPackStringRef(attr.data(), attr.length()));
  if (value.isString()) {
    return value.copyString();
  }
  return {};
}

auto getOptionalDuration(VPackSlice slice, std::string_view attr)
    -> std::optional<Supervision::Duration> {
  Slice value = slice.get(VPackStringRef(attr.data(), attr.length()));
  if (value.isNumber<int64_t>()) {
    return Supervision::Duration(value.getNumber<int64_t>());
  }
  return {};
}

}  // namespace

void DummyJob::toVelocyPack(VPackBuilder& builder) const {
  {
    VPackObjectBuilder ob(&builder);
    builder.add("type", VPackValue("dummy"));
    builder.add("jobId", VPackValue(_jobId));

    builder.add("timeCreated", VPackValue(timepointToString(_timeCreated)));
    addOptional(builder, "pendingAfter", _pendingAfter);
    addOptional(builder, "failureAfter", _failureAfter);
    addOptional(builder, "finishAfter", _finishAfter);
    addOptional(builder, "shard", _shard);
    addOptional(builder, "dbserver", _dbserver);
  }
}

void DummyJob::loadFromSlice(VPackSlice slice) {
  _timeCreated = stringToTimepoint(slice.get("timeCreated").copyString());
  _pendingAfter = getOptionalDuration(slice, "pendingAfter");
  _failureAfter = getOptionalDuration(slice, "failureAfter");
  _finishAfter = getOptionalDuration(slice, "finishAfter");
  _shard = getOptionalString(slice, "shard");
  _dbserver = getOptionalString(slice, "dbserver");
}
