////////////////////////////////////////////////////////////////////////////////
/// @brief test suite for Cluster maintenance
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Kaveh Vahedipour
/// @author Matthew Von-Maszewski
/// @author Copyright 2017-2018, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "gtest/gtest.h"

#include "ApplicationFeatures/GreetingsFeaturePhase.h"
#include "Cluster/Maintenance.h"
#include "Mocks/Servers.h"
#include "RocksDBEngine/RocksDBEngine.h"
#include "StorageEngine/EngineSelectorFeature.h"

#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

#include <fstream>
#include <iostream>
#include <iterator>
#include <random>
#include <typeinfo>

#include "MaintenanceFeatureMock.h"

using namespace arangodb;
using namespace arangodb::consensus;
using namespace arangodb::maintenance;

#ifndef _WIN32

char const* planStr =
#include "Plan.json"
    ;
char const* currentStr =
#include "Current.json"
    ;
char const* supervisionStr =
#include "Supervision.json"
    ;
char const* dbs0Str =
#include "DBServer0001.json"
    ;
char const* dbs1Str =
#include "DBServer0002.json"
    ;
char const* dbs2Str =
#include "DBServer0003.json"
    ;

int loadResources(void) { return 0; }

#else  // _WIN32
#include <Windows.h>
#include "jsonresource.h"
LPSTR planStr = nullptr;
LPSTR currentStr = nullptr;
LPSTR supervisionStr = nullptr;
LPSTR dbs0Str = nullptr;
LPSTR dbs1Str = nullptr;
LPSTR dbs2Str = nullptr;

LPSTR getResource(int which) {
  HRSRC myResource = ::FindResource(NULL, MAKEINTRESOURCE(which), RT_RCDATA);
  HGLOBAL myResourceData = ::LoadResource(NULL, myResource);
  return (LPSTR)::LockResource(myResourceData);
}
int loadResources(void) {
  if ((planStr == nullptr) && (currentStr == nullptr) && (supervisionStr == nullptr) &&
      (dbs0Str == nullptr) && (dbs1Str == nullptr) && (dbs2Str == nullptr)) {
    planStr = getResource(IDS_PLAN);
    currentStr = getResource(IDS_CURRENT);
    dbs0Str = getResource(IDS_DBSERVER0001);
    dbs1Str = getResource(IDS_DBSERVER0002);
    dbs2Str = getResource(IDS_DBSERVER0003);
    supervisionStr = getResource(IDS_SUPERVISION);
  }
  return 0;
}

#endif  // _WIN32

std::map<std::string, std::string> matchShortLongIds(Node const& supervision) {
  std::map<std::string, std::string> ret;
  for (auto const& dbs : supervision("Health").children()) {
    if (dbs.first.front() == 'P') {
      ret.emplace((*dbs.second)("ShortName").getString(), dbs.first);
    }
  }
  return ret;
}

Node createNodeFromBuilder(Builder const& builder) {
  Builder opBuilder;
  {
    VPackObjectBuilder a(&opBuilder);
    opBuilder.add("new", builder.slice());
  }

  Node node("");
  node.handle<SET>(opBuilder.slice());
  return node;
}

Builder createBuilder(char const* c) {
  VPackOptions options;
  options.checkAttributeUniqueness = true;
  VPackParser parser(&options);
  parser.parse(c);

  Builder builder;
  builder.add(parser.steal()->slice());
  return builder;
}

Node createNode(char const* c) {
  return createNodeFromBuilder(createBuilder(c));
}

// Random stuff
std::random_device rd;
std::mt19937 g(rd());

// Relevant agency
Node plan("");
Node originalPlan("");
Node supervision("");
Node current("");

std::vector<std::string> const shortNames{"DBServer0001", "DBServer0002",
                                          "DBServer0003"};

// map <shortId, UUID>
std::map<std::string, std::string> dbsIds;

std::string const PLAN_COL_PATH = "/Collections/";
std::string const PLAN_DB_PATH = "/Databases/";

size_t localId = 1016002;

VPackBuilder createDatabase(std::string const& dbname) {
  Builder builder;
  {
    VPackObjectBuilder o(&builder);
    builder.add("id", VPackValue(std::to_string(localId++)));
    builder.add("coordinator",
                VPackValue("CRDN-42df19c3-73d5-48f4-b02e-09b29008eff8"));
    builder.add(VPackValue("options"));
    { VPackObjectBuilder oo(&builder); }
    builder.add("name", VPackValue(dbname));
  }
  return builder;
}

void createPlanDatabase(std::string const& dbname, Node& plan) {
  plan(PLAN_DB_PATH + dbname) = createDatabase(dbname).slice();
}

VPackBuilder createIndex(std::string const& type, std::vector<std::string> const& fields,
                         bool unique, bool sparse, bool deduplicate) {
  VPackBuilder index;
  {
    VPackObjectBuilder o(&index);
    {
      index.add("deduplicate", VPackValue(deduplicate));
      index.add(VPackValue("fields"));
      {
        VPackArrayBuilder a(&index);
        for (auto const& field : fields) {
          index.add(VPackValue(field));
        }
      }
      index.add("id", VPackValue(std::to_string(localId++)));
      index.add("sparse", VPackValue(sparse));
      index.add("type", VPackValue(type));
      index.add("unique", VPackValue(unique));
    }
  }

  return index;
}

void createPlanIndex(std::string const& dbname, std::string const& colname,
                     std::string const& type, std::vector<std::string> const& fields,
                     bool unique, bool sparse, bool deduplicate, Node& plan) {
  VPackBuilder val;
  {
    VPackObjectBuilder o(&val);
    val.add("new", createIndex(type, fields, unique, sparse, deduplicate).slice());
  }
  plan(PLAN_COL_PATH + dbname + "/" + colname + "/indexes").handle<PUSH>(val.slice());
}

void createCollection(std::string const& colname, VPackBuilder& col) {
  VPackBuilder keyOptions;
  {
    VPackObjectBuilder o(&keyOptions);
    keyOptions.add("lastValue", VPackValue(0));
    keyOptions.add("type", VPackValue("traditional"));
    keyOptions.add("allowUserKeys", VPackValue(true));
  }

  VPackBuilder shardKeys;
  {
    VPackArrayBuilder a(&shardKeys);
    shardKeys.add(VPackValue("_key"));
  }

  VPackBuilder indexes;
  {
    VPackArrayBuilder a(&indexes);
    indexes.add(createIndex("primary", {"_key"}, true, false, false).slice());
  }

  col.add("id", VPackValue(std::to_string(localId++)));
  col.add("status", VPackValue(3));
  col.add("keyOptions", keyOptions.slice());
  col.add("cacheEnabled", VPackValue(false));
  col.add("waitForSync", VPackValue(false));
  col.add("type", VPackValue(2));
  col.add("isSystem", VPackValue(true));
  col.add("name", VPackValue(colname));
  col.add("shardingStrategy", VPackValue("hash"));
  col.add("statusString", VPackValue("loaded"));
  col.add("shardKeys", shardKeys.slice());
}

std::string S("s");
std::string C("c");

void createPlanShards(size_t numberOfShards, size_t replicationFactor, VPackBuilder& col) {
  auto servers = shortNames;
  std::shuffle(servers.begin(), servers.end(), g);

  col.add("numberOfShards", VPackValue(1));
  col.add("replicationFactor", VPackValue(2));
  col.add(VPackValue("shards"));
  {
    VPackObjectBuilder s(&col);
    for (size_t i = 0; i < numberOfShards; ++i) {
      col.add(VPackValue(S + std::to_string(localId++)));
      {
        VPackArrayBuilder a(&col);
        size_t j = 0;
        for (auto const& server : servers) {
          if (j++ < replicationFactor) {
            col.add(VPackValue(dbsIds[server]));
          }
        }
      }
    }
  }
}

void createPlanCollection(std::string const& dbname, std::string const& colname,
                          size_t numberOfShards, size_t replicationFactor, Node& plan) {
  VPackBuilder tmp;
  {
    VPackObjectBuilder o(&tmp);
    createCollection(colname, tmp);
    tmp.add("isSmart", VPackValue(false));
    tmp.add("deleted", VPackValue(false));
    createPlanShards(numberOfShards, replicationFactor, tmp);
  }

  Slice col = tmp.slice();
  auto id = col.get("id").copyString();
  plan(PLAN_COL_PATH + dbname + "/" + col.get("id").copyString()) = col;
}

void createLocalCollection(std::string const& dbname, std::string const& colname, Node& node) {
  size_t planId = std::stoull(colname);
  VPackBuilder tmp;
  {
    VPackObjectBuilder o(&tmp);
    createCollection(colname, tmp);
    tmp.add("planId", VPackValue(colname));
    tmp.add("theLeader", VPackValue(""));
    tmp.add("globallyUniqueId",
            VPackValue(C + colname + "/" + S + std::to_string(planId + 1)));
    tmp.add("objectId", VPackValue("9031415"));
  }
  node(dbname + "/" + S + std::to_string(planId + 1)) = tmp.slice();
}

std::map<std::string, std::string> collectionMap(Node const& plan) {
  std::map<std::string, std::string> ret;
  auto const pb = plan("Collections").toBuilder();
  auto const ps = pb.slice();
  for (auto const& db : VPackObjectIterator(ps)) {
    for (auto const& col : VPackObjectIterator(db.value)) {
      ret.emplace(db.key.copyString() + "/" + col.value.get("name").copyString(),
                  col.key.copyString());
    }
  }
  return ret;
}

namespace arangodb {
class LogicalCollection;
}

class MaintenanceTestActionDescription : public ::testing::Test {
  // private:
  //   tests::mocks::MockDBServer _server;

 protected:
  MaintenanceTestActionDescription() /*: _server{}*/ {
    loadResources();
    plan = createNode(planStr);
    originalPlan = plan;
    supervision = createNode(supervisionStr);
    current = createNode(currentStr);
    dbsIds = matchShortLongIds(supervision);
  }
};

TEST_F(MaintenanceTestActionDescription, construct_minimal_actiondescription) {
  ActionDescription desc(std::map<std::string, std::string>{{"name",
                                                             "SomeAction"}},
                         NORMAL_PRIORITY);
  ASSERT_EQ(desc.get("name"), "SomeAction");
}

TEST_F(MaintenanceTestActionDescription, construct_minimal_actiondescription_with_nullptr_props) {
  std::shared_ptr<VPackBuilder> props;
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
}

TEST_F(MaintenanceTestActionDescription, construct_minimal_actiondescription_with_empty_props) {
  std::shared_ptr<VPackBuilder> props;
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
}

TEST_F(MaintenanceTestActionDescription, retrieve_nonassigned_key_from_actiondescription) {
  std::shared_ptr<VPackBuilder> props;
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  try {
    auto bogus = desc.get("bogus");
    ASSERT_EQ(bogus, "bogus");
  } catch (std::out_of_range const&) {
  }
  std::string value;
  auto res = desc.get("bogus", value);
  ASSERT_TRUE(value.empty());
  ASSERT_FALSE(res.ok());
}

TEST_F(MaintenanceTestActionDescription, retrieve_nonassigned_key_from_actiondescription_2) {
  std::shared_ptr<VPackBuilder> props;
  ActionDescription desc({{"name", "SomeAction"}, {"bogus", "bogus"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  try {
    auto bogus = desc.get("bogus");
    ASSERT_EQ(bogus, "bogus");
  } catch (std::out_of_range const&) {
  }
  std::string value;
  auto res = desc.get("bogus", value);
  ASSERT_EQ(value, "bogus");
  ASSERT_TRUE(res.ok());
}

TEST_F(MaintenanceTestActionDescription, retrieve_nonassigned_properties_from_actiondescription) {
  std::shared_ptr<VPackBuilder> props;
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_EQ(desc.properties(), nullptr);
}

TEST_F(MaintenanceTestActionDescription, retrieve_empty_properties_from_actiondescription) {
  auto props = std::make_shared<VPackBuilder>();
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_TRUE(desc.properties()->isEmpty());
}

TEST_F(MaintenanceTestActionDescription, retrieve_empty_object_properties_from_actiondescription) {
  auto props = std::make_shared<VPackBuilder>();
  { VPackObjectBuilder empty(props.get()); }
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_TRUE(desc.properties()->slice().isEmptyObject());
}

TEST_F(MaintenanceTestActionDescription, retrieve_string_value_from_actiondescriptions_properties) {
  auto props = std::make_shared<VPackBuilder>();
  {
    VPackObjectBuilder obj(props.get());
    props->add("hello", VPackValue("world"));
  }
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_TRUE(desc.properties()->slice().hasKey("hello"));
  ASSERT_EQ(desc.properties()->slice().get("hello").copyString(), "world");
}

TEST_F(MaintenanceTestActionDescription, retrieve_double_value_from_actiondescriptions_properties) {
  double pi = 3.14159265359;
  auto props = std::make_shared<VPackBuilder>();
  {
    VPackObjectBuilder obj(props.get());
    props->add("pi", VPackValue(3.14159265359));
  }
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_TRUE(desc.properties()->slice().hasKey("pi"));
  ASSERT_EQ(desc.properties()->slice().get("pi").getNumber<double>(), pi);
}

TEST_F(MaintenanceTestActionDescription, retrieve_integer_value_from_actiondescriptions_property) {
  size_t one = 1;
  auto props = std::make_shared<VPackBuilder>();
  {
    VPackObjectBuilder obj(props.get());
    props->add("one", VPackValue(one));
  }
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_TRUE(desc.properties()->slice().hasKey("one"));
  ASSERT_EQ(desc.properties()->slice().get("one").getNumber<size_t>(), one);
}

TEST_F(MaintenanceTestActionDescription, retrieve_array_value_from_actiondescriptions_properties) {
  double pi = 3.14159265359;
  size_t one = 1;
  std::string hello("hello world!");
  auto props = std::make_shared<VPackBuilder>();
  {
    VPackObjectBuilder obj(props.get());
    props->add(VPackValue("array"));
    {
      VPackArrayBuilder arr(props.get());
      props->add(VPackValue(pi));
      props->add(VPackValue(one));
      props->add(VPackValue(hello));
    }
  }
  ActionDescription desc({{"name", "SomeAction"}}, NORMAL_PRIORITY, props);
  ASSERT_EQ(desc.get("name"), "SomeAction");
  ASSERT_TRUE(desc.properties()->slice().hasKey("array"));
  ASSERT_TRUE(desc.properties()->slice().get("array").isArray());
  ASSERT_EQ(desc.properties()->slice().get("array").length(), 3);
  ASSERT_EQ(desc.properties()->slice().get("array")[0].getNumber<double>(), pi);
  ASSERT_EQ(desc.properties()->slice().get("array")[1].getNumber<size_t>(), one);
  ASSERT_EQ(desc.properties()->slice().get("array")[2].copyString(), hello);
}

class MaintenanceTestActionPhaseOne : public ::testing::Test {
 protected:
  int _dummy;
  std::shared_ptr<arangodb::options::ProgramOptions> po;
  arangodb::application_features::ApplicationServer as;
  std::unique_ptr<TestMaintenanceFeature> feature;
  MaintenanceFeature::errors_t errors;

  std::map<std::string, Node> localNodes;

  arangodb::RocksDBEngine engine;  // arbitrary implementation that has index types registered
  arangodb::StorageEngine* origStorageEngine;

  MaintenanceTestActionPhaseOne()
      : _dummy(loadResources()),
        po(std::make_shared<arangodb::options::ProgramOptions>("test", std::string(),
                                                               std::string(),
                                                               "path")),
        as(po, nullptr),
        localNodes{{dbsIds[shortNames[0]], createNode(dbs0Str)},
                   {dbsIds[shortNames[1]], createNode(dbs1Str)},
                   {dbsIds[shortNames[2]], createNode(dbs2Str)}},
        engine(as),
        origStorageEngine(arangodb::EngineSelectorFeature::ENGINE) {
    as.addFeature<arangodb::MetricsFeature>();
    as.addFeature<arangodb::application_features::GreetingsFeaturePhase>(false);
    feature = std::make_unique<TestMaintenanceFeature>(as);

    arangodb::EngineSelectorFeature::ENGINE = &engine;
  }

  ~MaintenanceTestActionPhaseOne() {
    arangodb::EngineSelectorFeature::ENGINE = origStorageEngine;
  }
};

TEST_F(MaintenanceTestActionPhaseOne, in_sync_should_have_0_effects) {
  std::vector<ActionDescription> actions;

  for (auto const& node : localNodes) {
    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (actions.size() != 0) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), 0);
  }
}

TEST_F(MaintenanceTestActionPhaseOne, local_databases_one_more_empty_database_should_be_dropped) {
  std::vector<ActionDescription> actions;

  localNodes.begin()->second("db3") = arangodb::velocypack::Slice::emptyObjectSlice();

  arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                       localNodes.begin()->second.toBuilder().slice(),
                                       localNodes.begin()->first, errors, *feature, actions);

  if (actions.size() != 1) {
    std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
  }
  ASSERT_EQ(actions.size(), 1);
  ASSERT_EQ(actions.front().name(), "DropDatabase");
  ASSERT_EQ(actions.front().get("database"), "db3");
}

TEST_F(MaintenanceTestActionPhaseOne,
       local_databases_one_more_non_empty_database_should_be_dropped) {
  std::vector<ActionDescription> actions;
  localNodes.begin()->second("db3/col") = arangodb::velocypack::Slice::emptyObjectSlice();

  arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                       localNodes.begin()->second.toBuilder().slice(),
                                       localNodes.begin()->first, errors, *feature, actions);

  ASSERT_EQ(actions.size(), 1);
  ASSERT_EQ(actions.front().name(), "DropDatabase");
  ASSERT_EQ(actions.front().get("database"), "db3");
}

TEST_F(MaintenanceTestActionPhaseOne,
       add_one_collection_to_db3_in_plan_with_shards_for_all_db_servers) {
  std::string dbname("db3"), colname("x");

  plan = originalPlan;
  createPlanDatabase(dbname, plan);
  createPlanCollection(dbname, colname, 1, 3, plan);

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;

    node.second("db3") = arangodb::velocypack::Slice::emptyObjectSlice();

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (actions.size() != 1) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), 1);
    for (auto const& action : actions) {
      ASSERT_EQ(action.name(), "CreateCollection");
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne,
       add_two_more_collections_to_db3_in_plan_with_shards_for_all_db_servers) {
  std::string dbname("db3"), colname1("x"), colname2("y");

  plan = originalPlan;
  createPlanDatabase(dbname, plan);
  createPlanCollection(dbname, colname1, 1, 3, plan);
  createPlanCollection(dbname, colname2, 1, 3, plan);

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;

    node.second("db3") = arangodb::velocypack::Slice::emptyObjectSlice();

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (actions.size() != 2) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), 2);
    for (auto const& action : actions) {
      ASSERT_EQ(action.name(), "CreateCollection");
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne, add_an_index_to_queues) {
  plan = originalPlan;
  auto cid = collectionMap(plan).at("_system/_queues");
  auto shards = plan({"Collections", "_system", cid, "shards"}).children();

  createPlanIndex("_system", cid, "hash", {"someField"}, false, false, false, plan);

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;

    auto local = node.second;

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         local.toBuilder().slice(), node.first,
                                         errors, *feature, actions);

    size_t n = 0;
    for (auto const& shard : shards) {
      if (local.has({"_system", shard.first})) {
        ++n;
      }
    }

    if (actions.size() != n) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), n);
    for (auto const& action : actions) {
      ASSERT_EQ(action.name(), "EnsureIndex");
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne, remove_an_index_from_plan) {
  std::string dbname("_system");
  std::string indexes("indexes");

  plan = originalPlan;
  auto cid = collectionMap(plan).at("_system/bar");
  auto shards = plan({"Collections", dbname, cid, "shards"}).children();

  plan({"Collections", dbname, cid, indexes}).handle<POP>(arangodb::velocypack::Slice::emptyObjectSlice());

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;

    auto local = node.second;

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         local.toBuilder().slice(), node.first,
                                         errors, *feature, actions);

    size_t n = 0;
    for (auto const& shard : shards) {
      if (local.has({"_system", shard.first})) {
        ++n;
      }
    }

    if (actions.size() != n) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), n);
    for (auto const& action : actions) {
      ASSERT_EQ(action.name(), "DropIndex");
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne, add_one_collection_to_local) {
  plan = originalPlan;

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;
    createLocalCollection("_system", "1111111", node.second);

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (actions.size() != 1) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), 1);
    for (auto const& action : actions) {
      ASSERT_EQ(action.name(), "DropCollection");
      ASSERT_EQ(action.get("database"), "_system");
      ASSERT_EQ(action.get("collection"), "s1111112");
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne,
       modify_journalsize_in_plan_should_update_the_according_collection) {
  VPackBuilder v;
  v.add(VPackValue(0));

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;
    std::string dbname = "_system";
    std::string prop = arangodb::maintenance::JOURNAL_SIZE;

    auto cb = node.second(dbname).children().begin()->second->toBuilder();
    auto collection = cb.slice();
    auto shname = collection.get(NAME).copyString();

    (*node.second(dbname).children().begin()->second)(prop) = v.slice();

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    /*
    if (actions.size() != 1) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << actions  << std::endl;
    }
    ASSERT_EQ(actions.size(), 1);
    for (auto const& action : actions) {

      ASSERT_EQ(action.name(), "UpdateCollection");
      ASSERT_EQ(action.get("shard"), shname);
      ASSERT_EQ(action.get("database"), dbname);
      auto const props = action.properties();

    }
    */
  }
}

TEST_F(MaintenanceTestActionPhaseOne, have_theleader_set_to_empty) {
  VPackBuilder v;
  v.add(VPackValue(std::string()));

  for (auto node : localNodes) {
    std::vector<ActionDescription> actions;

    auto& collection = *node.second("foo").children().begin()->second;
    auto& leader = collection("theLeader");

    bool check = false;
    if (!leader.getString().empty()) {
      check = true;
      leader = v.slice();
    }

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (check) {
      if (actions.size() != 1) {
        std::cout << __FILE__ << ":" << __LINE__ << " " << actions << std::endl;
      }
      ASSERT_EQ(actions.size(), 1);
      for (auto const& action : actions) {
        ASSERT_EQ(action.name(), "TakeoverShardLeadership");
        ASSERT_TRUE(action.has("shard"));
        ASSERT_EQ(action.get("shard"), collection("name").getString());
        ASSERT_TRUE(action.get("localLeader").empty());
      }
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne,
       empty_db3_in_plan_should_drop_all_local_db3_collections_on_all_servers) {
  plan(PLAN_COL_PATH + "db3") = arangodb::velocypack::Slice::emptyObjectSlice();

  createPlanDatabase("db3", plan);

  for (auto& node : localNodes) {
    std::vector<ActionDescription> actions;
    node.second("db3") = node.second("_system");

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    ASSERT_EQ(actions.size(), node.second("db3").children().size());
    for (auto const& action : actions) {
      ASSERT_EQ(action.name(), "DropCollection");
    }
  }
}

TEST_F(MaintenanceTestActionPhaseOne, resign_leadership) {
  plan = originalPlan;
  std::string const dbname("_system");
  std::string const colname("bar");
  auto cid = collectionMap(plan).at(dbname + "/" + colname);
  auto& shards = plan({"Collections", dbname, cid, "shards"}).children();

  for (auto const& node : localNodes) {
    std::vector<ActionDescription> actions;

    std::string shname;

    for (auto const& shard : shards) {
      shname = shard.first;
      auto shardBuilder = shard.second->toBuilder();
      Slice servers = shardBuilder.slice();

      ASSERT_TRUE(servers.isArray());
      ASSERT_EQ(servers.length(), 2);
      auto const leader = servers[0].copyString();
      auto const follower = servers[1].copyString();

      if (leader == node.first) {
        VPackBuilder newServers;
        {
          VPackArrayBuilder a(&newServers);
          newServers.add(VPackValue(std::string("_") + leader));
          newServers.add(VPackValue(follower));
        }
        plan({"Collections", dbname, cid, "shards", shname}) = newServers.slice();
        break;
      }
    }

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (actions.size() != 1) {
      std::cout << actions << std::endl;
    }
    ASSERT_EQ(actions.size(), 1);
    ASSERT_EQ(actions[0].name(), "ResignShardLeadership");
    ASSERT_EQ(actions[0].get(DATABASE), dbname);
    ASSERT_EQ(actions[0].get(SHARD), shname);
  }
}

TEST_F(MaintenanceTestActionPhaseOne, removed_follower_in_plan_must_be_dropped) {
  plan = originalPlan;
  std::string const dbname("_system");
  std::string const colname("bar");
  auto cid = collectionMap(plan).at(dbname + "/" + colname);
  Node::Children& shards = plan({"Collections", dbname, cid, "shards"}).children();
  auto firstShard = shards.begin();
  VPackBuilder b = firstShard->second->toBuilder();
  std::string const shname = firstShard->first;
  std::string const leaderName = b.slice()[0].copyString();
  std::string const followerName = b.slice()[1].copyString();
  firstShard->second->handle<POP>(arangodb::velocypack::Slice::emptyObjectSlice());

  for (auto const& node : localNodes) {
    std::vector<ActionDescription> actions;

    arangodb::maintenance::diffPlanLocal(plan.toBuilder().slice(), 0,
                                         node.second.toBuilder().slice(),
                                         node.first, errors, *feature, actions);

    if (node.first == followerName) {
      // Must see an action dropping the shard
      ASSERT_EQ(actions.size(), 1);
      ASSERT_EQ(actions.front().name(), "DropCollection");
      ASSERT_EQ(actions.front().get(DATABASE), dbname);
      ASSERT_EQ(actions.front().get(COLLECTION), shname);
    } else if (node.first == leaderName) {
      // Must see an UpdateCollection action to drop the follower
      ASSERT_EQ(actions.size(), 1);
      ASSERT_EQ(actions.front().name(), "UpdateCollection");
      ASSERT_EQ(actions.front().get(DATABASE), dbname);
      ASSERT_EQ(actions.front().get(SHARD), shname);
      ASSERT_EQ(actions.front().get(FOLLOWERS_TO_DROP), followerName);
    } else {
      // No actions required
      ASSERT_EQ(actions.size(), 0);
    }
  }
}
