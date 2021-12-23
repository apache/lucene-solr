/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cluster.api;



import org.apache.solr.common.cloud.Replica;

import java.util.List;
import java.util.function.Predicate;

/**
 * Provide information to route a call to an appropriate node/core
 * One and only one method should be invoked on this
 */
public interface CallRouter  {


    /** if a request is made to replicas of a collection/shard
     * use this to filter out some replicas
     */
    CallRouter withReplicaFilter(Predicate<ShardReplica> predicate);
    /**
     * Any node in the cluster
     */
    CallRouter anyNode();
    /**
     * send to a specific node. usually admin requests
     */
    CallRouter toNode(String nodeName);

    /**
     * Make a request to any replica of the shard of type
     */
    CallRouter toShard(String collection, String shard, ReplicaType type);

    /**
     * Identify the shard using the route key and send the request to a given replica type
     */
    CallRouter toShard(String collection, ReplicaType type, String routeKey);

    /**
     * Make a request to a specific replica
     */
    CallRouter toReplica(String collection, String replicaName);

    /**
     * To any Solr node  that may host this collection
     */
    CallRouter toCollection(String collection);

    /**
     * Make a call directly to a specific core in a node
     */
    CallRouter toCore(String node, String core);

    /**
     * Make a call to one replica of each shard.
     * Can be used for distributed requests
     */
    List<CallRouter> toEachShard(String collection, ReplicaType type);

    /**Make a call to every replica of a collection
     */
    List<CallRouter> toEachReplica(String collection);



    HttpRemoteCall createHttpRpc();

    enum ReplicaType {
        LEADER{
            @Override
            public boolean test(ShardReplica r) {
                return r.isLeader();
            }
        }, NRT{
            @Override
            public boolean test(ShardReplica r) {
                return r.type() == Replica.Type.NRT;
            }
        }, TLOG {
            @Override
            public boolean test(ShardReplica r) {
                return r.type() == Replica.Type.TLOG ;
            }
        }
        , PULL {
            @Override
            public boolean test(ShardReplica r) {
                return r.type() == Replica.Type.PULL;
            }
        }, NON_LEADER {
            @Override
            public boolean test(ShardReplica r) {
                return !r.isLeader();
            }
        }, ANY {
            @Override
            public boolean test(ShardReplica r) {
                return true;
            }
        };

        public boolean test(ShardReplica r) {
            return false;
        }
    }
}
