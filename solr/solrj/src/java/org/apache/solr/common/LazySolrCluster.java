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

package org.apache.solr.common;

import org.apache.solr.cluster.api.*;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.WrappedSimpleMap;
import org.apache.zookeeper.KeeperException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;
import static org.apache.solr.common.cloud.ZkStateReader.getCollectionPathRoot;

/**
 * Reference implementation for SolrCluster.
 * As much as possible fetch all the values lazily because the value of anything
 * can change any moment
 * Creating an instance is a low cost operation. It does not result in a
 * network call or large object creation
 *
 */

public class LazySolrCluster implements SolrCluster {
    final ZkStateReader zkStateReader;

    private final Map<String, SolrCollectionImpl> cached = new ConcurrentHashMap<>();
    private final SimpleMap<SolrCollection> collections;
    private final SimpleMap<SolrCollection> collectionsAndAliases;
    private final SimpleMap<SolrNode> nodes;
    private SimpleMap<CollectionConfig> configs;

    public LazySolrCluster(ZkStateReader zkStateReader) {
        this.zkStateReader = zkStateReader;
        collections = lazyCollectionsMap(zkStateReader);
        collectionsAndAliases = lazyCollectionsWithAlias(zkStateReader);
        nodes = lazyNodeMap();
    }

    private SimpleMap<CollectionConfig> lazyConfigMap() {
        Set<String> configNames = new HashSet<>();
        new SimpleZkMap(zkStateReader, ZkStateReader.CONFIGS_ZKNODE)
                .abortableForEach((name, resource) -> {
                    if (!name.contains("/")) {
                        configNames.add(name);
                        return Boolean.TRUE;
                    }
                    return Boolean.FALSE;
                });

        return new SimpleMap<CollectionConfig>() {
            @Override
            public CollectionConfig get(String key) {
                if (configNames.contains(key)) {
                    return new ConfigImpl(key);
                } else {
                    return null;
                }
            }

            @Override
            public void forEachEntry(BiConsumer<String, ? super CollectionConfig> fun) {
                for (String name : configNames) {
                    fun.accept(name, new ConfigImpl(name));
                }
            }

            @Override
            public int size() {
                return configNames.size();
            }
        };
    }

    private SimpleMap<SolrNode> lazyNodeMap() {
        return new SimpleMap<SolrNode>() {
            @Override
            public SolrNode get(String key) {
                if (!zkStateReader.getClusterState().liveNodesContain(key)) {
                    return null;
                }
                return new Node(key);
            }

            @Override
            public void forEachEntry(BiConsumer<String, ? super SolrNode> fun) {
                for (String s : zkStateReader.getClusterState().getLiveNodes()) {
                    fun.accept(s, new Node(s));
                }
            }

            @Override
            public int size() {
                return zkStateReader.getClusterState().getLiveNodes().size();
            }
        };
    }

    private SimpleMap<SolrCollection> lazyCollectionsWithAlias(ZkStateReader zkStateReader) {
        return new SimpleMap<SolrCollection>() {
            @Override
            public SolrCollection get(String key) {
                SolrCollection result = collections.get(key);
                if (result != null) return result;
                Aliases aliases = zkStateReader.getAliases();
                List<String> aliasNames = aliases.resolveAliases(key);
                if (aliasNames == null || aliasNames.isEmpty()) return null;
                return _collection(aliasNames.get(0), null);
            }

            @Override
            public void forEachEntry(BiConsumer<String, ? super SolrCollection> fun) {
                collections.forEachEntry(fun);
                Aliases aliases = zkStateReader.getAliases();
                aliases.forEachAlias((s, colls) -> {
                    if (colls == null || colls.isEmpty()) return;
                    fun.accept(s, _collection(colls.get(0), null));
                });

            }

            @Override
            public int size() {
                return collections.size() + zkStateReader.getAliases().size();
            }
        };
    }

    private SimpleMap<SolrCollection> lazyCollectionsMap(ZkStateReader zkStateReader) {
        return new SimpleMap<SolrCollection>() {
            @Override
            public SolrCollection get(String key) {
                return _collection(key, null);
            }

            @Override
            public void forEachEntry(BiConsumer<String, ? super SolrCollection> fun) {
                zkStateReader.getClusterState().forEachCollection(coll -> fun.accept(coll.getName(), _collection(coll.getName(), coll)));
            }

            @Override
            public int size() {
                return zkStateReader.getClusterState().size();
            }
        };
    }

    private SolrCollection _collection(String key, DocCollection c) {
        if (c == null) c = zkStateReader.getCollection(key);
        if (c == null) {
            cached.remove(key);
            return null;
        }
        SolrCollectionImpl existing = cached.get(key);
        if (existing == null || existing.coll != c) {
            cached.put(key, existing = new SolrCollectionImpl(c, zkStateReader));
        }
        return existing;
    }

    @Override
    public SimpleMap<SolrCollection> collections() throws SolrException {
        return collections;
    }

    @Override
    public SimpleMap<SolrCollection> collections(boolean includeAlias) throws SolrException {
        return includeAlias ? collectionsAndAliases : collections;
    }

    @Override
    public SimpleMap<SolrNode> nodes() throws SolrException {
        return nodes;
    }

    @Override
    public SimpleMap<CollectionConfig> configs() throws SolrException {
        if (configs == null) {
            //these are lightweight objects and we don't care even if multiple objects ar ecreated b/c of a race condition
            configs = lazyConfigMap();
        }
        return configs;
    }

    @Override
    public String overseerNode() throws SolrException {
        return null;
    }

    @Override
    public String thisNode() {
        return null;
    }

    private class SolrCollectionImpl implements SolrCollection {
        final DocCollection coll;
        final SimpleMap<Shard> shards;
        final ZkStateReader zkStateReader;
        final Router router;
        String confName;

        private SolrCollectionImpl(DocCollection coll, ZkStateReader zkStateReader) {
            this.coll = coll;
            this.zkStateReader = zkStateReader;
            this.router = key -> coll.getRouter().getTargetSlice(key, null, null, null, null).getName();
            LinkedHashMap<String, Shard> map = new LinkedHashMap<>();
            for (Slice slice : coll.getSlices()) {
                map.put(slice.getName(), new ShardImpl(this, slice));
            }
            shards = new WrappedSimpleMap<>(map);

        }

        @Override
        public String name() {
            return coll.getName();
        }

        @Override
        public SimpleMap<Shard> shards() {
            return shards;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public String config() {
            if (confName == null) {
                // do this lazily . It's usually not necessary
                try {
                    byte[] d = zkStateReader.getZkClient().getData(getCollectionPathRoot(coll.getName()), null, null, true);
                    if (d == null || d.length == 0) return null;
                    Map m = (Map) Utils.fromJSON(d);
                    confName = (String) m.get("configName");
                } catch (KeeperException | InterruptedException e) {
                    SimpleZkMap.throwZkExp(e);
                    //cannot read from ZK
                    return null;

                }
            }
            return confName;
        }

        @Override
        public Router router() {
            return router;
        }
    }

    private class ShardImpl implements Shard {
        final SolrCollectionImpl collection;
        final Slice slice;
        final HashRange range;
        final SimpleMap<ShardReplica> replicas;

        private ShardImpl(SolrCollectionImpl collection, Slice slice) {
            this.collection = collection;
            this.slice = slice;
            range = _range(slice);
            replicas = _replicas();
        }

        private SimpleMap<ShardReplica> _replicas() {
            Map<String, ShardReplica> replicas = new HashMap<>();
            slice.forEach(replica -> replicas.put(replica.getName(), new ShardReplicaImpl(ShardImpl.this, replica)));
            return new WrappedSimpleMap<>(replicas);
        }

        private HashRange _range(Slice slice) {
            return slice.getRange() == null ?
                    null :
                    new HashRange() {
                        @Override
                        public int min() {
                            return slice.getRange().min;
                        }

                        @Override
                        public int max() {
                            return slice.getRange().max;
                        }
                    };
        }

        @Override
        public String name() {
            return slice.getName();
        }

        @Override
        public String collection() {
            return collection.name();
        }

        @Override
        public HashRange range() {
            return range;
        }

        @Override
        public SimpleMap<ShardReplica> replicas() {
            return replicas;
        }

        @Override
        public String leader() {
            Replica leader = slice.getLeader();
            return leader == null ? null : leader.getName();
        }
    }

    private class ShardReplicaImpl implements ShardReplica {
        private final ShardImpl shard;
        private final Replica replica;

        private ShardReplicaImpl(ShardImpl shard, Replica replica) {
            this.shard = shard;
            this.replica = replica;
        }

        @Override
        public String name() {
            return replica.getName();
        }

        @Override
        public String shard() {
            return shard.name();
        }

        @Override
        public String collection() {
            return shard.collection.name();
        }

        @Override
        public String node() {
            return replica.getNodeName();
        }

        @Override
        public String core() {
            return replica.getCoreName();
        }

        @Override
        public Replica.Type type() {
            return replica.getType();
        }

        @Override
        public boolean alive() {
            return zkStateReader.getClusterState().getLiveNodes().contains(node())
                    && replica.getState() == Replica.State.ACTIVE;
        }

        @Override
        public long indexSize() {
            //todo implement later
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public boolean isLeader() {
            return Objects.equals(shard.leader() , name());
        }

        @Override
        public String url(ApiType type) {
            String base = nodes.get(node()).baseUrl(type);
            if (type == ApiType.V2) {
                return base + "/cores/" + core();
            } else {
                return base + "/" + core();
            }
        }
    }

    private class Node implements SolrNode {
        private final String name;

        private Node(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String baseUrl(ApiType apiType) {
            return Utils.getBaseUrlForNodeName(name, zkStateReader.getClusterProperty(URL_SCHEME, "http"), apiType == ApiType.V2);
        }

        @Override
        public SimpleMap<ShardReplica> cores() {
            //todo implement later
            //this requires a call to the node
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    private class ConfigImpl implements CollectionConfig {
        final String name;
        final SimpleMap<Resource> resources;
        final String path;

        private ConfigImpl(String name) {
            this.name = name;
            path = ZkStateReader.CONFIGS_ZKNODE + "/" + name;
            this.resources = new SimpleZkMap(zkStateReader, path);
        }

        @Override
        public SimpleMap<Resource> resources() {
            return resources;
        }

        @Override
        public String name() {
            return name;
        }

    }

}

