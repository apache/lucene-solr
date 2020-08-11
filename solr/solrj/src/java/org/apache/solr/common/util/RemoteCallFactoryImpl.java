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
package org.apache.solr.common.util;

import org.apache.http.client.HttpClient;
import org.apache.solr.cluster.api.*;
import org.apache.solr.cluster.api.CallRouter.ReplicaType;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;

import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;

public class RemoteCallFactoryImpl implements RemoteCallFactory {
    final Builder builder;
    static final Random RANDOM;
    static {
        String seed = System.getProperty("tests.seed");
        if (seed == null) {
            RANDOM = new Random();
        } else {
            RANDOM = new Random(seed.hashCode());
        }
    }

    public RemoteCallFactoryImpl(Builder builder) {
        this.builder = builder;
    }

    public static class Builder {
        SolrCluster cluster;
        HttpClient httpClient;

        public Builder withCluster(SolrCluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder withHttpClient(HttpClient client) {
            this.httpClient = client;
            return this;
        }
    }

    @Override
    public CallRouter createCallRouter() {
        return new Router(builder.cluster);
    }

    @Override
    public HttpRemoteCall createHttpRemoteCall() {
        return new HttpRemoteCallImpl();
    }

    /**This is a node specific request
     */
    private static class NodeRouter implements PathSupplier {
        final Router router;
        final String node, core;
        private String path;


        private NodeRouter(Router router, String node, String core) {
            this.router = router;
            this.node = node;
            this.core = core;
        }

        @Override
        public Iterator<String> getPaths(ApiType apiType) {
            if(path == null) initPath(apiType);
            return Collections.singletonList(path).iterator();
        }


        private void initPath(ApiType apiType) {
            path = router.cluster.nodes().get(node).baseUrl(apiType);
            if (core != null) {
                if (apiType == ApiType.V2) {
                    path = path + "/cores/" + core;
                } else {
                    path = path + "/" + core;
                }
            }
        }
    }

    /**This is a collection specific request and the destination is one or more replicas
     *
     */
    private static class CollectionRouter implements PathSupplier , MapWriter {
        final String collectionName, shardName, replicaName;
        final Router router;
        final ReplicaType type;
        private List<ShardReplica> replicas;

        CollectionRouter(String collection, String shard, String replica, ReplicaType type, Router router) {
            this.collectionName = collection;
            this.shardName = shard;
            this.type = type == null? ReplicaType.ANY: type;
            this.replicaName = replica;
            this.router = router;
        }

        @Override
        public Iterator<String> getPaths(ApiType apiType) {
            if (replicas == null) replicas = identifyReplicas();
            Iterator<ShardReplica> delegate = replicas.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public String next() {
                    ShardReplica next = delegate.next();
                    if (next == null) {
                        return null;
                    }
                    return next.url(apiType);
                }
            };
        }

        private List<ShardReplica> identifyReplicas() {
            List<ShardReplica> replicas = new ArrayList<>();
            SolrCollection collection = router.cluster.collections().get(collectionName);
            if (collection == null) {
                throw new RemoteCallException(SolrException.ErrorCode.BAD_REQUEST, "No such collection :" + collectionName);
            }
            if (replicaName != null) {
                collection.shards().forEachEntry((s, shard) -> shard.replicas().forEachEntry((n, r) -> {
                    if (replicaName.equals(r.name()) && filterReplica(r)) {
                        replicas.add(r);
                    }
                }));
            } else if (shardName != null) {
                Shard shard = collection.shards().get(shardName);
                if (shard == null) {
                    throw new RemoteCallException(SolrException.ErrorCode.BAD_REQUEST, "No such shard :" + shardName);
                }
                shard.replicas().forEachEntry((s, replica) -> {
                    if (filterReplica(replica)) {
                        replicas.add(replica);
                    }
                });

            }
            if (replicas.isEmpty()) {
                throw new RemoteCallException(SolrException.ErrorCode.BAD_REQUEST, "No replica satisfies the route :" + Utils.toJSONString(this));
            }
            if (replicas.size() > 1) {
                Collections.shuffle(replicas, RANDOM);
            }
            return replicas;
        }

        private boolean filterReplica(ShardReplica r) {
            return  router.isReplicaAllowed(r) &&
                    type.test(r);
        }

        @Override
        public void writeMap(EntryWriter ew) throws IOException {
            ew.putIfNotNull(COLLECTION, collectionName);
            ew.putIfNotNull("shard", shardName);
            ew.putIfNotNull("replica", replicaName);
            ew.putIfNotNull("type", type);
        }
    }
    private static class Router implements CallRouter {
        PathSupplier pathSupplier;
        Predicate<ShardReplica> filter;
        final SolrCluster cluster;

        private Router(SolrCluster cluster) {
            this.cluster = cluster;
        }

        private boolean isReplicaAllowed(ShardReplica r) {
            return filter == null || filter.test(r);
        }

        @Override
        public CallRouter withReplicaFilter(Predicate<ShardReplica> filter) {
            this.filter = filter;
            return this;
        }

        @Override
        public CallRouter anyNode() {
            pathSupplier = new NodeRouter(this, null, null);
            return this;
        }

        @Override
        public CallRouter toNode(String nodeName) {
            pathSupplier = new NodeRouter(this, nodeName, null);
            return this;
        }

        @Override
        public CallRouter toShard(String collection, String shard, ReplicaType type) {
            new CollectionRouter(collection, shard, null, type, this);
            return this;
        }

        @Override
        public CallRouter toShard(String collection, ReplicaType type, String routeKey) {
            String shardName = cluster.collections(true).get(collection).router().shard(routeKey);
            pathSupplier = new CollectionRouter(collection, shardName,null, type, this);
            return this;
        }

        @Override
        public CallRouter toReplica(String collection, String replicaName) {
            pathSupplier = new CollectionRouter(collection, null,replicaName, ReplicaType.ANY, this);
            return this;
        }

        @Override
        public CallRouter toCollection(String collection) {
            pathSupplier = new CollectionRouter(collection, null, null, ReplicaType.ANY , this);
            return this;
        }

        @Override
        public CallRouter toCore(String node, String core) {
            pathSupplier = new NodeRouter(this, node, core);
            return this;
        }

        @Override
        public List<CallRouter> toEachShard(String collection, ReplicaType type) {
            List<CallRouter> result = new ArrayList<>();
            cluster.collections()
                    .get(collection)
                    .shards()
                    .forEachKey(it -> result.add(new Router(cluster).toShard(collection, it, type)));
            return result;
        }

        @Override
        public List<CallRouter> toEachReplica(String collection) {
            List<CallRouter> result = new ArrayList<>();
            cluster.collections()
                    .get(collection)
                            .shards()
                                  .forEachEntry((s1, shard) -> shard.replicas()
                                           .forEachKey(it -> result.add(new Router(cluster).toReplica(collection, it))));

            return result;
        }

        @Override
        public HttpRemoteCall createHttpRpc() {
            return new HttpRemoteCallImpl().withPathSupplier(this.pathSupplier);
        }


    }
    private static class HttpRemoteCallImpl implements HttpRemoteCall {
        private PathSupplier pathSupplier;
        private HttpRemoteCall.Method method = Method.GET;
        private StringBuilder queryStr;
        private NamedList<String> headers;
        private ResponseConsumer responseConsumer;
        private LinkedList<HeaderConsumer> headerConsumers;
        private InputSupplier inputSupplier;
        private String handler;
        private ApiType apiType;

        @Override
        public HttpRemoteCall withPathSupplier(PathSupplier pathSupplier) {
            this.pathSupplier = pathSupplier;
            return this;
        }

        @Override
        public HttpRemoteCall addParam(String key, String val) {
            return _appendParam(key, val,true);
        }

        private HttpRemoteCallImpl _appendParam(String key, String val, boolean encVal) {
            if (queryStr == null) {
                queryStr = new StringBuilder("?");
            } else {
                queryStr.append("&");
            }
            queryStr.append(URLEncoder.encode(key, StandardCharsets.UTF_8)).append("=");
            if (val != null) {
                if(encVal){
                    queryStr.append(val);
                } else {
                    queryStr.append(URLEncoder.encode(val, StandardCharsets.UTF_8));
                }
            }
            return this;
        }

        @Override
        public HttpRemoteCall addParam(String key, int val) {
            return _appendParam(key, String.valueOf(val), false);
        }

        @Override
        public HttpRemoteCall addParam(String key, boolean val) {
            return _appendParam(key, String.valueOf(val), false);
        }

        @Override
        public HttpRemoteCall addParam(String key, float val) {
            return _appendParam(key, String.valueOf(val), false);
        }

        @Override
        public HttpRemoteCall addParam(String key, double val) {
            return _appendParam(key, String.valueOf(val), false);
        }

        @Override
        public HttpRemoteCall addHeader(String key, String val) {
            if(headers == null) headers = new NamedList<>();
            headers.add(key, val);
            return this;
        }

        @Override
        public HttpRemoteCall withResponseConsumer(ResponseConsumer sink) {
            this.responseConsumer = sink;
            return this;
        }

        @Override
        public HttpRemoteCall withHeaderConsumer(HeaderConsumer headerConsumer) {
            if(headerConsumers == null) headerConsumers = new LinkedList<>();
            headerConsumers.add(headerConsumer);
            return this;
        }

        @Override
        public HttpRemoteCall withPayload(InputSupplier payload) {
            this.inputSupplier = payload;
            return this;
        }

        @Override
        public HttpRemoteCall withMethod(HttpRemoteCall.Method method) {
            this.method = method;
            return this;
        }

        @Override
        public HttpRemoteCall withPath( String handler, ApiType type) {
            this.handler = handler;
            this.apiType = type;
            return null;
        }

        @Override
        public Object invoke() throws RemoteCallException {
            return null;
        }
    }
}
