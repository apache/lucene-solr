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

/**
 * <p>This package contains the interfaces giving access to cluster state, including nodes, collections and the
 * structure of the collections (shards and replicas). These interfaces allow separating external code contribution
 * from the internal Solr implementations of these concepts to make usage simpler and to not require changes to
 * external contributed code every time the internal abstractions are modified.</p>
 *
 * <p>The top level abstraction is {@link org.apache.solr.cluster.Cluster}. The cluster is composed of {@link org.apache.solr.cluster.Node}s.
 * Indexes are stored in {@link org.apache.solr.cluster.SolrCollection}s, composed of {@link org.apache.solr.cluster.Shard}s
 * whose actual copies on {@link org.apache.solr.cluster.Node}s are called {@link org.apache.solr.cluster.Replica}s.</p>
 */
package org.apache.solr.cluster;