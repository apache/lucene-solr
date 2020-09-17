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

package org.apache.solr.cluster;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * <p>A representation of the SolrCloud cluster state, providing information on which nodes and collections are part of
 * the cluster and a way to get to more detailed info.
 */
public interface Cluster {
  /**
   * @return current set of live nodes.
   */
  Set<Node> getLiveNodes();

  /**
   * Returns info about the given collection if one exists.
   *
   * @return {@code null} if no collection of the given name exists in the cluster.
   */
  SolrCollection getCollection(String collectionName) throws IOException;

  /**
   * @return an iterator over all {@link SolrCollection}s in the cluster.
   */
  Iterator<SolrCollection> iterator();

  /**
   * Allow foreach iteration on all collections of the cluster, such as: {@code for (SolrCollection c : cluster.collections()) {...}}.
   */
  Iterable<SolrCollection> collections();
}
