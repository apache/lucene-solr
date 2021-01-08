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

import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementRequest;

import java.util.Iterator;
import java.util.Set;

/**
 * Represents a Collection in SolrCloud (unrelated to {@link java.util.Collection} that uses the nicer name).
 */
public interface SolrCollection {
  /**
   * The collection name (value passed to {@link Cluster#getCollection(String)}).
   */
  String getName();

  /**
   * <p>Returns the {@link Shard} of the given name for that collection, if such a shard exists.</p>
   *
   * <p>Note that when a request for adding replicas for a collection is received by a {@link PlacementPlugin}, it is
   * possible that replicas need to be added to non existing shards (see {@link PlacementRequest#getShardNames()}.
   * Non existing shards <b>will not</b> be returned by this method. Only shards already existing will be returned.</p>
   *
   * @return {@code null} if the shard does not or does not yet exist for the collection.
   */
  Shard getShard(String name);

  /**
   * @return an iterator over {@link Shard}s of this {@link SolrCollection}.
   */
  Iterator<Shard> iterator();

  /**
   * Allow foreach iteration on shards such as: {@code for (Shard s : solrCollection.shards()) {...}}.
   */
  Iterable<Shard> shards();

  /**
   * @return a set of the names of the shards defined for this collection. This set is backed by an internal map so should
   * not be modified.
   */
  Set<String> getShardNames();

    /**
     * <p>Returns the value of a custom property name set on the {@link SolrCollection} or {@code null} when no such
     * property was set. Properties are set through the Collection API. See for example {@code COLLECTIONPROP} in the Solr reference guide.
     *
     * <p><b>{@link PlacementPlugin} related note:</b></p>
     * <p>Using custom properties in conjunction with ad hoc {@link PlacementPlugin} code allows customizing placement
     * decisions per collection.
     *
     * <p>For example if a collection is to be placed only on nodes using located in a specific availability zone, it can be
     * identified as such using some custom property (collection property could for example be called "availabilityZone" and have
     * value "az1" in that case), and the placement plugin (implementing {@link PlacementPlugin}) would then
     * {@link AttributeFetcher#requestNodeSystemProperty(String)} for that property from all nodes and only place replicas
     * of this collection on {@link Node}'s for which this attribute is non empty and equal.
     */
  String getCustomProperty(String customPropertyName);

  /*
   * There might be missing pieces here (and in other classes in this package) and these would have to be added when
   * starting to use these interfaces to code real world placement and balancing code (plugins).
   */
}
