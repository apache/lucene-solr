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

package org.apache.solr.cluster.placement;

import java.util.Iterator;

/**
 * Represents a Collection in SolrCloud (unrelated to {@link java.util.Collection} that uses the nicer name).
 */
public interface SolrCollection {
  /**
   * The collection name (value passed to {@link Cluster#getCollection(String)}).
   */
  String getName();

  /**
   * Returns the {@link Shard} of the given name for that collection, if such a shard exists. Note that when a request
   * for adding replicas for a collection is received, it is possible that replicas need to be added for non existing
   * shards (see {@link PlacementRequest#getShardNames()}. Non existing shards will not be returned by this
   * method. Only shards already existing will be returned.
   * @return {@code null} if the shard does not or does not yet exist for the collection.
   */
  Shard getShard(String name);

  /**
   * @return an iterator over {@link Shard}s already existing for this {@link SolrCollection}. When a collection is being
   * created, replicas have to be added to shards that do not already exist for the collection and that will not be returned
   * by this iterator nor will be fetchable using {@link #getShard(String)}.
   */
  Iterator<Shard> iterator();

  /**
   * Allow foreach iteration on shards such as: {@code for (Shard s : solrCollection.shards()) {...}}.
   */
  Iterable<Shard> shards();

  /**
   * <p>Returns the value of a custom property name set on the {@link SolrCollection} or {@code null} when no such
   * property was set.
   *
   * <p>Properties are set through the Collection API. See for example {@code COLLECTIONPROP} in the Solr reference guide.
   * Using custom properties in conjunction with plugin code understanding them allows the Solr client to customize placement
   * decisions per collection.
   *
   * <p>For example if a collection is to be placed only on nodes using SSD storage and not rotating disks, it can be
   * identified as such using some custom property (collection property could for example be called "driveType" and have
   * value "ssd" in that case), and the placement plugin (implementing {@link PlacementPlugin}) would then
   * {@link AttributeFetcher#requestNodeSystemProperty(String)} for that property from all nodes and only place replicas
   * of this collection on {@link Node}'s for which
   * {@link AttributeValues#getDiskType(Node)} is non empty and equal to {@link AttributeFetcher.DiskHardwareType#SSD}.
   */
  String getCustomProperty(String customPropertyName);

  /*
   * There might be missing pieces here (and in other classes in this package) and these would have to be added when
   * starting to use these interfaces to code real world placement and balancing code (plugins) as well as when the Solr
   * side implementation of these interfaces is done.
   *
   * For example, attributes that are associated to a collection internally but that are (currently) not believed to be
   * of interest to placement plugin code:
   * - Routing hash range per shard (or the router used in a collection)
   * -
   *
   * This comment should be removed eventually.
   */
}
