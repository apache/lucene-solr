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

package org.apache.solr.common.cloud;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Interface to determine if a set of liveNodes and a collection's state matches some expecatations.
 *
 * @see ZkStateReader#waitForState(String, long, TimeUnit, CollectionStatePredicate)
 * @see ZkStateReader#waitForState(String, long, TimeUnit, Predicate)
 */
public interface CollectionStatePredicate {

  /**
   * Check if the set of liveNodes <em>and</em> the collection state matches a required state
   * <p>
   * Note that both liveNodes and collectionState should be consulted to determine
   * the overall state.
   * </p>
   *
   * @param liveNodes the current set of live nodes
   * @param collectionState the latest collection state, or null if the collection
   *                        does not exist
   * @return true if the input matches the requirements of this predicate
   */
  boolean matches(Set<String> liveNodes, DocCollection collectionState);

}
