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

/**
 * Callback registered with {@link ZkStateReader#registerCloudCollectionsListener(CloudCollectionsListener)}
 * and called whenever the cloud's set of collections changes.
 */
public interface CloudCollectionsListener {

  /**
   * Called when a collection is created, a collection is deleted or a watched collection's state changes.
   *
   * Note that, due to the way Zookeeper watchers are implemented, a single call may be
   * the result of multiple or no collection creation or deletions. Also, multiple calls to this method can be made
   * with the same set of collections, ie. without any new updates.
   *
   * @param oldCollections       the previous set of collections
   * @param newCollections       the new set of collections
   */
  void onChange(Set<String> oldCollections, Set<String> newCollections);

}
