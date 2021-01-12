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
package org.apache.solr.cluster.events;

import java.io.Closeable;

/**
 * Components that want to be notified of cluster-wide events should use this.
 *
 * XXX should this work only for ClusterSingleton-s? some types of events may be
 * XXX difficult (or pointless) to propagate to every node.
 */
public interface ClusterEventListener extends Closeable {

  /**
   * Handle the event. Implementations should be non-blocking - if any long
   * processing is needed it should be performed asynchronously.
   * @param event cluster event
   */
  void onEvent(ClusterEvent event);

}
