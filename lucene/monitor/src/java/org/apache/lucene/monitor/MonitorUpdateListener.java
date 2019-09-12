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

package org.apache.lucene.monitor;

import java.util.List;

/**
 * For reporting events on a Monitor's query index
 */
public interface MonitorUpdateListener {

  /**
   * Called after a set of queries have been added to the Monitor's query index
   */
  default void afterUpdate(List<MonitorQuery> updates) {};

  /**
   * Called after a set of queries have been deleted from the Monitor's query index
   */
  default void afterDelete(List<String> queryIds) {};

  /**
   * Called after all queries have been removed from the Monitor's query index
   */
  default void afterClear() {};

  /**
   * Called after the Monitor's query cache has been purged of deleted queries
   */
  default void onPurge() {};

  /**
   * Called if there was an error removing deleted queries from the Monitor's query cache
   */
  default void onPurgeError(Throwable t) {};

}
