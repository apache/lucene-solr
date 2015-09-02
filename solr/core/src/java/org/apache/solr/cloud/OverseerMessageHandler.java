package org.apache.solr.cloud;

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

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;

/**
 * Interface for processing messages received by an {@link OverseerTaskProcessor}
 */
public interface OverseerMessageHandler {

  /**
   * @param message the message to process
   * @param operation the operation to process
   *
   * @return response
   */
  SolrResponse processMessage(ZkNodeProps message, String operation);

  /**
   * @return the name of the OverseerMessageHandler
   */
  String getName();

  /**
   * @param operation the operation to be timed
   *
   * @return the name of the timer to use for the operation
   */
  String getTimerName(String operation);

  /**
   * @param message the message being processed
   *
   * @return the taskKey for the message for handling task exclusivity
   */
  String getTaskKey(ZkNodeProps message);

  /**
   * @param taskKey the key associated with the task, cached from getTaskKey
   * @param message the message being processed
   */
  void markExclusiveTask(String taskKey, ZkNodeProps message);
  
  /**
   * @param taskKey the key associated with the task
   * @param operation the operation being processed
   * @param message the message being processed
   */
  void unmarkExclusiveTask(String taskKey, String operation, ZkNodeProps message);

  /**
   * @param taskKey the key associated with the task
   * @param message the message being processed
   *
   * @return the exclusive marking
   */
  ExclusiveMarking checkExclusiveMarking(String taskKey, ZkNodeProps message);

  enum ExclusiveMarking {
    NOTDETERMINED,    // not enough context, fall back to the processor (i.e. look at running tasks)
    EXCLUSIVE,
    NONEXCLUSIVE
  }
}
