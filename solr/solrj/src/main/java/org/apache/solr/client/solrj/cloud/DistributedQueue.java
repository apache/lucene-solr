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
package org.apache.solr.client.solrj.cloud;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.solr.common.util.Pair;

/**
 * Distributed queue component. Methods largely follow those in {@link java.util.Queue}.
 */
public interface DistributedQueue {
  byte[] peek() throws Exception;

  byte[] peek(boolean block) throws Exception;

  byte[] peek(long wait) throws Exception;

  byte[] poll() throws Exception;

  byte[] remove() throws Exception;

  byte[] take() throws Exception;

  void offer(byte[] data) throws Exception;

  /**
   * Retrieve statistics about the queue size, operations and their timings.
   */
  Map<String, Object> getStats();

  /**
   * Peek multiple elements from the queue in a single call.
   * @param max maximum elements to retrieve
   * @param waitMillis if less than maximum element is in the queue then wait at most this time for at least one new element.
   * @param acceptFilter peek only elements that pass this filter
   * @return peeked elements
   * @throws Exception on errors
   */
  Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws Exception;

}
