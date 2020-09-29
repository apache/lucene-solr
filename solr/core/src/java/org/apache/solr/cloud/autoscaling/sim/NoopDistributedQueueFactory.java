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
package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.common.util.Pair;

/**
 * A queue factory implementation that does nothing.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class NoopDistributedQueueFactory implements DistributedQueueFactory {

  public static final DistributedQueueFactory INSTANCE = new NoopDistributedQueueFactory();

  @Override
  public DistributedQueue makeQueue(String path) throws IOException {
    return NoopDistributedQueue.INSTANCE;
  }

  @Override
  public void removeQueue(String path) throws IOException {

  }

  private static final class NoopDistributedQueue implements DistributedQueue {
    static final DistributedQueue INSTANCE = new NoopDistributedQueue();

    @Override
    public byte[] peek() throws Exception {
      return new byte[0];
    }

    @Override
    public byte[] peek(boolean block) throws Exception {
      return new byte[0];
    }

    @Override
    public byte[] peek(long wait) throws Exception {
      return new byte[0];
    }

    @Override
    public byte[] poll() throws Exception {
      return new byte[0];
    }

    @Override
    public byte[] remove() throws Exception {
      return new byte[0];
    }

    @Override
    public byte[] take() throws Exception {
      return new byte[0];
    }

    @Override
    public void offer(byte[] data) throws Exception {

    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws Exception {
      return Collections.emptyList();
    }
  }
}
