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
package org.apache.solr.store.blockcache;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @lucene.experimental
 */
public class BufferStore implements Store {

  private static final Store EMPTY = new Store() {

    @Override
    public byte[] takeBuffer(int bufferSize) {
      return new byte[bufferSize];
    }

    @Override
    public void putBuffer(byte[] buffer) {
    }
  };

  private final static ConcurrentMap<Integer, BufferStore> bufferStores = new ConcurrentHashMap<>(8192, 0.75f, 512);

  private final BlockingQueue<byte[]> buffers;

  private final int bufferSize;

  private final AtomicLong shardBuffercacheAllocate;
  private final AtomicLong shardBuffercacheLost;

  /**
   * For testing only
   *
   * @lucene.internal
   */
  static void clearBufferStores() {
    bufferStores.clear();
  }
  
  public synchronized static void initNewBuffer(int bufferSize, long totalAmount) {
    initNewBuffer(bufferSize, totalAmount, null);
  }

  public synchronized static void initNewBuffer(int bufferSize, long totalAmount, Metrics metrics) {
    if (totalAmount == 0) {
      return;
    }
    BufferStore bufferStore = bufferStores.get(bufferSize);
    if (bufferStore == null) {
      long count = totalAmount / bufferSize;
      if (count > Integer.MAX_VALUE) {
        count = Integer.MAX_VALUE;
      }
      AtomicLong shardBuffercacheLost = new AtomicLong(0);
      AtomicLong shardBuffercacheAllocate = new AtomicLong(0);
      if (metrics != null) {
        shardBuffercacheLost = metrics.shardBuffercacheLost;
        shardBuffercacheAllocate = metrics.shardBuffercacheAllocate;
      }
      BufferStore store = new BufferStore(bufferSize, (int) count, shardBuffercacheAllocate, shardBuffercacheLost);
      bufferStores.put(bufferSize, store);
    }
  }

  private BufferStore(int bufferSize, int count, AtomicLong shardBuffercacheAllocate, AtomicLong shardBuffercacheLost) {
    this.bufferSize = bufferSize;
    this.shardBuffercacheAllocate = shardBuffercacheAllocate;
    this.shardBuffercacheLost = shardBuffercacheLost;
    buffers = setupBuffers(bufferSize, count);
  }

  private static BlockingQueue<byte[]> setupBuffers(int bufferSize, int count) {
    BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(count);
    for (int i = 0; i < count; i++) {
      queue.add(new byte[bufferSize]);
    }
    return queue;
  }

  public static Store instance(int bufferSize) {
    BufferStore bufferStore = bufferStores.get(bufferSize);
    if (bufferStore == null) {
      return EMPTY;
    }
    return bufferStore;
  }

  @Override
  public byte[] takeBuffer(int bufferSize) {
    if (this.bufferSize != bufferSize) {
      throw new RuntimeException("Buffer with length [" + bufferSize + "] does not match buffer size of ["
          + bufferSize + "]");
    }
    return newBuffer(buffers.poll());
  }

  @Override
  public void putBuffer(byte[] buffer) {
    if (buffer == null) {
      return;
    }
    if (buffer.length != bufferSize) {
      throw new RuntimeException("Buffer with length [" + buffer.length + "] does not match buffer size of ["
          + bufferSize + "]");
    }
    checkReturn(buffers.offer(buffer));
  }

  private void checkReturn(boolean accepted) {
    if (!accepted) {
      shardBuffercacheLost.incrementAndGet();
    }
  }

  private byte[] newBuffer(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    shardBuffercacheAllocate.incrementAndGet();
    return new byte[bufferSize];
  }
}
