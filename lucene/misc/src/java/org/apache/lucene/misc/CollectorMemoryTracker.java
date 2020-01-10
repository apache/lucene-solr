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

package org.apache.lucene.misc;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.MemoryTracker;

/**
 * Default implementation of {@code MemoryTracker} that tracks
 * allocations and allows setting a memory limit per collector
 */
public class CollectorMemoryTracker implements MemoryTracker {
  private String name;
  private AtomicLong memoryUsage;
  private final long memoryLimit;

  public CollectorMemoryTracker(String name, long memoryLimit) {
    this.name = name;
    this.memoryUsage = new AtomicLong();
    this.memoryLimit = memoryLimit;
  }

  @Override
  public void updateBytes(long bytes) {
    long currentMemoryUsage = memoryUsage.addAndGet(bytes);

    if (currentMemoryUsage > memoryLimit) {
      throw new IllegalStateException("Memory limit exceeded for " + name);
    }
    if (currentMemoryUsage < 0) {
      throw new IllegalStateException("Illegal Memory State for " + name);
    }
  }

  @Override
  public long getBytes() {
    return memoryUsage.get();
  }
}
