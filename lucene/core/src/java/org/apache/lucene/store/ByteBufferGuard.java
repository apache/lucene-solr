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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A guard that is created for every {@link ByteBufferIndexInput} that tries on best effort
 * to reject any access to the {@link ByteBuffer} behind, once it is unmapped. A single instance
 * of this is used for the original and all clones, so once the original is closed and unmapped
 * all clones also throw {@link AlreadyClosedException}, triggered by a {@link NullPointerException}.
 * <p>
 * This code uses the trick that is also used in
 * {@link java.lang.invoke.MutableCallSite#syncAll(java.lang.invoke.MutableCallSite[])} to
 * invalidate switch points. It also yields the current thread to give other threads a chance
 * to finish in-flight requests...
 */
final class ByteBufferGuard {
  
  /**
   * Pass in an implementation of this interface to cleanup ByteBuffers.
   * MMapDirectory implements this to allow unmapping of bytebuffers with private Java APIs.
   */
  @FunctionalInterface
  static interface BufferCleaner {
    void freeBuffer(String resourceDescription, ByteBuffer b) throws IOException;
  }
  
  private final String resourceDescription;
  private final BufferCleaner cleaner;
  
  /** not volatile, we use store-store barrier! */
  private boolean invalidated = false;
  
  /** the actual store-store barrier. */
  private final AtomicInteger barrier = new AtomicInteger();
  
  /**
   * Creates an instance to be used for a single {@link ByteBufferIndexInput} which
   * must be shared by all of its clones.
   */
  public ByteBufferGuard(String resourceDescription, BufferCleaner cleaner) {
    this.resourceDescription = resourceDescription;
    this.cleaner = cleaner;
  }
  
  /**
   * Invalidates this guard and unmaps (if supported).
   */
  public void invalidateAndUnmap(ByteBuffer... bufs) throws IOException {
    if (cleaner != null) {
      invalidated = true;
      // this should trigger a happens-before - so flushes all caches
      barrier.lazySet(0);
      Thread.yield();
      for (ByteBuffer b : bufs) {
        cleaner.freeBuffer(resourceDescription, b);
      }
    }
  }
  
  private void ensureValid() {
    if (invalidated) {
      // this triggers an AlreadyClosedException in ByteBufferIndexInput:
      throw new NullPointerException();
    }
  }
  
  public void getBytes(ByteBuffer receiver, byte[] dst, int offset, int length) {
    ensureValid();
    receiver.get(dst, offset, length);
  }
  
  public byte getByte(ByteBuffer receiver) {
    ensureValid();
    return receiver.get();
  }
  
  public short getShort(ByteBuffer receiver) {
    ensureValid();
    return receiver.getShort();
  }
  
  public int getInt(ByteBuffer receiver) {
    ensureValid();
    return receiver.getInt();
  }
  
  public long getLong(ByteBuffer receiver) {
    ensureValid();
    return receiver.getLong();
  }
  
  public byte getByte(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.get(pos);
  }
  
  public short getShort(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.getShort(pos);
  }
  
  public int getInt(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.getInt(pos);
  }
  
  public long getLong(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.getLong(pos);
  }
    
}
