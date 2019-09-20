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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

/**
 * A guard that is created for every {@link ByteBufferIndexInput} that tries on best effort
 * to reject any access to the {@link ByteBuffer} behind, once it is unmapped. A single instance
 * of this is used for the original and all clones, so once the original is closed and unmapped
 * all clones also throw {@link AlreadyClosedException}, triggered by a {@link NullPointerException}.
 * <p>
 * This code tries to hopefully flush any CPU caches using a full fence (volatile write) and
 * <em>eventually</em> see the state change using opaque reads. It also yields the current thread
 * to give other threads a chance to finish in-flight requests...
 * 
 * @see <a href="http://gee.cs.oswego.edu/dl/html/j9mm.html">Doug Lea: Using JDK 9 Memory Order Modes</a>
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
  
  @SuppressWarnings("unused")
  private volatile boolean invalidated = false;
  
  /** Used to access the volatile variable with different memory semantics
   * (volatile write for barrier with memory_order_seq_cst semantics, opaque reads with memory_order_relaxed semantics): */
  private static final VarHandle VH_INVALIDATED;
  static {
    try {
      VH_INVALIDATED = MethodHandles.lookup().findVarHandle(ByteBufferGuard.class, "invalidated", boolean.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
  
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
      // This call should flush any CPU caches and as a result make
      // the "invalidated" field update visible to other threads:
      VH_INVALIDATED.setVolatile(this, true); 
      // we give other threads a bit of time to finish reads on their ByteBuffer...:
      Thread.yield();
      // finally unmap the ByteBuffers:
      for (ByteBuffer b : bufs) {
        cleaner.freeBuffer(resourceDescription, b);
      }
    }
  }
  
  private void ensureValid() {
    if (cleaner != null && (boolean) VH_INVALIDATED.getOpaque(this)) {
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
