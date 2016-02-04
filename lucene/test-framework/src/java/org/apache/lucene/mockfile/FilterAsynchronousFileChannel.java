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
package org.apache.lucene.mockfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.util.Objects;
import java.util.concurrent.Future;

/**  
 * A {@code FilterAsynchronousFileChannel} contains another 
 * {@code AsynchronousFileChannel}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public class FilterAsynchronousFileChannel extends AsynchronousFileChannel {
  
  /** 
   * The underlying {@code AsynchronousFileChannel} instance. 
   */
  protected final AsynchronousFileChannel delegate;
  
  /**
   * Construct a {@code FilterAsynchronousFileChannel} based on 
   * the specified base channel.
   * <p>
   * Note that base channel is closed if this channel is closed.
   * @param delegate specified base channel.
   */
  public FilterAsynchronousFileChannel(AsynchronousFileChannel delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }
  
  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public AsynchronousFileChannel truncate(long size) throws IOException {
    delegate.truncate(size);
    return this;
  }

  @Override
  public void force(boolean metaData) throws IOException {
    delegate.force(metaData);
  }

  @Override
  public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock,? super A> handler) {
    delegate.lock(position, size, shared, attachment, handler);
  }

  @Override
  public Future<FileLock> lock(long position, long size, boolean shared) {
    return delegate.lock(position, size, shared);
  }

  @Override
  public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    return delegate.tryLock(position, size, shared);
  }

  @Override
  public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer,? super A> handler) {
    delegate.read(dst, position, attachment, handler);
  }

  @Override
  public Future<Integer> read(ByteBuffer dst, long position) {
    return delegate.read(dst, position);
  }

  @Override
  public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer,? super A> handler) {
    delegate.write(src, position, attachment, handler);
  }

  @Override
  public Future<Integer> write(ByteBuffer src, long position) {
    return delegate.write(src, position);
  }
}
