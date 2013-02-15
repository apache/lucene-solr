package org.apache.lucene.store;
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
import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.IOContext.Context;

/**
 * 
 * A {@link Directory} wrapper that allows {@link IndexOutput} rate limiting using
 * {@link IOContext.Context IO context} specific {@link RateLimiter rate limiters}.
 * 
 *  @see #setRateLimiter(RateLimiter, IOContext.Context)
 * @lucene.experimental
 */
public final class RateLimitedDirectoryWrapper extends Directory {
  
  private final Directory delegate;
  // we need to be volatile here to make sure we see all the values that are set
  // / modified concurrently
  private volatile RateLimiter[] contextRateLimiters = new RateLimiter[IOContext.Context
      .values().length];
  
  public RateLimitedDirectoryWrapper(Directory wrapped) {
    this.delegate = wrapped;
  }
  
  public Directory getDelegate() {
    return delegate;
  }
  
  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    return delegate.listAll();
  }
  
  @Override
  public boolean fileExists(String name) throws IOException {
    ensureOpen();
    return delegate.fileExists(name);
  }
  
  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    delegate.deleteFile(name);
  }
  
  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    return delegate.fileLength(name);
  }
  
  @Override
  public IndexOutput createOutput(String name, IOContext context)
      throws IOException {
    ensureOpen();
    final IndexOutput output = delegate.createOutput(name, context);
    final RateLimiter limiter = getRateLimiter(context.context);
    if (limiter != null) {
      return new RateLimitedIndexOutput(limiter, output);
    }
    return output;
  }
  
  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
    delegate.sync(names);
  }
  
  @Override
  public IndexInput openInput(String name, IOContext context)
      throws IOException {
    ensureOpen();
    return delegate.openInput(name, context);
  }
  
  @Override
  public void close() throws IOException {
    isOpen = false;
    delegate.close();
  }
  
  @Override
  public IndexInputSlicer createSlicer(String name, IOContext context)
      throws IOException {
    ensureOpen();
    return delegate.createSlicer(name, context);
  }
  
  @Override
  public Lock makeLock(String name) {
    ensureOpen();
    return delegate.makeLock(name);
  }

  @Override
  public void clearLock(String name) throws IOException {
    ensureOpen();
    delegate.clearLock(name);
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    ensureOpen();
    delegate.setLockFactory(lockFactory);
  }

  @Override
  public LockFactory getLockFactory() {
    ensureOpen();
    return delegate.getLockFactory();
  }

  @Override
  public String getLockID() {
    ensureOpen();
    return delegate.getLockID();
  }

  @Override
  public String toString() {
    return "RateLimitedDirectoryWrapper(" + delegate.toString() + ")";
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    ensureOpen();
    delegate.copy(to, src, dest, context);
  }
  
  private RateLimiter getRateLimiter(IOContext.Context context) {
    assert context != null;
    return contextRateLimiters[context.ordinal()];
  }
  
  /**
   * Sets the maximum (approx) MB/sec allowed by all write IO performed by
   * {@link IndexOutput} created with the given {@link IOContext.Context}. Pass
   * <code>null</code> to have no limit.
   * 
   * <p>
   * <b>NOTE</b>: For already created {@link IndexOutput} instances there is no
   * guarantee this new rate will apply to them; it will only be guaranteed to
   * apply for new created {@link IndexOutput} instances.
   * <p>
   * <b>NOTE</b>: this is an optional operation and might not be respected by
   * all Directory implementations. Currently only {@link FSDirectory buffered}
   * Directory implementations use rate-limiting.
   * 
   * @throws IllegalArgumentException
   *           if context is <code>null</code>
   * @throws AlreadyClosedException if the {@link Directory} is already closed
   * @lucene.experimental
   */
  public void setMaxWriteMBPerSec(Double mbPerSec, IOContext.Context context) {
    ensureOpen();
    if (context == null) {
      throw new IllegalArgumentException("Context must not be null");
    }
    final int ord = context.ordinal();
    final RateLimiter limiter = contextRateLimiters[ord];
    if (mbPerSec == null) {
      if (limiter != null) {
        limiter.setMbPerSec(Double.MAX_VALUE);
        contextRateLimiters[ord] = null;
      }
    } else if (limiter != null) {
      limiter.setMbPerSec(mbPerSec);
      contextRateLimiters[ord] = limiter; // cross the mem barrier again
    } else {
      contextRateLimiters[ord] = new RateLimiter.SimpleRateLimiter(mbPerSec);
    }
  }
  
  /**
   * Sets the rate limiter to be used to limit (approx) MB/sec allowed by all IO
   * performed with the given {@link IOContext.Context context}. Pass <code>null</code> to
   * have no limit.
   * 
   * <p>
   * Passing an instance of rate limiter compared to setting it using
   * {@link #setMaxWriteMBPerSec(Double, IOContext.Context)}
   * allows to use the same limiter instance across several directories globally
   * limiting IO across them.
   * 
   * @throws IllegalArgumentException
   *           if context is <code>null</code>
   * @throws AlreadyClosedException if the {@link Directory} is already closed           
   * @lucene.experimental
   */
  public void setRateLimiter(RateLimiter mergeWriteRateLimiter,
      Context context) {
    ensureOpen();
    if (context == null) {
      throw new IllegalArgumentException("Context must not be null");
    }
    contextRateLimiters[context.ordinal()] = mergeWriteRateLimiter;
  }
  
  /**
   * See {@link #setMaxWriteMBPerSec}.
   * 
   * @throws IllegalArgumentException
   *           if context is <code>null</code>
   * @throws AlreadyClosedException if the {@link Directory} is already closed
   * @lucene.experimental
   */
  public Double getMaxWriteMBPerSec(IOContext.Context context) {
    ensureOpen();
    if (context == null) {
      throw new IllegalArgumentException("Context must not be null");
    }
    RateLimiter limiter = getRateLimiter(context);
    return limiter == null ? null : limiter.getMbPerSec();
  }
  
}
