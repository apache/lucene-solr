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

/**
 * A {@link RateLimiter rate limiting} {@link IndexOutput}
 * 
 * @lucene.internal
 */
final class RateLimitedIndexOutput extends BufferedIndexOutput {
  
  private final IndexOutput delegate;
  private final BufferedIndexOutput bufferedDelegate;
  private final RateLimiter rateLimiter;

  RateLimitedIndexOutput(final RateLimiter rateLimiter, final IndexOutput delegate) {
    // TODO should we make buffer size configurable
    if (delegate instanceof BufferedIndexOutput) {
      bufferedDelegate = (BufferedIndexOutput) delegate;
      this.delegate = delegate;
    } else {
      this.delegate = delegate;
      bufferedDelegate = null;
    }
    this.rateLimiter = rateLimiter;
  }
  
  @Override
  protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
    rateLimiter.pause(len);
    if (bufferedDelegate != null) {
      bufferedDelegate.flushBuffer(b, offset, len);
    } else {
      delegate.writeBytes(b, offset, len);
    }
    
  }
  
  @Override
  public long length() throws IOException {
    return delegate.length();
  }

  @Override
  public void seek(long pos) throws IOException {
    flush();
    delegate.seek(pos);
  }


  @Override
  public void flush() throws IOException {
    try {
      super.flush();
    } finally { 
      delegate.flush();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      delegate.close();
    }
  }
}
