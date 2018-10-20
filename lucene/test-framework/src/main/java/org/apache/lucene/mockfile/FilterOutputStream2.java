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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**  
 * A {@code FilterOutputStream2} contains another 
 * {@code OutputStream}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 * <p>
 * Note: unlike {@link FilterOutputStream} this class
 * delegates every method by default. This means to transform
 * {@code write} calls, you need to override multiple methods.
 * On the other hand, it is less trappy: a simple implementation 
 * that just overrides {@code close} will not force bytes to be 
 * written one-at-a-time.
 */
public abstract class FilterOutputStream2 extends OutputStream {
  
  /** 
   * The underlying {@code OutputStream} instance. 
   */
  protected final OutputStream delegate;
  
  /**
   * Construct a {@code FilterOutputStream2} based on 
   * the specified base stream.
   * <p>
   * Note that base stream is closed if this stream is closed.
   * @param delegate specified base stream.
   */
  public FilterOutputStream2(OutputStream delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }
  
  @Override
  public void write(byte[] b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    delegate.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);
  }
}
