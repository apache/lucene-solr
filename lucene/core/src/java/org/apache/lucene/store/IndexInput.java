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

import java.io.Closeable;
import java.io.IOException;

/** Abstract base class for input from a file in a {@link Directory}.  A
 * random-access input stream.  Used for all Lucene index input operations.
 *
 * <p>{@code IndexInput} may only be used from one thread, because it is not
 * thread safe (it keeps internal state like file position). To allow
 * multithreaded use, every {@code IndexInput} instance must be cloned before
 * used in another thread. Subclasses must therefore implement {@link #clone()},
 * returning a new {@code IndexInput} which operates on the same underlying
 * resource, but positioned independently. Lucene never closes cloned
 * {@code IndexInput}s, it will only do this on the original one.
 * The original instance must take care that cloned instances throw
 * {@link AlreadyClosedException} when the original one is closed.
 
 * @see Directory
 */
public abstract class IndexInput extends DataInput implements Cloneable,Closeable {

  private final String resourceDescription;

  /** resourceDescription should be a non-null, opaque string
   *  describing this resource; it's returned from
   *  {@link #toString}. */
  protected IndexInput(String resourceDescription) {
    if (resourceDescription == null) {
      throw new IllegalArgumentException("resourceDescription must not be null");
    }
    this.resourceDescription = resourceDescription;
  }

  /** Closes the stream to further operations. */
  @Override
  public abstract void close() throws IOException;

  /** Returns the current position in this file, where the next read will
   * occur.
   * @see #seek(long)
   */
  public abstract long getFilePointer();

  /** Sets current position in this file, where the next read will occur.
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /** The number of bytes in the file. */
  public abstract long length();

  @Override
  public String toString() {
    return resourceDescription;
  }
  
  /** {@inheritDoc}
   * <p><b>Warning:</b> Lucene never closes cloned
   * {@code IndexInput}s, it will only do this on the original one.
   * The original instance must take care that cloned instances throw
   * {@link AlreadyClosedException} when the original one is closed.
   */
  @Override
  public IndexInput clone() {
    return (IndexInput) super.clone();
  }
}
