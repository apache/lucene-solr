package org.apache.lucene.index.values;

/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.values.IndexDocValues.Source;

/**
 * Abstract base class for {@link IndexDocValues} {@link Source} cache.
 * <p>
 * {@link Source} instances loaded via {@link IndexDocValues#load()} are entirely memory resident
 * and need to be maintained by the caller. Each call to
 * {@link IndexDocValues#load()} will cause an entire reload of
 * the underlying data. Source instances obtained from
 * {@link IndexDocValues#getSource()} and {@link IndexDocValues#getSource()}
 * respectively are maintained by a {@link SourceCache} that is closed (
 * {@link #close(IndexDocValues)}) once the {@link IndexReader} that created the
 * {@link IndexDocValues} instance is closed.
 * <p>
 * Unless {@link Source} instances are managed by another entity it is
 * recommended to use the cached variants to obtain a source instance.
 * <p>
 * Implementation of this API must be thread-safe.
 * 
 * @see IndexDocValues#setCache(SourceCache)
 * @see IndexDocValues#getSource()
 * 
 * @lucene.experimental
 */
public abstract class SourceCache {

  /**
   * Atomically loads a {@link Source} into the cache from the given
   * {@link IndexDocValues} and returns it iff no other {@link Source} has already
   * been cached. Otherwise the cached source is returned.
   * <p>
   * This method will not return <code>null</code>
   */
  public abstract Source load(IndexDocValues values) throws IOException;

  /**
   * Atomically invalidates the cached {@link Source} 
   * instances if any and empties the cache.
   */
  public abstract void invalidate(IndexDocValues values);

  /**
   * Atomically closes the cache and frees all resources.
   */
  public synchronized void close(IndexDocValues values) {
    invalidate(values);
  }

  /**
   * Simple per {@link IndexDocValues} instance cache implementation that holds a
   * {@link Source} a member variable.
   * <p>
   * If a {@link DirectSourceCache} instance is closed or invalidated the cached
   * reference are simply set to <code>null</code>
   */
  public static final class DirectSourceCache extends SourceCache {
    private Source ref;

    public synchronized Source load(IndexDocValues values) throws IOException {
      if (ref == null) {
        ref = values.load();
      }
      return ref;
    }

    public synchronized void invalidate(IndexDocValues values) {
      ref = null;
    }
  }

}
