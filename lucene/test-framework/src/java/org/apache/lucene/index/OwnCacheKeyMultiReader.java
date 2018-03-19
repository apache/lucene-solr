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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.util.IOUtils;

/**
 * A {@link MultiReader} that has its own cache key, occasionally useful for
 * testing purposes.
 */
public final class OwnCacheKeyMultiReader extends MultiReader {

  private final Set<ClosedListener> readerClosedListeners = new CopyOnWriteArraySet<>();

  private final CacheHelper cacheHelper = new CacheHelper() {
    private final CacheKey cacheKey = new CacheKey();

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      ensureOpen();
      readerClosedListeners.add(listener);
    }

  };

  /** Sole constructor. */
  public OwnCacheKeyMultiReader(IndexReader... subReaders) throws IOException {
    super(subReaders);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return cacheHelper;
  }

  @Override
  void notifyReaderClosedListeners() throws IOException {
    synchronized(readerClosedListeners) {
      IOUtils.applyToAll(readerClosedListeners, l -> l.onClose(cacheHelper.getKey()));
    }
  }

}
