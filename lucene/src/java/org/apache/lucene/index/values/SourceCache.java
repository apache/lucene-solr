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
import java.util.Comparator;

import org.apache.lucene.index.values.DocValues.SortedSource;
import org.apache.lucene.index.values.DocValues.Source;
import org.apache.lucene.util.BytesRef;

/**
 * Per {@link DocValues} {@link Source} cache.
 * @lucene.experimental
 */
public abstract class SourceCache {
  public abstract Source load(DocValues values) throws IOException;

  public abstract SortedSource loadSorted(DocValues values,
      Comparator<BytesRef> comp) throws IOException;

  public abstract void invalidate(DocValues values);

  public synchronized void close(DocValues values) {
    invalidate(values);
  }

  public static final class DirectSourceCache extends SourceCache {
    private Source ref;
    private SortedSource sortedRef;

    public synchronized Source load(DocValues values) throws IOException {
      if (ref == null)
        ref = values.load();
      return ref;
    }

    public synchronized SortedSource loadSorted(DocValues values,
        Comparator<BytesRef> comp) throws IOException {
      if (sortedRef == null)
        sortedRef = values.loadSorted(comp);
      return sortedRef;
    }

    public synchronized void invalidate(DocValues values) {
      ref = null;
      sortedRef = null;
    }
  }

}
