package org.apache.lucene.search.suggest.fst;

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

import java.util.*;

import org.apache.lucene.util.BytesRef;

/**
 * An {@link BytesRefSorter} that keeps all the entries in memory.
 */
public final class InMemorySorter implements BytesRefSorter {
  // TODO: use a single byte[] to back up all entries?
  private final ArrayList<BytesRef> refs = new ArrayList<BytesRef>();
  
  private boolean closed = false;

  @Override
  public void add(BytesRef utf8) {
    if (closed) throw new IllegalStateException();
    refs.add(BytesRef.deepCopyOf(utf8));
  }

  @Override
  public Iterator<BytesRef> iterator() {
    closed = true;
    Collections.sort(refs, BytesRef.getUTF8SortedAsUnicodeComparator());
    return Collections.unmodifiableCollection(refs).iterator();
  }
}
