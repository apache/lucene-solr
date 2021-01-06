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
package org.apache.lucene.search.suggest;

import java.util.Comparator;
import org.apache.lucene.search.suggest.fst.BytesRefSorter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;

/**
 * An {@link BytesRefSorter} that keeps all the entries in memory.
 *
 * @lucene.experimental
 * @lucene.internal
 */
public final class InMemorySorter implements BytesRefSorter {
  private final BytesRefArray buffer = new BytesRefArray(Counter.newCounter());
  private boolean closed = false;
  private final Comparator<BytesRef> comparator;

  /** Creates an InMemorySorter, sorting entries by the provided comparator. */
  public InMemorySorter(Comparator<BytesRef> comparator) {
    this.comparator = comparator;
  }

  @Override
  public void add(BytesRef utf8) {
    if (closed) throw new IllegalStateException();
    buffer.append(utf8);
  }

  @Override
  public BytesRefIterator iterator() {
    closed = true;
    return buffer.iterator(comparator);
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }
}
