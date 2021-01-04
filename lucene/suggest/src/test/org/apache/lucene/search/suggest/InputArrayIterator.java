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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** A {@link InputIterator} over a sequence of {@link Input}s. */
public final class InputArrayIterator implements InputIterator {
  private final Iterator<Input> i;
  private final boolean hasPayloads;
  private final boolean hasContexts;
  private boolean first;
  private Input current;
  private final BytesRefBuilder spare = new BytesRefBuilder();

  public InputArrayIterator(Iterator<Input> i) {
    this.i = i;
    if (i.hasNext()) {
      current = i.next();
      first = true;
      this.hasPayloads = current.hasPayloads;
      this.hasContexts = current.hasContexts;
    } else {
      this.hasPayloads = false;
      this.hasContexts = false;
    }
  }

  public InputArrayIterator(Input[] i) {
    this(Arrays.asList(i));
  }

  public InputArrayIterator(Iterable<Input> i) {
    this(i.iterator());
  }

  @Override
  public long weight() {
    return current.v;
  }

  @Override
  public BytesRef next() {
    if (i.hasNext() || (first && current != null)) {
      if (first) {
        first = false;
      } else {
        current = i.next();
      }
      spare.copyBytes(current.term);
      return spare.get();
    }
    return null;
  }

  @Override
  public BytesRef payload() {
    return current.payload;
  }

  @Override
  public boolean hasPayloads() {
    return hasPayloads;
  }

  @Override
  public Set<BytesRef> contexts() {
    return current.contexts;
  }

  @Override
  public boolean hasContexts() {
    return hasContexts;
  }
}
