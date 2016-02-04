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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * A {@link Filter} that produces {@link BitDocIdSet}s.
 * @deprecated Use {@link BitSetProducer} instead
 */
@Deprecated
public abstract class BitDocIdSetFilter extends Filter implements BitSetProducer {

  /** Sole constructor, typically called from sub-classes. */
  protected BitDocIdSetFilter() {}

  /**
   * Same as {@link #getDocIdSet(LeafReaderContext, Bits)} but does not take
   * acceptDocs into account and guarantees to return a {@link BitDocIdSet}.
   */
  public abstract BitDocIdSet getDocIdSet(LeafReaderContext context) throws IOException;

  @Override
  public final DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    return BitsFilteredDocIdSet.wrap(getDocIdSet(context), acceptDocs);
  }

  @Override
  public final BitSet getBitSet(LeafReaderContext context) throws IOException {
    final BitDocIdSet set = getDocIdSet(context);
    if (set == null) {
      return null;
    } else {
      final BitSet bits = set.bits();
      return Objects.requireNonNull(bits);
    }
  }

}
