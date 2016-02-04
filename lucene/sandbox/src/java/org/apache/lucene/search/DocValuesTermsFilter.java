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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link Filter} that only accepts documents whose single
 * term value in the specified field is contained in the
 * provided set of allowed terms.
 * 
 * <p>
 * This is the same functionality as TermsFilter (from
 * queries/), except this filter requires that the
 * field contains only a single term for all documents.
 * Because of drastically different implementations, they
 * also have different performance characteristics, as
 * described below.
 * 
 * <p>
 * With each search, this filter translates the specified
 * set of Terms into a private {@link FixedBitSet} keyed by
 * term number per unique {@link IndexReader} (normally one
 * reader per segment).  Then, during matching, the term
 * number for each docID is retrieved from the cache and
 * then checked for inclusion using the {@link FixedBitSet}.
 * Since all testing is done using RAM resident data
 * structures, performance should be very fast, most likely
 * fast enough to not require further caching of the
 * DocIdSet for each possible combination of terms.
 * However, because docIDs are simply scanned linearly, an
 * index with a great many small documents may find this
 * linear scan too costly.
 * 
 * <p>
 * In contrast, TermsFilter builds up an {@link FixedBitSet},
 * keyed by docID, every time it's created, by enumerating
 * through all matching docs using {@link org.apache.lucene.index.PostingsEnum} to seek
 * and scan through each term's docID list.  While there is
 * no linear scan of all docIDs, besides the allocation of
 * the underlying array in the {@link FixedBitSet}, this
 * approach requires a number of "disk seeks" in proportion
 * to the number of terms, which can be exceptionally costly
 * when there are cache misses in the OS's IO cache.
 * 
 * <p>
 * Generally, this filter will be slower on the first
 * invocation for a given field, but subsequent invocations,
 * even if you change the allowed set of Terms, should be
 * faster than TermsFilter, especially as the number of
 * Terms being matched increases.  If you are matching only
 * a very small number of terms, and those terms in turn
 * match a very small number of documents, TermsFilter may
 * perform faster.
 *
 * <p>
 * Which filter is best is very application dependent.
 * @deprecated Use {@link DocValuesTermsQuery} and boolean {@link BooleanClause.Occur#FILTER} clauses instead
 *
 * @lucene.experimental
 */
@Deprecated
public class DocValuesTermsFilter extends Filter {
  private String field;
  private BytesRef[] terms;

  public DocValuesTermsFilter(String field, BytesRef... terms) {
    this.field = field;
    this.terms = terms;
  }

  public DocValuesTermsFilter(String field, String... terms) {
    this.field = field;
    this.terms = new BytesRef[terms.length];
    for (int i = 0; i < terms.length; i++)
      this.terms[i] = new BytesRef(terms[i]);
  }

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    final SortedDocValues fcsi = DocValues.getSorted(context.reader(), field);
    final FixedBitSet bits = new FixedBitSet(fcsi.getValueCount());
    for (int i=0;i<terms.length;i++) {
      int ord = fcsi.lookupTerm(terms[i]);
      if (ord >= 0) {
        bits.set(ord);
      }
    }
    return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
      @Override
      protected final boolean matchDoc(int doc) {
        int ord = fcsi.getOrd(doc);
        if (ord == -1) {
          // missing
          return false;
        } else {
          return bits.get(ord);
        }
      }
    };
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder sb = new StringBuilder();
    sb.append(field).append(": [");
    for (BytesRef term : terms) {
      sb.append(term).append(", ");
    }
    if (terms.length > 0) {
      sb.setLength(sb.length() - 2);
    }
    return sb.append(']').toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj) == false) {
      return false;
    }
    DocValuesTermsFilter other = (DocValuesTermsFilter) obj;
    return field.equals(other.field) && Arrays.equals(terms, other.terms);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + field.hashCode();
    h = 31 * h + Arrays.hashCode(terms);
    return h;
  }
}
