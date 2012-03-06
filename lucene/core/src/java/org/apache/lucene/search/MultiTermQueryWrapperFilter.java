package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Bits;

/**
 * A wrapper for {@link MultiTermQuery}, that exposes its
 * functionality as a {@link Filter}.
 * <P>
 * <code>MultiTermQueryWrapperFilter</code> is not designed to
 * be used by itself. Normally you subclass it to provide a Filter
 * counterpart for a {@link MultiTermQuery} subclass.
 * <P>
 * For example, {@link TermRangeFilter} and {@link PrefixFilter} extend
 * <code>MultiTermQueryWrapperFilter</code>.
 * This class also provides the functionality behind
 * {@link MultiTermQuery#CONSTANT_SCORE_FILTER_REWRITE};
 * this is why it is not abstract.
 */
public class MultiTermQueryWrapperFilter<Q extends MultiTermQuery> extends Filter {
    
  protected final Q query;

  /**
   * Wrap a {@link MultiTermQuery} as a Filter.
   */
  protected MultiTermQueryWrapperFilter(Q query) {
      this.query = query;
  }
  
  @Override
  public String toString() {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString();
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (o==null) return false;
    if (this.getClass().equals(o.getClass())) {
      return this.query.equals( ((MultiTermQueryWrapperFilter)o).query );
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return query.hashCode();
  }

  /** Returns the field name for this query */
  public final String getField() { return query.getField(); }
  
  /**
   * Returns a DocIdSet with documents that should be permitted in search
   * results.
   */
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    final AtomicReader reader = context.reader();
    final Fields fields = reader.fields();
    if (fields == null) {
      // reader has no fields
      return DocIdSet.EMPTY_DOCIDSET;
    }

    final Terms terms = fields.terms(query.field);
    if (terms == null) {
      // field does not exist
      return DocIdSet.EMPTY_DOCIDSET;
    }

    final TermsEnum termsEnum = query.getTermsEnum(terms);
    assert termsEnum != null;
    if (termsEnum.next() != null) {
      // fill into a FixedBitSet
      final FixedBitSet bitSet = new FixedBitSet(context.reader().maxDoc());
      DocsEnum docsEnum = null;
      do {
        // System.out.println("  iter termCount=" + termCount + " term=" +
        // enumerator.term().toBytesString());
        docsEnum = termsEnum.docs(acceptDocs, docsEnum, false);
        int docid;
        while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          bitSet.set(docid);
        }
      } while (termsEnum.next() != null);
      // System.out.println("  done termCount=" + termCount);

      return bitSet;
    } else {
      return DocIdSet.EMPTY_DOCIDSET;
    }
  }
}
