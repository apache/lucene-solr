package org.apache.lucene.queries;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ToStringUtils;

/**
 * Specialization for a disjunction over many terms that behaves like a
 * {@link ConstantScoreQuery} over a {@link BooleanQuery} containing only
 * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 * <p>For instance in the following example, both @{code q1} and {@code q2}
 * would yield the same scores:
 * <pre class="prettyprint">
 * Query q1 = new TermsQuery(new Term("field", "foo"), new Term("field", "bar"));
 * 
 * BooleanQuery bq = new BooleanQuery();
 * bq.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
 * bq.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);
 * Query q2 = new ConstantScoreQuery(bq);
 * </pre>
 * <p>This query creates a bit set and sets bits that match any of the
 * wrapped terms. While this might help performance when there are many terms,
 * it would be slower than a {@link BooleanQuery} when there are few terms to
 * match.
 * <p>NOTE: This query produces scores that are equal to its boost
 */
public class TermsQuery extends Query implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TermsQuery.class);

  private final PrefixCodedTerms termData;
  private final int termDataHashCode; // cached hashcode of termData

  private static Term[] toTermArray(String field, List<BytesRef> termBytes) {
    Term[] array = new Term[termBytes.size()];
    int i = 0;
    for (BytesRef t : termBytes) {
      array[i++] = new Term(field, t);
    }
    return array;
  }

  /**
   * Creates a new {@link TermsQuery} from the given list. The list
   * can contain duplicate terms and multiple fields.
   */
  public TermsQuery(final List<Term> terms) {
    Term[] sortedTerms = terms.toArray(new Term[terms.size()]);
    ArrayUtil.timSort(sortedTerms);
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    Term previous = null;
    for (Term term : sortedTerms) {
      if (term.equals(previous) == false) {
        builder.add(term);
      }
      previous = term;
    }
    termData = builder.finish();
    termDataHashCode = termData.hashCode();
  }

  /**
   * Creates a new {@link TermsQuery} from the given {@link BytesRef} list for
   * a single field.
   */
  public TermsQuery(final String field, final List<BytesRef> terms) {
    this(toTermArray(field, terms));
  }

  /**
   * Creates a new {@link TermsQuery} from the given {@link BytesRef} array for
   * a single field.
   */
  public TermsQuery(final String field, final BytesRef...terms) {
    // this ctor prevents unnecessary Term creations
   this(field, Arrays.asList(terms));
  }

  /**
   * Creates a new {@link TermsQuery} from the given array. The array can
   * contain duplicate terms and multiple fields.
   */
  public TermsQuery(final Term... terms) {
    this(Arrays.asList(terms));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    TermsQuery that = (TermsQuery) obj;
    // termData might be heavy to compare so check the hash code first
    return termDataHashCode == that.termDataHashCode
        && termData.equals(that.termData);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + termDataHashCode;
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    TermIterator iterator = termData.iterator();
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if (!first) {
        builder.append(' ');
      }
      first = false;
      builder.append(iterator.field()).append(':');
      builder.append(term.utf8ToString());
    }
    builder.append(ToStringUtils.boost(getBoost()));

    return builder.toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + termData.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {

      @Override
      public void extractTerms(Set<Term> terms) {
        // no-op
        // This query is for abuse cases when the number of terms is too high to
        // run efficiently as a BooleanQuery. So likewise we hide its terms in
        // order to protect highlighters
      }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final LeafReader reader = context.reader();
        BitDocIdSet.Builder builder = new BitDocIdSet.Builder(reader.maxDoc());
        final Fields fields = reader.fields();
        String lastField = null;
        Terms terms = null;
        TermsEnum termsEnum = null;
        PostingsEnum docs = null;
        TermIterator iterator = termData.iterator();
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
          String field = iterator.field();
          // comparing references is fine here
          if (field != lastField) {
            terms = fields.terms(field);
            if (terms == null) {
              termsEnum = null;
            } else {
              termsEnum = terms.iterator();
            }
          }
          if (termsEnum != null && termsEnum.seekExact(term)) {
            docs = termsEnum.postings(acceptDocs, docs, PostingsEnum.NONE);
            builder.or(docs);
          }
        }
        BitDocIdSet result = builder.build();
        if (result == null) {
          return null;
        }

        final DocIdSetIterator disi = result.iterator();
        if (disi == null) {
          return null;
        }

        return new ConstantScoreScorer(this, score(), disi);
      }
    };
  }
}
