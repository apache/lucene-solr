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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;


/**
 * A query that applies a filter to the results of another query.
 *
 * <p>Note: the bits are retrieved from the filter each time this
 * query is used in a search - use a CachingWrapperFilter to avoid
 * regenerating the bits every time.
 *
 * <p>Created: Apr 20, 2004 8:58:29 AM
 *
 * @since   1.4
 * @see     CachingWrapperFilter
 */
public class FilteredQuery
extends Query {

  Query query;
  Filter filter;

  /**
   * Constructs a new query which applies a filter to the results of the original query.
   * Filter.getDocIdSet() will be called every time this query is used in a search.
   * @param query  Query to be filtered, cannot be <code>null</code>.
   * @param filter Filter to apply to query results, cannot be <code>null</code>.
   */
  public FilteredQuery (Query query, Filter filter) {
    this.query = query;
    this.filter = filter;
  }
  
  /**
   * Expert: decides if a filter should be executed as "random-access" or not.
   * random-access means the filter "filters" in a similar way as deleted docs are filtered
   * in lucene. This is faster when the filter accepts many documents.
   * However, when the filter is very sparse, it can be faster to execute the query+filter
   * as a conjunction in some cases.
   * 
   * The default implementation returns true if the first document accepted by the
   * filter is < 100.
   * 
   * @lucene.internal
   */
  protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
    return firstFilterDoc < 100;
  }

  /**
   * Returns a Weight that applies the filter to the enclosed query's Weight.
   * This is accomplished by overriding the Scorer returned by the Weight.
   */
  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    final Weight weight = query.createWeight (searcher);
    return new Weight() {
      
      @Override
      public boolean scoresDocsOutOfOrder() {
        // TODO: Support out-of-order scoring!
        // For now we return false here, as we always get the scorer in order
        return false;
      }

      @Override
      public float getValueForNormalization() throws IOException { 
        return weight.getValueForNormalization() * getBoost() * getBoost(); // boost sub-weight
      }

      @Override
      public void normalize (float norm, float topLevelBoost) { 
        weight.normalize(norm, topLevelBoost * getBoost()); // incorporate boost
      }

      @Override
      public Explanation explain (AtomicReaderContext ir, int i) throws IOException {
        Explanation inner = weight.explain (ir, i);
        Filter f = FilteredQuery.this.filter;
        DocIdSet docIdSet = f.getDocIdSet(ir, ir.reader.getLiveDocs());
        DocIdSetIterator docIdSetIterator = docIdSet == null ? DocIdSet.EMPTY_DOCIDSET.iterator() : docIdSet.iterator();
        if (docIdSetIterator == null) {
          docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.iterator();
        }
        if (docIdSetIterator.advance(i) == i) {
          return inner;
        } else {
          Explanation result = new Explanation
            (0.0f, "failure to match filter: " + f.toString());
          result.addDetail(inner);
          return result;
        }
      }

      // return this query
      @Override
      public Query getQuery() { return FilteredQuery.this; }

      // return a filtering scorer
      @Override
      public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
        assert filter != null;

        final DocIdSet filterDocIdSet = filter.getDocIdSet(context, acceptDocs);
        if (filterDocIdSet == null) {
          // this means the filter does not accept any documents.
          return null;
        }
        
        final DocIdSetIterator filterIter = filterDocIdSet.iterator();
        if (filterIter == null) {
          // this means the filter does not accept any documents.
          return null;
        }

        final int firstFilterDoc = filterIter.nextDoc();
        if (firstFilterDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return null;
        }
        
        final Bits filterAcceptDocs = filterDocIdSet.bits();
        final boolean useRandomAccess = (filterAcceptDocs != null && FilteredQuery.this.useRandomAccess(filterAcceptDocs, firstFilterDoc));

        if (useRandomAccess) {
          // if we are using random access, we return the inner scorer, just with other acceptDocs
          // TODO, replace this by when BooleanWeight is fixed to be consistent with its scorer implementations:
          // return weight.scorer(context, scoreDocsInOrder, topScorer, filterAcceptDocs);
          return weight.scorer(context, true, topScorer, filterAcceptDocs);
        } else {
          assert firstFilterDoc > -1;
          // we are gonna advance() this scorer, so we set inorder=true/toplevel=false
          // we pass null as acceptDocs, as our filter has already respected acceptDocs, no need to do twice
          final Scorer scorer = weight.scorer(context, true, false, null);
          return (scorer == null) ? null : new Scorer(this) {
            private int scorerDoc = -1, filterDoc = firstFilterDoc;
            
            // optimization: we are topScorer and collect directly using short-circuited algo
            @Override
            public void score(Collector collector) throws IOException {
              int filterDoc = firstFilterDoc;
              int scorerDoc = scorer.advance(filterDoc);
              // the normalization trick already applies the boost of this query,
              // so we can use the wrapped scorer directly:
              collector.setScorer(scorer);
              for (;;) {
                if (scorerDoc == filterDoc) {
                  // Check if scorer has exhausted, only before collecting.
                  if (scorerDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                  }
                  collector.collect(scorerDoc);
                  filterDoc = filterIter.nextDoc();
                  scorerDoc = scorer.advance(filterDoc);
                } else if (scorerDoc > filterDoc) {
                  filterDoc = filterIter.advance(scorerDoc);
                } else {
                  scorerDoc = scorer.advance(filterDoc);
                }
              }
            }
            
            private int advanceToNextCommonDoc() throws IOException {
              for (;;) {
                if (scorerDoc < filterDoc) {
                  scorerDoc = scorer.advance(filterDoc);
                } else if (scorerDoc == filterDoc) {
                  return scorerDoc;
                } else {
                  filterDoc = filterIter.advance(scorerDoc);
                }
              }
            }

            @Override
            public int nextDoc() throws IOException {
              // don't go to next doc on first call
              // (because filterIter is already on first doc):
              if (scorerDoc != -1) {
                filterDoc = filterIter.nextDoc();
              }
              return advanceToNextCommonDoc();
            }
            
            @Override
            public int advance(int target) throws IOException {
              if (target > filterDoc) {
                filterDoc = filterIter.advance(target);
              }
              return advanceToNextCommonDoc();
            }

            @Override
            public int docID() {
              return scorerDoc;
            }
            
            @Override
            public float score() throws IOException {
              return scorer.score();
            }
          };
        }
      }
    };
  }

  /** Rewrites the wrapped query. */
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = query.rewrite(reader);
    if (rewritten != query) {
      FilteredQuery clone = (FilteredQuery)this.clone();
      clone.query = rewritten;
      return clone;
    } else {
      return this;
    }
  }

  public Query getQuery() {
    return query;
  }

  public Filter getFilter() {
    return filter;
  }

  // inherit javadoc
  @Override
  public void extractTerms(Set<Term> terms) {
      getQuery().extractTerms(terms);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString (String s) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("filtered(");
    buffer.append(query.toString(s));
    buffer.append(")->");
    buffer.append(filter);
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (o instanceof FilteredQuery) {
      FilteredQuery fq = (FilteredQuery) o;
      return (query.equals(fq.query) && filter.equals(fq.filter) && getBoost()==fq.getBoost());
    }
    return false;
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return query.hashCode() ^ filter.hashCode() + Float.floatToRawIntBits(getBoost());
  }
}
