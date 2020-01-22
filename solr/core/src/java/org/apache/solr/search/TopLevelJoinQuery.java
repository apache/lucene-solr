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

package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.join.MVTermOrdinalCollector;
import org.apache.solr.search.join.SVTermOrdinalCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopLevelJoinQuery extends JoinQuery {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TopLevelJoinQuery(String fromField, String toField, String coreName, Query subQuery) {
    super(fromField, toField, coreName, subQuery);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    if (! (searcher instanceof SolrIndexSearcher)) {
      log.debug("Falling back to JoinQueryWeight because searcher [{}] is not the required SolrIndexSearcher", searcher);
      return super.createWeight(searcher, scoreMode, boost);
    }

    final SolrIndexSearcher solrSearcher = (SolrIndexSearcher) searcher;
    final JoinQueryWeight weight = new JoinQueryWeight(solrSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    final SolrIndexSearcher fromSearcher = weight.fromSearcher;
    final SolrIndexSearcher toSearcher = weight.toSearcher;

    try {
      final DocValuesWrapper topLevelFromDocValues = validateAndFetchDocValues(fromSearcher, fromField, "from");
      final DocValuesWrapper topLevelToDocValues = validateAndFetchDocValues(toSearcher, toField, "to");
      if (topLevelFromDocValues == null || topLevelToDocValues == null) {
        return createNoMatchesWeight(boost);
      }

      final LongBitSet fromOrdBitSet = findOrdinalsMatchingFromQuery(fromSearcher, topLevelFromDocValues);
      final LongBitSet toOrdBitSet = new LongBitSet(topLevelToDocValues.getValueCount());
      final BitsetBounds toBitsetBounds = convertFromOrdinalsIntoToField(fromOrdBitSet, topLevelFromDocValues, toOrdBitSet, topLevelToDocValues);

      final boolean toMultivalued = toSearcher.getSchema().getFieldOrNull(toField).multiValued();
      return new ConstantScoreWeight(this, boost) {
        public Scorer scorer(LeafReaderContext context) throws IOException {
          if (toBitsetBounds.lower == BitsetBounds.NO_MATCHES) {
            return null;
          }

          final DocIdSetIterator toApproximation = (toMultivalued) ? context.reader().getSortedSetDocValues(toField) :
              context.reader().getSortedDocValues(toField);
          if (toApproximation == null) {
            return null;
          }

          final int docBase = context.docBase;
          return new ConstantScoreScorer(this, this.score(), scoreMode, new TwoPhaseIterator(toApproximation) {
            public boolean matches() throws IOException {
              final boolean hasDoc = topLevelToDocValues.advanceExact(docBase + approximation.docID());
              if (hasDoc) {
                if (topLevelToDocValues.currentPositionMatchesOrdinals(toOrdBitSet)) {
                  return true;
                }
              }
              return false;
            }

            public float matchCost() {
              return 10.0F;
            }
          });

        }

        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Weight createNoMatchesWeight(float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return null;
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  private DocValuesWrapper validateAndFetchDocValues(SolrIndexSearcher solrSearcher, String fieldName, String querySide) throws IOException {
    final IndexSchema schema = solrSearcher.getSchema();
    final SchemaField field = schema.getFieldOrNull(fieldName);
    if (field == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, querySide + " field '" + fieldName + "' does not exist");
    }

    if (!field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "'top-level' join queries require both 'from' and 'to' fields to have docValues, but " + querySide +
              " field [" + fieldName +  "] does not.");
    }


    final DocValuesWrapper wrapper = (field.multiValued()) ?
        new DocValuesWrapper(DocValues.getSortedSet(solrSearcher.getSlowAtomicReader(), fieldName)) :
        new DocValuesWrapper(DocValues.getSorted(solrSearcher.getSlowAtomicReader(), fieldName));
    if (wrapper.getValueCount() == 0) {
      return null;
    }
    return wrapper;
  }

  private LongBitSet findOrdinalsMatchingFromQuery(SolrIndexSearcher fromSearcher, DocValuesWrapper fromDocValues) throws IOException {
    final LongBitSet fromOrdBitSet = new LongBitSet(fromDocValues.getValueCount());
    final Collector fromCollector = fromDocValues.isMultivalued() ?
        new MVTermOrdinalCollector(fromField, fromDocValues.getSortedSetDocValues(), fromOrdBitSet) :
        new SVTermOrdinalCollector(fromField, fromDocValues.getSortedDocValues(), fromOrdBitSet);

    fromSearcher.search(q, fromCollector);

    return fromOrdBitSet;
  }

  private BitsetBounds convertFromOrdinalsIntoToField(LongBitSet fromOrdBitSet, DocValuesWrapper fromDocValuesWrapper,
                                                      LongBitSet toOrdBitSet, DocValuesWrapper toValues) throws IOException {
    long fromOrdinal = 0;
    long firstToOrd = BitsetBounds.NO_MATCHES;
    long lastToOrd = 0;

    while (fromOrdinal < fromOrdBitSet.length() && (fromOrdinal = fromOrdBitSet.nextSetBit(fromOrdinal)) >= 0) {
      final BytesRef fromBytesRef = fromDocValuesWrapper.lookupOrd(fromOrdinal);
      final long toOrdinal = lookupTerm(toValues, fromBytesRef, lastToOrd);
      if (toOrdinal >= 0) {
        toOrdBitSet.set(toOrdinal);
        if (firstToOrd == BitsetBounds.NO_MATCHES) firstToOrd = toOrdinal;
        lastToOrd = toOrdinal;
      }
      fromOrdinal++;
    }

      return new BitsetBounds(firstToOrd, lastToOrd);
  }

  /*
   * Same binary-search based implementation as SortedSetDocValues.lookupTerm(BytesRef), but with an
   * optimization to narrow the search space where possible by providing a startOrd instead of beginning each search
   * at 0.
   */
  private long lookupTerm(DocValuesWrapper docValues, BytesRef key, long startOrd) throws IOException {
    long low = startOrd;
    long high = docValues.getValueCount()-1;

    while (low <= high) {
      long mid = (low + high) >>> 1;
      final BytesRef term = docValues.lookupOrd(mid);
      int cmp = term.compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }

  /**
   * {@Link SortedDocValues} and {@link SortedSetDocValues} share several methods with identical signatures and semantics.
   *
   * But since these methods don't belong to any shared interface or parent class, there's no type declaration that
   * holds objects of both types and allows these methods to be invoked.  This makes it tricky to write client code using
   * these objects without a lot of conditionals and casting.
   *
   * This class wraps one or the other of these objects and provides access to some of their shared methods.
   */
  class DocValuesWrapper {
    SortedSetDocValues docValuesSet;
    SortedDocValues docValues;
    boolean multivalued;

    public DocValuesWrapper(SortedSetDocValues docValuesSet) { this.docValuesSet = docValuesSet; this.multivalued = true;}
    public DocValuesWrapper(SortedDocValues docValues) { this.docValues = docValues; }

    public BytesRef lookupOrd(long fromOrdinal) throws IOException {
      return docValues == null ? docValuesSet.lookupOrd(fromOrdinal) : docValues.lookupOrd((int) fromOrdinal);
    }

    public long getValueCount() {
      return docValues == null ? docValuesSet.getValueCount() : docValues.getValueCount();
    }

    public boolean isMultivalued() {
      return multivalued;
    }

    public SortedSetDocValues getSortedSetDocValues() { return docValuesSet; }
    public SortedDocValues getSortedDocValues() { return docValues; }
    public boolean advanceExact(int target) throws IOException {
      if (multivalued) {
        return docValuesSet.advanceExact(target);
      }
      return docValues.advanceExact(target);
    }

    public boolean currentPositionMatchesOrdinals(LongBitSet ordinalsToMatch) throws IOException {
      if (multivalued) {
        for (long ord = docValuesSet.nextOrd(); ord != -1L; ord = docValuesSet.nextOrd()) {
          if (ordinalsToMatch.get(ord)) {
            return true;
          }
        }
      } else {
        return ordinalsToMatch.get(docValues.ordValue());
      }

      return false;
    }
  }

  private static class BitsetBounds {
    public static final long NO_MATCHES = -1L;
    public final long lower;
    public final long upper;

    public BitsetBounds(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }
}
