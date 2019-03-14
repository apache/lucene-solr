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
import java.util.Set;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.PriorityQueue;

/**
 * Holds all implementations of classes in the o.a.l.search package as a
 * back-compatibility test. It does not run any tests per-se, however if
 * someone adds a method to an interface or abstract method to an abstract
 * class, one of the implementations here will fail to compile and so we know
 * back-compat policy was violated.
 */
final class JustCompileSearch {

  private static final String UNSUPPORTED_MSG = "unsupported: used for back-compat testing only !";

  static final class JustCompileCollector extends SimpleCollector {

    @Override
    public void collect(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void setScorer(Scorable scorer) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public ScoreMode scoreMode() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileDocIdSet extends DocIdSet {

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long ramBytesUsed() {
      return 0L;
    }
  }

  static final class JustCompileDocIdSetIterator extends DocIdSetIterator {

    @Override
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileFieldComparator extends FieldComparator<Object> {

    @Override
    public void setTopValue(Object value) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public Object value(int slot) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int compare(int slot1, int slot2) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileFieldComparatorSource extends FieldComparatorSource {

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits,
        int sortPos, boolean reversed) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

  static final class JustCompileFilteredDocIdSetIterator extends FilteredDocIdSetIterator {

    public JustCompileFilteredDocIdSetIterator(DocIdSetIterator innerIter) {
      super(innerIter);
    }

    @Override
    protected boolean match(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileQuery extends Query {

    @Override
    public String toString(String field) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public boolean equals(Object obj) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileScorer extends Scorer {

    protected JustCompileScorer(Weight weight) {
      super(weight);
    }

    @Override
    public float score() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileSimilarity extends Similarity {

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long computeNorm(FieldInvertState state) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileTopDocsCollector extends TopDocsCollector<ScoreDoc> {

    protected JustCompileTopDocsCollector(PriorityQueue<ScoreDoc> pq) {
      super(pq);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }

    @Override
    public TopDocs topDocs() {
        throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }

    @Override
    public TopDocs topDocs( int start ) {
        throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }

    @Override
    public TopDocs topDocs( int start, int end ) {
        throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }

    @Override
    public ScoreMode scoreMode() {
      throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }
  }

  static final class JustCompileWeight extends Weight {

    protected JustCompileWeight() {
      super(null);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

}
