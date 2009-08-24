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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
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

  static final class JustCompileSearcher extends Searcher {

    protected Weight createWeight(Query query) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public void close() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Document doc(int i) throws CorruptIndexException, IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public int[] docFreqs(Term[] terms) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Explanation explain(Query query, int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Similarity getSimilarity() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void search(Query query, Collector results) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void search(Query query, Filter filter, Collector results)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public TopDocs search(Query query, Filter filter, int n) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public TopFieldDocs search(Query query, Filter filter, int n, Sort sort)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public TopDocs search(Query query, int n) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public void setSimilarity(Similarity similarity) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public int docFreq(Term term) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Explanation explain(Weight weight, int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public int maxDoc() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Query rewrite(Query query) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void search(Weight weight, Filter filter, Collector results)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public TopDocs search(Weight weight, Filter filter, int n)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public TopFieldDocs search(Weight weight, Filter filter, int n, Sort sort)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Document doc(int n, FieldSelector fieldSelector)
        throws CorruptIndexException, IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
  static final class JustCompileCollector extends Collector {

    public void collect(int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void setNextReader(IndexReader reader, int docBase)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void setScorer(Scorer scorer) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public boolean acceptsDocsOutOfOrder() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }
  
  static final class JustCompileDocIdSet extends DocIdSet {

    public DocIdSetIterator iterator() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileDocIdSetIterator extends DocIdSetIterator {

    /** @deprecated delete in 3.0 */
    public int doc() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** @deprecated delete in 3.0 */
    public boolean next() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** @deprecated delete in 3.0 */
    public boolean skipTo(int target) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }
  
  static final class JustCompileExtendedFieldCacheLongParser implements FieldCache.LongParser {

    public long parseLong(String string) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
  static final class JustCompileExtendedFieldCacheDoubleParser implements FieldCache.DoubleParser {
    
    public double parseDouble(String string) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFieldComparator extends FieldComparator {

    public int compare(int slot1, int slot2) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public int compareBottom(int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void copy(int slot, int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void setBottom(int slot) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void setNextReader(IndexReader reader, int docBase)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Comparable value(int slot) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFieldComparatorSource extends FieldComparatorSource {

    public FieldComparator newComparator(String fieldname, int numHits,
        int sortPos, boolean reversed) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFilter extends Filter {
    // Filter is just an abstract class with no abstract methods. However it is
    // still added here in case someone will add abstract methods in the future.
  }

  static final class JustCompileFilteredDocIdSet extends FilteredDocIdSet {

    public JustCompileFilteredDocIdSet(DocIdSet innerSet) {
      super(innerSet);
    }

    protected boolean match(int docid) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFilteredDocIdSetIterator extends FilteredDocIdSetIterator {

    public JustCompileFilteredDocIdSetIterator(DocIdSetIterator innerIter) {
      super(innerIter);
    }

    protected boolean match(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFilteredTermEnum extends FilteredTermEnum {

    public float difference() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    protected boolean endEnum() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    protected boolean termCompare(Term term) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileMultiTermQuery extends MultiTermQuery {

    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompilePhraseScorer extends PhraseScorer {

    JustCompilePhraseScorer(Weight weight, TermPositions[] tps, int[] offsets,
        Similarity similarity, byte[] norms) {
      super(weight, tps, offsets, similarity, norms);
    }

    protected float phraseFreq() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileQuery extends Query {

    public String toString(String field) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
  static final class JustCompileScorer extends Scorer {

    protected JustCompileScorer(Similarity similarity) {
      super(similarity);
    }

    protected boolean score(Collector collector, int max, int firstDocID)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public Explanation explain(int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float score() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** @deprecated delete in 3.0 */
    public int doc() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** @deprecated delete in 3.0. */
    public boolean next() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** @deprecated delete in 3.0. */
    public boolean skipTo(int target) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }
  
  static final class JustCompileSimilarity extends Similarity {

    public float coord(int overlap, int maxOverlap) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float idf(int docFreq, int numDocs) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float lengthNorm(String fieldName, int numTokens) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float queryNorm(float sumOfSquaredWeights) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float sloppyFreq(int distance) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float tf(float freq) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileSpanFilter extends SpanFilter {

    public SpanFilterResult bitSpans(IndexReader reader) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileTopDocsCollector extends TopDocsCollector {

    protected JustCompileTopDocsCollector(PriorityQueue pq) {
      super(pq);
    }

    public void collect(int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void setNextReader(IndexReader reader, int docBase)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void setScorer(Scorer scorer) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    public boolean acceptsDocsOutOfOrder() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

  static final class JustCompileWeight extends Weight {

    public Explanation explain(IndexReader reader, int doc) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Query getQuery() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float getValue() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public void normalize(float norm) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** @deprecated delete in 3.0 */
    public Scorer scorer(IndexReader reader) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public float sumOfSquaredWeights() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
}
