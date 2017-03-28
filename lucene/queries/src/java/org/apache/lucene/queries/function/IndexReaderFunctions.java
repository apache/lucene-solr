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

package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

/**
 * Class exposing static helper methods for generating DoubleValuesSource instances
 * over some IndexReader statistics
 */
public final class IndexReaderFunctions {

  // non-instantiable class
  private IndexReaderFunctions() {}

  /**
   * Creates a constant value source returning the docFreq of a given term
   *
   * @see IndexReader#docFreq(Term)
   */
  public static DoubleValuesSource docFreq(Term term) {
    return new IndexReaderDoubleValuesSource(r -> (double) r.docFreq(term), "docFreq(" + term.toString() + ")");
  }

  /**
   * Creates a constant value source returning the index's maxDoc
   *
   * @see IndexReader#maxDoc()
   */
  public static DoubleValuesSource maxDoc() {
    return new IndexReaderDoubleValuesSource(IndexReader::maxDoc, "maxDoc()");
  }

  /**
   * Creates a constant value source returning the index's numDocs
   *
   * @see IndexReader#numDocs()
   */
  public static DoubleValuesSource numDocs() {
    return new IndexReaderDoubleValuesSource(IndexReader::numDocs, "numDocs()");
  }

  /**
   * Creates a constant value source returning the number of deleted docs in the index
   *
   * @see IndexReader#numDeletedDocs()
   */
  public static DoubleValuesSource numDeletedDocs() {
    return new IndexReaderDoubleValuesSource(IndexReader::numDeletedDocs, "numDeletedDocs()");
  }

  /**
   * Creates a constant value source returning the sumTotalTermFreq for a field
   *
   * @see IndexReader#getSumTotalTermFreq(String)
   */
  public static LongValuesSource sumTotalTermFreq(String field) {
    return new SumTotalTermFreqValuesSource(field);
  }

  private static class SumTotalTermFreqValuesSource extends LongValuesSource {

    private final String field;

    private SumTotalTermFreqValuesSource(String field) {
      this.field = field;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      throw new UnsupportedOperationException("IndexReaderFunction must be rewritten before use");
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SumTotalTermFreqValuesSource that = (SumTotalTermFreqValuesSource) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field);
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return new NoCacheConstantLongValuesSource(searcher.getIndexReader().getSumTotalTermFreq(field), this);
    }

    @Override
    public String toString() {
      return "sumTotalTermFreq(" + field + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class NoCacheConstantLongValuesSource extends LongValuesSource {

    final long value;
    final LongValuesSource parent;

    private NoCacheConstantLongValuesSource(long value, LongValuesSource parent) {
      this.value = value;
      this.parent = parent;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new LongValues() {
        @Override
        public long longValue() throws IOException {
          return value;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof NoCacheConstantLongValuesSource)) return false;
      NoCacheConstantLongValuesSource that = (NoCacheConstantLongValuesSource) o;
      return value == that.value &&
          Objects.equals(parent, that.parent);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, parent);
    }

    @Override
    public String toString() {
      return parent.toString();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  /**
   * Creates a value source that returns the term freq of a given term for each document
   *
   * @see PostingsEnum#freq()
   */
  public static DoubleValuesSource termFreq(Term term) {
    return new TermFreqDoubleValuesSource(term);
  }

  private static class TermFreqDoubleValuesSource extends DoubleValuesSource {

    private final Term term;

    private TermFreqDoubleValuesSource(Term term) {
      this.term = term;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      Terms terms = ctx.reader().terms(term.field());
      TermsEnum te = terms == null ? null : terms.iterator();

      if (te == null || te.seekExact(term.bytes()) == false) {
        return DoubleValues.EMPTY;
      }

      final PostingsEnum pe = te.postings(null);
      assert pe != null;

      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return pe.freq();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          if (pe.docID() > doc)
            return false;
          return pe.docID() == doc || pe.advance(doc) == doc;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public String toString() {
      return "termFreq(" + term.toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TermFreqDoubleValuesSource that = (TermFreqDoubleValuesSource) o;
      return Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
      return Objects.hash(term);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

  /**
   * Creates a constant value source returning the totalTermFreq for a given term
   *
   * @see IndexReader#totalTermFreq(Term)
   */
  public static DoubleValuesSource totalTermFreq(Term term) {
    return new IndexReaderDoubleValuesSource(r -> r.totalTermFreq(term), "totalTermFreq(" + term.toString() + ")");
  }

  /**
   * Creates a constant value source returning the sumDocFreq for a given field
   *
   * @see IndexReader#getSumDocFreq(String)
   */
  public static DoubleValuesSource sumDocFreq(String field) {
    return new IndexReaderDoubleValuesSource(r -> r.getSumDocFreq(field), "sumDocFreq(" + field + ")");
  }

  /**
   * Creates a constant value source returning the docCount for a given field
   *
   * @see IndexReader#getDocCount(String)
   */
  public static DoubleValuesSource docCount(String field) {
    return new IndexReaderDoubleValuesSource(r -> r.getDocCount(field), "docCount(" + field + ")");
  }

  @FunctionalInterface
  private interface ReaderFunction {
    double apply(IndexReader reader) throws IOException;
  }

  private static class IndexReaderDoubleValuesSource extends DoubleValuesSource {

    private final ReaderFunction func;
    private final String description;

    private IndexReaderDoubleValuesSource(ReaderFunction func, String description) {
      this.func = func;
      this.description = description;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      throw new UnsupportedOperationException("IndexReaderFunction must be rewritten before use");
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return new NoCacheConstantDoubleValuesSource(func.apply(searcher.getIndexReader()), this);
    }

    @Override
    public String toString() {
      return description;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IndexReaderDoubleValuesSource that = (IndexReaderDoubleValuesSource) o;
      return Objects.equals(description, that.description) && Objects.equals(func, that.func);
    }

    @Override
    public int hashCode() {
      return Objects.hash(description, func);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class NoCacheConstantDoubleValuesSource extends DoubleValuesSource {

    final double value;
    final DoubleValuesSource parent;

    private NoCacheConstantDoubleValuesSource(double value, DoubleValuesSource parent) {
      this.value = value;
      this.parent = parent;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return value;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof NoCacheConstantDoubleValuesSource)) return false;
      NoCacheConstantDoubleValuesSource that = (NoCacheConstantDoubleValuesSource) o;
      return Double.compare(that.value, value) == 0 &&
          Objects.equals(parent, that.parent);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, parent);
    }

    @Override
    public String toString() {
      return parent.toString();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

}
