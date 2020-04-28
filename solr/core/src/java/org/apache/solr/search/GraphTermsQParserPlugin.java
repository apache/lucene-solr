
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;

/**
 *  The GraphTermsQuery builds a disjunction query from a list of terms. The terms are first filtered by the maxDocFreq parameter.
 *  This allows graph traversals to skip traversing high frequency nodes which is often desirable from a performance standpoint.
 *
 *   Syntax: {!graphTerms f=field maxDocFreq=10000}term1,term2,term3
 */
public class GraphTermsQParserPlugin extends QParserPlugin {
  public static final String NAME = "graphTerms";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fname = localParams.get(QueryParsing.F);
        FieldType ft = req.getSchema().getFieldTypeNoEx(fname);
        int maxDocFreq = localParams.getInt("maxDocFreq", Integer.MAX_VALUE);
        String qstr = localParams.get(QueryParsing.V);//never null

        if (qstr.length() == 0) {
          return new MatchNoDocsQuery();
        }

        final String[] splitVals = qstr.split(",");

        SchemaField sf = req.getSchema().getField(fname);

        // if we don't limit by maxDocFreq, then simply use a normal set query
        if (maxDocFreq == Integer.MAX_VALUE) {
          return sf.getType().getSetQuery(this, sf, Arrays.asList(splitVals));
        }

        if (sf.getType().isPointField()) {
          PointSetQuery setQ = null;
          if (sf.getType().getNumberType() == NumberType.INTEGER) {
            int[] vals = new int[splitVals.length];
            for (int i=0; i<vals.length; i++) {
              vals[i] = Integer.parseInt(splitVals[i]);
            }
            Arrays.sort(vals);
            setQ = PointSetQuery.newSetQuery(sf.getName(), vals);
          } else if (sf.getType().getNumberType() == NumberType.LONG || sf.getType().getNumberType() == NumberType.DATE) {
            long[] vals = new long[splitVals.length];
            for (int i=0; i<vals.length; i++) {
              vals[i] = Long.parseLong(splitVals[i]);
            }
            Arrays.sort(vals);
            setQ = PointSetQuery.newSetQuery(sf.getName(), vals);
          } else if (sf.getType().getNumberType() == NumberType.FLOAT) {
            float[] vals = new float[splitVals.length];
            for (int i=0; i<vals.length; i++) {
              vals[i] = Float.parseFloat(splitVals[i]);
            }
            Arrays.sort(vals);
            setQ = PointSetQuery.newSetQuery(sf.getName(), vals);
          } else if (sf.getType().getNumberType() == NumberType.DOUBLE) {
            double[] vals = new double[splitVals.length];
            for (int i=0; i<vals.length; i++) {
              vals[i] = Double.parseDouble(splitVals[i]);
            }
            Arrays.sort(vals);
            setQ = PointSetQuery.newSetQuery(sf.getName(), vals);
          }

          setQ.setMaxDocFreq(maxDocFreq);
          return setQ;
        }

        Term[] terms = new Term[splitVals.length];
        BytesRefBuilder term = new BytesRefBuilder();
        for (int i = 0; i < splitVals.length; i++) {
          String stringVal = splitVals[i].trim();
          if (ft != null) {
            ft.readableToIndexed(stringVal, term);
          } else {
            term.copyChars(stringVal);
          }
          BytesRef ref = term.toBytesRef();
          terms[i] = new Term(fname, ref);
        }

        ArrayUtil.timSort(terms);
        return new ConstantScoreQuery(new GraphTermsQuery(fname, terms, maxDocFreq));
      }
    };
  }

  private class GraphTermsQuery extends Query implements ExtendedQuery {

    private Term[] queryTerms;
    private String field;
    private int maxDocFreq;
    private Object id;

    public GraphTermsQuery(String field, Term[] terms, int maxDocFreq) {
      this.maxDocFreq = maxDocFreq;
      this.field = field;
      this.queryTerms = terms;
      this.id = new Object();
    }

    //Just for cloning
    private GraphTermsQuery(String field, Term[] terms, int maxDocFreq, Object id) {
      this.field = field;
      this.queryTerms = terms;
      this.maxDocFreq = maxDocFreq;
      this.id = id;
    }

    public boolean getCache() {
      return false;
    }

    public boolean getCacheSep() {
      return false;
    }

    public void setCacheSep(boolean sep) {

    }

    public void setCache(boolean cache) {

    }

    public int getCost() {
      return 1; // Not a post filter. The GraphTermsQuery will typically be used as the main query.
    }

    public void setCost(int cost) {

    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      return this;
    }

    public int hashCode() {
      return 31 * classHash() + id.hashCode();
    }

    public boolean equals(Object other) {
      return sameClassAs(other) &&
             id == ((GraphTermsQuery) other).id;
    }

    public GraphTermsQuery clone() {
      GraphTermsQuery clone = new GraphTermsQuery(this.field,
                                                  this.queryTerms,
                                                  this.maxDocFreq,
                                                  this.id);
      return clone;
    }

    @Override
    public String toString(String defaultField) {
      return Arrays.stream(this.queryTerms).map(Term::toString).collect(Collectors.joining(","));
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

      List<TermStates> finalContexts = new ArrayList<>();
      List<Term> finalTerms = new ArrayList<>();
      {
        List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();
        TermStates[] termStates = new TermStates[this.queryTerms.length];
        collectTermStates(searcher.getIndexReader(), contexts, termStates, this.queryTerms);
        for(int i=0; i<termStates.length; i++) {
          TermStates ts = termStates[i];
          if(ts != null && ts.docFreq() <= this.maxDocFreq) {
            finalContexts.add(ts);
            finalTerms.add(queryTerms[i]);
          }
        }
      }

      return new ConstantScoreWeight(this, boost) {

        @Override
        public void extractTerms(Set<Term> terms) {
          // no-op
          // This query is for abuse cases when the number of terms is too high to
          // run efficiently as a BooleanQuery. So likewise we hide its terms in
          // order to protect highlighters
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final LeafReader reader = context.reader();
          Terms terms = reader.terms(field);
          if (terms == null) {
            return null;
          }
          TermsEnum  termsEnum = terms.iterator();
          PostingsEnum docs = null;
          DocIdSetBuilder builder = new DocIdSetBuilder(reader.maxDoc(), terms);
          for (int i=0; i<finalContexts.size(); i++) {
            TermStates ts = finalContexts.get(i);
            TermState termState = ts.get(context);
            if(termState != null) {
              Term term = finalTerms.get(i);
              termsEnum.seekExact(term.bytes(), ts.get(context));
              docs = termsEnum.postings(docs, PostingsEnum.NONE);
              builder.add(docs);
            }
          }
          DocIdSet docIdSet = builder.build();
          DocIdSetIterator disi = docIdSet.iterator();
          return disi == null ? null : new ConstantScoreScorer(this, score(), scoreMode, disi);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }

      };
    }

    private void collectTermStates(IndexReader reader,
                                   List<LeafReaderContext> leaves,
                                   TermStates[] contextArray,
                                   Term[] queryTerms) throws IOException {
      TermsEnum termsEnum = null;
      for (LeafReaderContext context : leaves) {

        Terms terms = context.reader().terms(this.field);
        if (terms == null) {
          // field does not exist
          continue;
        }

        termsEnum = terms.iterator();

        if (termsEnum == TermsEnum.EMPTY) continue;

        for (int i = 0; i < queryTerms.length; i++) {
          Term term = queryTerms[i];
          TermStates termStates = contextArray[i];

          if (termsEnum.seekExact(term.bytes())) {
            if (termStates == null) {
              contextArray[i] = new TermStates(reader.getContext(),
                  termsEnum.termState(), context.ord, termsEnum.docFreq(),
                  termsEnum.totalTermFreq());
            } else {
              termStates.register(termsEnum.termState(), context.ord,
                  termsEnum.docFreq(), termsEnum.totalTermFreq());
            }
          }
        }
      }
    }
  }
}



// modified version of PointInSetQuery
abstract class PointSetQuery extends Query implements DocSetProducer, Accountable {
  protected static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(PointSetQuery.class);

  // A little bit overkill for us, since all of our "terms" are always in the same field:
  final PrefixCodedTerms sortedPackedPoints;
  final int sortedPackedPointsHashCode;
  final String field;
  final int bytesPerDim;
  final int numDims;
  int maxDocFreq = Integer.MAX_VALUE;
  final long ramBytesUsed; // cache

  /**
   * Iterator of encoded point values.
   */
  // TODO: if we want to stream, maybe we should use jdk stream class?
  public static abstract class Stream implements BytesRefIterator {
    @Override
    public abstract BytesRef next();
  };

  public void setMaxDocFreq(int maxDocFreq) {
    this.maxDocFreq = maxDocFreq;
  }

  public static PointSetQuery newSetQuery(String field, float... sortedValues) {

    final BytesRef encoded = new BytesRef(new byte[Float.BYTES]);

    return new PointSetQuery(field, 1, Float.BYTES,
        new PointSetQuery.Stream() {

          int upto;

          @Override
          public BytesRef next() {
            if (upto == sortedValues.length) {
              return null;
            } else {
              FloatPoint.encodeDimension(sortedValues[upto], encoded.bytes, 0);
              upto++;
              return encoded;
            }
          }
        }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == Float.BYTES;
        return Float.toString(FloatPoint.decodeDimension(value, 0));
      }
    };
  }

  public static PointSetQuery newSetQuery(String field, long... sortedValues) {
    final BytesRef encoded = new BytesRef(new byte[Long.BYTES]);

    return new PointSetQuery(field, 1, Long.BYTES,
        new PointSetQuery.Stream() {

          int upto;

          @Override
          public BytesRef next() {
            if (upto == sortedValues.length) {
              return null;
            } else {
              LongPoint.encodeDimension(sortedValues[upto], encoded.bytes, 0);
              upto++;
              return encoded;
            }
          }
        }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == Long.BYTES;
        return Long.toString(LongPoint.decodeDimension(value, 0));
      }
    };
  }

  public static PointSetQuery newSetQuery(String field, int... sortedValues) {
    final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

    return new PointSetQuery(field, 1, Integer.BYTES,
        new PointSetQuery.Stream() {

          int upto;

          @Override
          public BytesRef next() {
            if (upto == sortedValues.length) {
              return null;
            } else {
              IntPoint.encodeDimension(sortedValues[upto], encoded.bytes, 0);
              upto++;
              return encoded;
            }
          }
        }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == Integer.BYTES;
        return Integer.toString(IntPoint.decodeDimension(value, 0));
      }
    };
  }

  public static PointSetQuery newSetQuery(String field, double... values) {

    // Don't unexpectedly change the user's incoming values array:
    double[] sortedValues = values.clone();
    Arrays.sort(sortedValues);

    final BytesRef encoded = new BytesRef(new byte[Double.BYTES]);

    return new PointSetQuery(field, 1, Double.BYTES,
        new PointSetQuery.Stream() {

          int upto;

          @Override
          public BytesRef next() {
            if (upto == sortedValues.length) {
              return null;
            } else {
              DoublePoint.encodeDimension(sortedValues[upto], encoded.bytes, 0);
              upto++;
              return encoded;
            }
          }
        }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == Double.BYTES;
        return Double.toString(DoublePoint.decodeDimension(value, 0));
      }
    };
  }

  public PointSetQuery(String field, int numDims, int bytesPerDim, Stream packedPoints) {
    this.field = field;
    this.bytesPerDim = bytesPerDim;
    this.numDims = numDims;

    // In the 1D case this works well (the more points, the more common prefixes they share, typically), but in
    // the > 1 D case, where we are only looking at the first dimension's prefix bytes, it can at worst not hurt:
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    BytesRef current;
    while ((current = packedPoints.next()) != null) {
      if (current.length != numDims * bytesPerDim) {
        throw new IllegalArgumentException("packed point length should be " + (numDims * bytesPerDim) + " but got " + current.length + "; field=\"" + field + "\" numDims=" + numDims + " bytesPerDim=" + bytesPerDim);
      }
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else {
        int cmp = previous.get().compareTo(current);
        if (cmp == 0) {
          continue; // deduplicate
        } else if (cmp > 0) {
          throw new IllegalArgumentException("values are out of order: saw " + previous + " before " + current);
        }
      }
      builder.add(field, current);
      previous.copyBytes(current);
    }
    sortedPackedPoints = builder.finish();
    sortedPackedPointsHashCode = sortedPackedPoints.hashCode();
    ramBytesUsed = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(sortedPackedPoints);
  }

  private FixedBitSet getLiveDocs(IndexSearcher searcher) throws IOException {
    if (!searcher.getIndexReader().hasDeletions()) {
      return null;
    }
    if (searcher instanceof SolrIndexSearcher) {
      return ((SolrIndexSearcher) searcher).getLiveDocSet().getBits();
    } else {
      // TODO Does this ever happen?  In Solr should always be SolrIndexSearcher?
      //smallSetSize==0 thus will always produce a BitDocSet (FixedBitSet)
      DocSetCollector docSetCollector = new DocSetCollector(0, searcher.getIndexReader().maxDoc());
      searcher.search(new MatchAllDocsQuery(), docSetCollector);
      return ((BitDocSet) docSetCollector.getDocSet()).getBits();
    }
  }

  @Override
  public DocSet createDocSet(SolrIndexSearcher searcher) throws IOException {
    return getDocSet(searcher);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  public DocSet getDocSet(IndexSearcher searcher) throws IOException {
    IndexReaderContext top = ReaderUtil.getTopLevelContext(searcher.getTopReaderContext());
    List<LeafReaderContext> segs = top.leaves();
    DocSetBuilder builder = new DocSetBuilder(top.reader().maxDoc(), Math.min(64,(top.reader().maxDoc()>>>10)+4));
    PointValues[] segPoints = new PointValues[segs.size()];
    for (int i=0; i<segPoints.length; i++) {
      segPoints[i] = segs.get(i).reader().getPointValues(field);
    }

    int maxCollect = Math.min(maxDocFreq, top.reader().maxDoc());

    PointSetQuery.CutoffPointVisitor visitor = new PointSetQuery.CutoffPointVisitor(maxCollect);
    PrefixCodedTerms.TermIterator iterator = sortedPackedPoints.iterator();
    outer: for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
      visitor.setPoint(point);
      for (int i=0; i<segs.size(); i++) {
        if (segPoints[i] == null) continue;
        visitor.setBase(segs.get(i).docBase);
        segPoints[i].intersect(visitor);
        if (visitor.getCount() > maxDocFreq) {
          continue outer;
        }
      }
      int collected = visitor.getCount();
      int[] ids = visitor.getGlobalIds();
      for (int i=0; i<collected; i++) {
        builder.add( ids[i] );
      }
    }

    FixedBitSet liveDocs = getLiveDocs(searcher);
    DocSet set = builder.build(liveDocs);
    return set;
  }


  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {
      Filter filter;

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        if (filter == null) {
          DocSet set = getDocSet(searcher);
          filter = set.getTopFilter();
        }

        // Although this set only includes live docs, other filters can be pushed down to queries.
        DocIdSet readerSet = filter.getDocIdSet(context, null);
        if (readerSet == null) {
          return null;
        }
        DocIdSetIterator readerSetIterator = readerSet.iterator();
        if (readerSetIterator == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), scoreMode, readerSetIterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }


  /** Cutoff point visitor that collects a maximum number of points before stopping. */
  private class CutoffPointVisitor implements PointValues.IntersectVisitor {
    int[] ids;
    int base;
    int count;
    private final byte[] pointBytes;

    public CutoffPointVisitor(int sz) {
      this.pointBytes = new byte[bytesPerDim * numDims];
      ids = new int[sz];
    }

    private void add(int id) {
      if (count < ids.length) {
        ids[count] = id + base;
      }
      count++;
    }

    public int getCount() { return count; }

    public int[] getGlobalIds() { return ids; }

    public void setPoint(BytesRef point) {
      // we verified this up front in query's ctor:
      assert point.length == pointBytes.length;
      System.arraycopy(point.bytes, point.offset, pointBytes, 0, pointBytes.length);
      count = 0;
    }

    public void setBase(int base) {
      this.base = base;
    }

    @Override
    public void grow(int count) {
    }

    @Override
    public void visit(int docID) {
      add(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      if (Arrays.equals(packedValue, pointBytes)) {
        add(docID);
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

      boolean crosses = false;

      for(int dim=0;dim<numDims;dim++) {
        int offset = dim*bytesPerDim;

        int cmpMin = FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, pointBytes, offset, offset + bytesPerDim);
        if (cmpMin > 0) {
          return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }

        int cmpMax = FutureArrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, pointBytes, offset, offset + bytesPerDim);
        if (cmpMax < 0) {
          return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }

        if (cmpMin != 0 || cmpMax != 0) {
          crosses = true;
        }
      }

      if (crosses) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      } else {
        // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal to our point,
        // which can easily happen if many docs share this one value
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
    }
  }

  public String getField() {
    return field;
  }

  public int getNumDims() {
    return numDims;
  }

  public int getBytesPerDim() {
    return bytesPerDim;
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + sortedPackedPointsHashCode;
    hash = 31 * hash + numDims;
    hash = 31 * hash + bytesPerDim;
    hash = 31 * hash + maxDocFreq;
    return hash;
  }

  @Override
  public final boolean equals(Object other) {
    return sameClassAs(other) &&
        equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(PointSetQuery other) {
    return other.field.equals(field) &&
        other.numDims == numDims &&
        other.bytesPerDim == bytesPerDim &&
        other.sortedPackedPointsHashCode == sortedPackedPointsHashCode &&
        other.sortedPackedPoints.equals(sortedPackedPoints) &&
        other.maxDocFreq == maxDocFreq;
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    sb.append("{");

    PrefixCodedTerms.TermIterator iterator = sortedPackedPoints.iterator();
    byte[] pointBytes = new byte[numDims * bytesPerDim];
    boolean first = true;
    for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
      if (first == false) {
        sb.append(" ");
      }
      first = false;
      System.arraycopy(point.bytes, point.offset, pointBytes, 0, pointBytes.length);
      sb.append(toString(pointBytes));
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  protected abstract String toString(byte[] value);
}
