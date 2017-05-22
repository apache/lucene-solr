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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;

// A TermsIncludingScoreQuery variant for point values:
abstract class PointInSetIncludingScoreQuery extends Query {

  static BiFunction<byte[], Class<? extends Number>, String> toString = (value, numericType) -> {
    if (Integer.class.equals(numericType)) {
      return Integer.toString(IntPoint.decodeDimension(value, 0));
    } else if (Long.class.equals(numericType)) {
      return Long.toString(LongPoint.decodeDimension(value, 0));
    } else if (Float.class.equals(numericType)) {
      return Float.toString(FloatPoint.decodeDimension(value, 0));
    } else if (Double.class.equals(numericType)) {
      return Double.toString(DoublePoint.decodeDimension(value, 0));
    } else {
      return "unsupported";
    }
  };

  final ScoreMode scoreMode;
  final Query originalQuery;
  final boolean multipleValuesPerDocument;
  final PrefixCodedTerms sortedPackedPoints;
  final int sortedPackedPointsHashCode;
  final String field;
  final int bytesPerDim;

  final List<Float> aggregatedJoinScores;

  static abstract class Stream extends PointInSetQuery.Stream {

    float score;

  }

  PointInSetIncludingScoreQuery(ScoreMode scoreMode, Query originalQuery, boolean multipleValuesPerDocument,
                                String field, int bytesPerDim, Stream packedPoints) {
    this.scoreMode = scoreMode;
    this.originalQuery = originalQuery;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.field = field;
    if (bytesPerDim < 1 || bytesPerDim > PointValues.MAX_NUM_BYTES) {
      throw new IllegalArgumentException("bytesPerDim must be > 0 and <= " + PointValues.MAX_NUM_BYTES + "; got " + bytesPerDim);
    }
    this.bytesPerDim = bytesPerDim;

    aggregatedJoinScores = new ArrayList<>();
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    BytesRef current;
    while ((current = packedPoints.next()) != null) {
      if (current.length != bytesPerDim) {
        throw new IllegalArgumentException("packed point length should be " + (bytesPerDim) + " but got " + current.length + "; field=\"" + field + "\"bytesPerDim=" + bytesPerDim);
      }
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else {
        int cmp = previous.get().compareTo(current);
        if (cmp == 0) {
          throw new IllegalArgumentException("unexpected duplicated value: " + current);
        } else if (cmp >= 0) {
          throw new IllegalArgumentException("values are out of order: saw " + previous + " before " + current);
        }
      }
      builder.add(field, current);
      aggregatedJoinScores.add(packedPoints.score);
      previous.copyBytes(current);
    }
    sortedPackedPoints = builder.finish();
    sortedPackedPointsHashCode = sortedPackedPoints.hashCode();
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight originalWeight = originalQuery.createWeight(searcher, needsScores);
    return new Weight(this) {

      @Override
      public void extractTerms(Set<Term> terms) {
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Scorer scorer = scorer(context);
        if (scorer != null) {
          int target = scorer.iterator().advance(doc);
          if (doc == target) {
            return Explanation.match(scorer.score(), "A match");
          }
        }
        return Explanation.noMatch("Not a match");
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return originalWeight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float boost) {
        originalWeight.normalize(norm, boost);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues();
        if (values == null) {
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          return null;
        }
        if (fieldInfo.getPointDimensionCount() != 1) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numDims=" + fieldInfo.getPointDimensionCount() + " but this query has numDims=1");
        }
        if (fieldInfo.getPointNumBytes() != bytesPerDim) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + fieldInfo.getPointNumBytes() + " but this query has bytesPerDim=" + bytesPerDim);
        }

        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        float[] scores = new float[reader.maxDoc()];
        values.intersect(field, new MergePointVisitor(sortedPackedPoints, result, scores));
        return new Scorer(this) {

          DocIdSetIterator disi = new BitSetIterator(result, 10L);

          @Override
          public float score() throws IOException {
            return scores[docID()];
          }

          @Override
          public int freq() throws IOException {
            return 1;
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public DocIdSetIterator iterator() {
            return disi;
          }

        };
      }
    };
  }

  private class MergePointVisitor implements IntersectVisitor {

    private final FixedBitSet result;
    private final float[] scores;

    private TermIterator iterator;
    private Iterator<Float> scoreIterator;
    private BytesRef nextQueryPoint;
    float nextScore;
    private final BytesRef scratch = new BytesRef();

    private MergePointVisitor(PrefixCodedTerms sortedPackedPoints, FixedBitSet result, float[] scores) throws IOException {
      this.result = result;
      this.scores = scores;
      scratch.length = bytesPerDim;
      this.iterator = sortedPackedPoints.iterator();
      this.scoreIterator = aggregatedJoinScores.iterator();
      nextQueryPoint = iterator.next();
      if (scoreIterator.hasNext()) {
        nextScore = scoreIterator.next();
      }
    }

    @Override
    public void visit(int docID) {
      throw new IllegalStateException("shouldn't get here, since CELL_INSIDE_QUERY isn't emitted");
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      scratch.bytes = packedValue;
      while (nextQueryPoint != null) {
        int cmp = nextQueryPoint.compareTo(scratch);
        if (cmp == 0) {
          // Query point equals index point, so collect and return
          if (multipleValuesPerDocument) {
            if (result.get(docID) == false) {
              result.set(docID);
              scores[docID] = nextScore;
            }
          } else {
            result.set(docID);
            scores[docID] = nextScore;
          }
          break;
        } else if (cmp < 0) {
          // Query point is before index point, so we move to next query point
          nextQueryPoint = iterator.next();
          if (scoreIterator.hasNext()) {
            nextScore = scoreIterator.next();
          }
        } else {
          // Query point is after index point, so we don't collect and we return:
          break;
        }
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      while (nextQueryPoint != null) {
        scratch.bytes = minPackedValue;
        int cmpMin = nextQueryPoint.compareTo(scratch);
        if (cmpMin < 0) {
          // query point is before the start of this cell
          nextQueryPoint = iterator.next();
          if (scoreIterator.hasNext()) {
            nextScore = scoreIterator.next();
          }
          continue;
        }
        scratch.bytes = maxPackedValue;
        int cmpMax = nextQueryPoint.compareTo(scratch);
        if (cmpMax > 0) {
          // query point is after the end of this cell
          return Relation.CELL_OUTSIDE_QUERY;
        }

        return Relation.CELL_CROSSES_QUERY;
      }

      // We exhausted all points in the query:
      return Relation.CELL_OUTSIDE_QUERY;
    }
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + scoreMode.hashCode();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + originalQuery.hashCode();
    hash = 31 * hash + sortedPackedPointsHashCode;
    hash = 31 * hash + bytesPerDim;
    return hash;
  }

  @Override
  public final boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(PointInSetIncludingScoreQuery other) {
    return other.scoreMode.equals(scoreMode) &&
           other.field.equals(field) &&
           other.originalQuery.equals(originalQuery) &&
           other.bytesPerDim == bytesPerDim &&
           other.sortedPackedPointsHashCode == sortedPackedPointsHashCode &&
           other.sortedPackedPoints.equals(sortedPackedPoints);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    sb.append("{");

    TermIterator iterator = sortedPackedPoints.iterator();
    byte[] pointBytes = new byte[bytesPerDim];
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

  protected abstract String toString(byte[] value);
}
