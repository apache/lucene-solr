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
package org.apache.lucene.spatial.bbox;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;

import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.simple.RectangleImpl;

/**
 * An implementation of the Lucene ValueSource model to support spatial relevance ranking.
 */
public class BBoxSimilarityValueSource extends ValueSource {

  private final BBoxFieldInfo field;
  private final BBoxSimilarity similarity;

  /**
   * Constructor.
   *
   * @param queryEnvelope the query envelope
   * @param queryPower the query power (scoring algorithm)
   * @param targetPower the target power (scoring algorithm)
   */
  public BBoxSimilarityValueSource(BBoxSimilarity similarity, BBoxFieldInfo field) {
    this.similarity = similarity;
    this.field = field;
  }

  /**
   * Returns the ValueSource description.
   *
   * @return the description
   */
  @Override
  public String description() {
    return "BBoxSimilarityValueSource(" + similarity + ")";
  }


  /**
   * Returns the DocValues used by the function query.
   *
   * @param reader the index reader
   * @return the values
   */
  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    AtomicReader reader = readerContext.reader();
    final double[] minX = FieldCache.DEFAULT.getDoubles(reader, field.minX, true);
    final double[] minY = FieldCache.DEFAULT.getDoubles(reader, field.minY, true);
    final double[] maxX = FieldCache.DEFAULT.getDoubles(reader, field.maxX, true);
    final double[] maxY = FieldCache.DEFAULT.getDoubles(reader, field.maxY, true);

    final Bits validMinX = FieldCache.DEFAULT.getDocsWithField(reader, field.minX);
    final Bits validMaxX = FieldCache.DEFAULT.getDocsWithField(reader, field.maxX);

    return new FunctionValues() {
      @Override
      public float floatVal(int doc) {
        // make sure it has minX and area
        if (validMinX.get(doc) && validMaxX.get(doc)) {
          Rectangle rect = new RectangleImpl(
              minX[doc], maxX[doc],
              minY[doc], maxY[doc]);
          return (float) similarity.score(rect, null);
        }
        return 0;
      }

      public Explanation explain(int doc) {
        // make sure it has minX and area
        if (validMinX.get(doc) && validMaxX.get(doc)) {
          Rectangle rect = new RectangleImpl(
              minX[doc], maxX[doc],
              minY[doc], maxY[doc]);
          Explanation exp = new Explanation();
          similarity.score(rect, exp);
          return exp;
        }
        return new Explanation(0, "No BBox");
      }

      @Override
      public String toString(int doc) {
        return description() + "=" + floatVal(doc);
      }
    };
  }

  /**
   * Determines if this ValueSource is equal to another.
   *
   * @param o the ValueSource to compare
   * @return <code>true</code> if the two objects are based upon the same query envelope
   */
  @Override
  public boolean equals(Object o) {
    if (o.getClass() != BBoxSimilarityValueSource.class) {
      return false;
    }

    BBoxSimilarityValueSource other = (BBoxSimilarityValueSource) o;
    return similarity.equals(other.similarity);
  }

  @Override
  public int hashCode() {
    return BBoxSimilarityValueSource.class.hashCode() + similarity.hashCode();
  }
}
