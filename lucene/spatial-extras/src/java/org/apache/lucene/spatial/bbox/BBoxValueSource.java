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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.locationtech.spatial4j.shape.Rectangle;

/**
 * A ValueSource in which the indexed Rectangle is returned from
 * {@link org.apache.lucene.queries.function.FunctionValues#objectVal(int)}.
 *
 * @lucene.internal
 */
class BBoxValueSource extends ValueSource {

  private final BBoxStrategy strategy;

  public BBoxValueSource(BBoxStrategy strategy) {
    this.strategy = strategy;
  }

  @Override
  public String description() {
    return "bboxShape(" + strategy.getFieldName() + ")";
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    LeafReader reader = readerContext.reader();
    final NumericDocValues minX = DocValues.getNumeric(reader, strategy.field_minX);
    final NumericDocValues minY = DocValues.getNumeric(reader, strategy.field_minY);
    final NumericDocValues maxX = DocValues.getNumeric(reader, strategy.field_maxX);
    final NumericDocValues maxY = DocValues.getNumeric(reader, strategy.field_maxY);

    //reused
    final Rectangle rect = strategy.getSpatialContext().makeRectangle(0,0,0,0);

    return new FunctionValues() {
      private int lastDocID = -1;

      private double getDocValue(NumericDocValues values, int doc) throws IOException {
        int curDocID = values.docID();
        if (doc > curDocID) {
          curDocID = values.advance(doc);
        }
        if (doc == curDocID) {
          return Double.longBitsToDouble(values.longValue());
        } else {
          return 0.0;
        }
      }

      @Override
      public Object objectVal(int doc) throws IOException {
        if (doc < lastDocID) {
          throw new AssertionError("docs were sent out-of-order: lastDocID=" + lastDocID + " vs doc=" + doc);
        }
        lastDocID = doc;

        double minXValue = getDocValue(minX, doc);
        if (minX.docID() != doc) {
          return null;
        } else {
          double minYValue = getDocValue(minY, doc);
          double maxXValue = getDocValue(maxX, doc);
          double maxYValue = getDocValue(maxY, doc);
          rect.reset(minXValue, maxXValue, minYValue, maxYValue);
          return rect;
        }
      }

      @Override
      public String strVal(int doc) throws IOException {//TODO support WKT output once Spatial4j does
        Object v = objectVal(doc);
        return v == null ? null : v.toString();
      }

      @Override
      public boolean exists(int doc) throws IOException {
        getDocValue(minX, doc);
        return minX.docID() == doc;
      }

      @Override
      public Explanation explain(int doc) throws IOException {
        return Explanation.match(Float.NaN, toString(doc));
      }

      @Override
      public String toString(int doc) throws IOException {
        return description() + '=' + strVal(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BBoxValueSource that = (BBoxValueSource) o;

    if (!strategy.equals(that.strategy)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return strategy.hashCode();
  }
}
