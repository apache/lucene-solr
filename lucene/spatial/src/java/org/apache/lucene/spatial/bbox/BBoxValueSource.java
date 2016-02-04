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

import com.spatial4j.core.shape.Rectangle;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;

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

    final Bits validBits = DocValues.getDocsWithField(reader, strategy.field_minX);//could have chosen any field
    //reused
    final Rectangle rect = strategy.getSpatialContext().makeRectangle(0,0,0,0);

    return new FunctionValues() {
      @Override
      public Object objectVal(int doc) {
        if (!validBits.get(doc)) {
          return null;
        } else {
          rect.reset(
              Double.longBitsToDouble(minX.get(doc)), Double.longBitsToDouble(maxX.get(doc)),
              Double.longBitsToDouble(minY.get(doc)), Double.longBitsToDouble(maxY.get(doc)));
          return rect;
        }
      }

      @Override
      public String strVal(int doc) {//TODO support WKT output once Spatial4j does
        Object v = objectVal(doc);
        return v == null ? null : v.toString();
      }

      @Override
      public boolean exists(int doc) {
        return validBits.get(doc);
      }

      @Override
      public Explanation explain(int doc) {
        return Explanation.match(Float.NaN, toString(doc));
      }

      @Override
      public String toString(int doc) {
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
