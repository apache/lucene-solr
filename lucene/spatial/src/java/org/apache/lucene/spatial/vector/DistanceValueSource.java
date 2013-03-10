package org.apache.lucene.spatial.vector;

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

import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;

/**
 * An implementation of the Lucene ValueSource model that returns the distance
 * for a {@link PointVectorStrategy}.
 *
 * @lucene.internal
 */
public class DistanceValueSource extends ValueSource {

  private PointVectorStrategy strategy;
  private final Point from;

  /**
   * Constructor.
   */
  public DistanceValueSource(PointVectorStrategy strategy, Point from) {
    this.strategy = strategy;
    this.from = from;
  }

  /**
   * Returns the ValueSource description.
   */
  @Override
  public String description() {
    return "DistanceValueSource("+strategy+", "+from+")";
  }

  /**
   * Returns the FunctionValues used by the function query.
   */
  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    AtomicReader reader = readerContext.reader();

    final FieldCache.Doubles ptX = FieldCache.DEFAULT.getDoubles(reader, strategy.getFieldNameX(), true);
    final FieldCache.Doubles ptY = FieldCache.DEFAULT.getDoubles(reader, strategy.getFieldNameY(), true);
    final Bits validX =  FieldCache.DEFAULT.getDocsWithField(reader, strategy.getFieldNameX());
    final Bits validY =  FieldCache.DEFAULT.getDocsWithField(reader, strategy.getFieldNameY());

    return new FunctionValues() {

      private final Point from = DistanceValueSource.this.from;
      private final DistanceCalculator calculator = strategy.getSpatialContext().getDistCalc();
      private final double nullValue = (strategy.getSpatialContext().isGeo() ? 180 : Double.MAX_VALUE);

      @Override
      public float floatVal(int doc) {
        return (float) doubleVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        // make sure it has minX and area
        if (validX.get(doc)) {
          assert validY.get(doc);
          return calculator.distance(from, ptX.get(doc), ptY.get(doc));
        }
        return nullValue;
      }

      @Override
      public String toString(int doc) {
        return description() + "=" + floatVal(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DistanceValueSource that = (DistanceValueSource) o;

    if (!from.equals(that.from)) return false;
    if (!strategy.equals(that.strategy)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return from.hashCode();
  }
}
