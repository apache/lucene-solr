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
package org.apache.lucene.spatial.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

/**
 * The area of a Shape retrieved from a ValueSource via
 * {@link org.apache.lucene.queries.function.FunctionValues#objectVal(int)}.
 *
 * @see Shape#getArea(org.locationtech.spatial4j.context.SpatialContext)
 *
 * @lucene.experimental
 */
public class ShapeAreaValueSource extends ValueSource {
  private final ValueSource shapeValueSource;
  private final SpatialContext ctx;//not part of identity; should be associated with shapeValueSource indirectly
  private final boolean geoArea;
  private double multiplier;

  public ShapeAreaValueSource(ValueSource shapeValueSource, SpatialContext ctx, boolean geoArea, double multiplier) {
    this.shapeValueSource = shapeValueSource;
    this.ctx = ctx;
    this.geoArea = geoArea;
    this.multiplier = multiplier;
  }

  @Override
  public String description() {
    return "area(" + shapeValueSource.description() + ",geo=" + geoArea + ")";
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    shapeValueSource.createWeight(context, searcher);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues shapeValues = shapeValueSource.getValues(context, readerContext);

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        Shape shape = (Shape) shapeValues.objectVal(doc);
        if (shape == null || shape.isEmpty())
          return 0;//or NaN?
        //This part of Spatial4j API is kinda weird. Passing null means 2D area, otherwise geo
        //   assuming ctx.isGeo()
        return shape.getArea( geoArea ? ctx : null ) * multiplier;
      }

      @Override
      public boolean exists(int doc) {
        return shapeValues.exists(doc);
      }

      @Override
      public Explanation explain(int doc) {
        Explanation exp = super.explain(doc);
        List<Explanation> details = new ArrayList<>(Arrays.asList(exp.getDetails()));
        details.add(shapeValues.explain(doc));
        return Explanation.match(exp.getValue(), exp.getDescription(), details);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ShapeAreaValueSource that = (ShapeAreaValueSource) o;

    if (geoArea != that.geoArea) return false;
    if (!shapeValueSource.equals(that.shapeValueSource)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = shapeValueSource.hashCode();
    result = 31 * result + (geoArea ? 1 : 0);
    return result;
  }

}
