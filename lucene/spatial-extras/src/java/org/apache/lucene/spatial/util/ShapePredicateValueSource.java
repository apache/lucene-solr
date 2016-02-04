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

import com.spatial4j.core.shape.Shape;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A boolean ValueSource that compares a shape from a provided ValueSource with a given Shape and sees
 * if it matches a given {@link SpatialOperation} (the predicate).
 *
 * @lucene.experimental
 */
public class ShapePredicateValueSource extends ValueSource {
  private final ValueSource shapeValuesource;//the left hand side
  private final SpatialOperation op;
  private final Shape queryShape;//the right hand side (constant)

  /**
   *
   * @param shapeValuesource Must yield {@link Shape} instances from its objectVal(doc). If null
   *                         then the result is false. This is the left-hand (indexed) side.
   * @param op the predicate
   * @param queryShape The shape on the right-hand (query) side.
   */
  public ShapePredicateValueSource(ValueSource shapeValuesource, SpatialOperation op, Shape queryShape) {
    this.shapeValuesource = shapeValuesource;
    this.op = op;
    this.queryShape = queryShape;
  }

  @Override
  public String description() {
    return shapeValuesource + " " + op + " " + queryShape;
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    shapeValuesource.createWeight(context, searcher);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues shapeValues = shapeValuesource.getValues(context, readerContext);

    return new BoolDocValues(this) {
      @Override
      public boolean boolVal(int doc) {
        Shape indexedShape = (Shape) shapeValues.objectVal(doc);
        if (indexedShape == null)
          return false;
        return op.evaluate(indexedShape, queryShape);
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

    ShapePredicateValueSource that = (ShapePredicateValueSource) o;

    if (!shapeValuesource.equals(that.shapeValuesource)) return false;
    if (!op.equals(that.op)) return false;
    if (!queryShape.equals(that.queryShape)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = shapeValuesource.hashCode();
    result = 31 * result + op.hashCode();
    result = 31 * result + queryShape.hashCode();
    return result;
  }
}
