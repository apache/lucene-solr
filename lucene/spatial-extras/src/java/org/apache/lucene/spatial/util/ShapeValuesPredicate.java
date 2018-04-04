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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SegmentCacheable;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.spatial.ShapeValues;
import org.apache.lucene.spatial.ShapeValuesSource;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Compares a shape from a provided {@link ShapeValuesSource} with a given Shape and sees
 * if it matches a given {@link SpatialOperation} (the predicate).
 *
 * Consumers should call {@link #iterator(LeafReaderContext, DocIdSetIterator)} to obtain a
 * {@link TwoPhaseIterator} over a particular {@link DocIdSetIterator}.  The initial DocIdSetIterator
 * will be used as the approximation, and the {@link SpatialOperation} comparison will only be
 * performed in {@link TwoPhaseIterator#matches()}
 *
 * @lucene.experimental
 */
public class ShapeValuesPredicate implements SegmentCacheable {
  private final ShapeValuesSource shapeValuesource;//the left hand side
  private final SpatialOperation op;
  private final Shape queryShape;//the right hand side (constant)

  /**
   *
   * @param shapeValuesource Must yield {@link Shape} instances from its objectVal(doc). If null
   *                         then the result is false. This is the left-hand (indexed) side.
   * @param op the predicate
   * @param queryShape The shape on the right-hand (query) side.
   */
  public ShapeValuesPredicate(ShapeValuesSource shapeValuesource, SpatialOperation op, Shape queryShape) {
    this.shapeValuesource = shapeValuesource;
    this.op = op;
    this.queryShape = queryShape;
  }

  @Override
  public String toString() {
    return shapeValuesource + " " + op + " " + queryShape;
  }

  public TwoPhaseIterator iterator(LeafReaderContext ctx, DocIdSetIterator approximation) throws IOException {
    final ShapeValues shapeValues = shapeValuesource.getValues(ctx);
    return new TwoPhaseIterator(approximation) {
      @Override
      public boolean matches() throws IOException {
        return shapeValues.advanceExact(approximation.docID()) && op.evaluate(shapeValues.value(), queryShape);
      }

      @Override
      public float matchCost() {
        return 100; // is this necessary?
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ShapeValuesPredicate that = (ShapeValuesPredicate) o;

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

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return shapeValuesource.isCacheable(ctx);
  }
}
