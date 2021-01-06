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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.spatial.ShapeValues;
import org.apache.lucene.spatial.ShapeValuesSource;
import org.locationtech.spatial4j.shape.Rectangle;

/**
 * A base class for calculating a spatial relevance rank per document from a provided {@link
 * ShapeValuesSource} returning a {@link org.locationtech.spatial4j.shape.Rectangle} per-document.
 *
 * <p>Implementers: remember to implement equals and hashCode if you have fields!
 *
 * @lucene.experimental
 */
public abstract class BBoxSimilarityValueSource extends DoubleValuesSource {

  private final ShapeValuesSource bboxValueSource;

  public BBoxSimilarityValueSource(ShapeValuesSource bboxValueSource) {
    this.bboxValueSource = bboxValueSource;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "("
        + bboxValueSource.toString()
        + ","
        + similarityDescription()
        + ")";
  }

  /**
   * A comma-separated list of configurable items of the subclass to put into {@link #toString()}.
   */
  protected abstract String similarityDescription();

  @Override
  public DoubleValues getValues(LeafReaderContext readerContext, DoubleValues scores)
      throws IOException {

    final ShapeValues shapeValues = bboxValueSource.getValues(readerContext);
    return DoubleValues.withDefault(
        new DoubleValues() {
          @Override
          public double doubleValue() throws IOException {
            return score((Rectangle) shapeValues.value(), null);
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            return shapeValues.advanceExact(doc);
          }
        },
        0);
  }

  /**
   * Return a relevancy score. If {@code exp} is provided then diagnostic information is added.
   *
   * @param rect The indexed rectangle; not null.
   * @param exp Optional diagnostic holder.
   * @return a score.
   */
  protected abstract double score(Rectangle rect, AtomicReference<Explanation> exp);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false; // same class

    BBoxSimilarityValueSource that = (BBoxSimilarityValueSource) o;

    if (!bboxValueSource.equals(that.bboxValueSource)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return bboxValueSource.hashCode();
  }

  @Override
  public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
      throws IOException {
    DoubleValues dv =
        getValues(
            ctx,
            DoubleValuesSource.constant(scoreExplanation.getValue().doubleValue())
                .getValues(ctx, null));
    if (dv.advanceExact(docId)) {
      AtomicReference<Explanation> explanation = new AtomicReference<>();
      final ShapeValues shapeValues = bboxValueSource.getValues(ctx);
      if (shapeValues.advanceExact(docId)) {
        score((Rectangle) shapeValues.value(), explanation);
        return explanation.get();
      }
    }
    return Explanation.noMatch(this.toString());
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return bboxValueSource.isCacheable(ctx);
  }

  @Override
  public boolean needsScores() {
    return false;
  }
}
