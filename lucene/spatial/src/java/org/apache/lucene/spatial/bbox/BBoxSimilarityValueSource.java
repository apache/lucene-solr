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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

import com.spatial4j.core.shape.Rectangle;

/**
 * A base class for calculating a spatial relevance rank per document from a provided
 * {@link ValueSource} in which {@link FunctionValues#objectVal(int)} returns a {@link
 * com.spatial4j.core.shape.Rectangle}.
 * <p>
 * Implementers: remember to implement equals and hashCode if you have
 * fields!
 *
 * @lucene.experimental
 */
public abstract class BBoxSimilarityValueSource extends ValueSource {

  private final ValueSource bboxValueSource;

  public BBoxSimilarityValueSource(ValueSource bboxValueSource) {
    this.bboxValueSource = bboxValueSource;
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    bboxValueSource.createWeight(context, searcher);
  }

  @Override
  public String description() {
    return getClass().getSimpleName()+"(" + bboxValueSource.description() + "," + similarityDescription() + ")";
  }

  /** A comma-separated list of configurable items of the subclass to put into {@link #description()}. */
  protected abstract String similarityDescription();

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

    final FunctionValues shapeValues = bboxValueSource.getValues(context, readerContext);

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        //? limit to Rect or call getBoundingBox()? latter would encourage bad practice
        final Rectangle rect = (Rectangle) shapeValues.objectVal(doc);
        return rect==null ? 0 : score(rect, null);
      }

      @Override
      public boolean exists(int doc) {
        return shapeValues.exists(doc);
      }

      @Override
      public Explanation explain(int doc) {
        final Rectangle rect = (Rectangle) shapeValues.objectVal(doc);
        if (rect == null)
          return Explanation.noMatch("no rect");
        AtomicReference<Explanation> explanation = new AtomicReference<>();
        score(rect, explanation);
        return explanation.get();
      }
    };
  }

  /**
   * Return a relevancy score. If {@code exp} is provided then diagnostic information is added.
   * @param rect The indexed rectangle; not null.
   * @param exp Optional diagnostic holder.
   * @return a score.
   */
  protected abstract double score(Rectangle rect, AtomicReference<Explanation> exp);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;//same class

    BBoxSimilarityValueSource that = (BBoxSimilarityValueSource) o;

    if (!bboxValueSource.equals(that.bboxValueSource)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return bboxValueSource.hashCode();
  }
}
