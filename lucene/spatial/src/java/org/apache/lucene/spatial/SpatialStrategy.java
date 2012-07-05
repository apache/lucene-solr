package org.apache.lucene.spatial;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.query.SpatialArgs;

/**
 * must be thread safe
 *
 * @lucene.experimental
 */
public abstract class SpatialStrategy {

  protected boolean ignoreIncompatibleGeometry = false;
  protected final SpatialContext ctx;
  private final String fieldName;

  /**
   * Constructs the spatial strategy with its mandatory arguments.
   */
  public SpatialStrategy(SpatialContext ctx, String fieldName) {
    if (ctx == null)
      throw new IllegalArgumentException("ctx is required");
    this.ctx = ctx;
    if (fieldName == null || fieldName.length() == 0)
      throw new IllegalArgumentException("fieldName is required");
    this.fieldName = fieldName;
  }

  public SpatialContext getSpatialContext() {
    return ctx;
  }

  /** Corresponds with Solr's  FieldType.isPolyField(). */
  public boolean isPolyField() {
    return false;
  }

  /**
   * The name of the field or the prefix of them if there are multiple
   * fields needed internally.
   * @return Not null.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Corresponds with Solr's FieldType.createField().
   *
   * This may return a null field if it does not want to make anything.
   * This is reasonable behavior if 'ignoreIncompatibleGeometry=true' and the
   * geometry is incompatible
   */
  public abstract IndexableField createField(Shape shape, boolean index, boolean store);

  /** Corresponds with Solr's FieldType.createFields(). */
  public IndexableField[] createFields(Shape shape, boolean index, boolean store) {
    return new IndexableField[] { createField(shape, index, store) };
  }

  /**
   * The value source yields a number that is proportional to the distance between the query shape and indexed data.
   */
  public abstract ValueSource makeValueSource(SpatialArgs args);

  /**
   * Make a query which has a score based on the distance from the data to the query shape.
   * The default implementation constructs a {@link FilteredQuery} based on
   * {@link #makeFilter(org.apache.lucene.spatial.query.SpatialArgs)} and
   * {@link #makeValueSource(org.apache.lucene.spatial.query.SpatialArgs)}.
   */
  public Query makeQuery(SpatialArgs args) {
    Filter filter = makeFilter(args);
    ValueSource vs = makeValueSource(args);
    return new FilteredQuery(new FunctionQuery(vs), filter);
  }
  /**
   * Make a Filter
   */
  public abstract Filter makeFilter(SpatialArgs args);

  public boolean isIgnoreIncompatibleGeometry() {
    return ignoreIncompatibleGeometry;
  }

  public void setIgnoreIncompatibleGeometry(boolean ignoreIncompatibleGeometry) {
    this.ignoreIncompatibleGeometry = ignoreIncompatibleGeometry;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+" field:"+fieldName+" ctx="+ctx;
  }
}
