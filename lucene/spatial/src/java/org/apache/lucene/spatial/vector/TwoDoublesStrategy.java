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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.lucene.spatial.util.CachingDoubleValueSource;
import org.apache.lucene.spatial.util.ValueSourceFilter;

/**
 * @lucene.experimental
 */
public class TwoDoublesStrategy extends SpatialStrategy {

  public static final String SUFFIX_X = "__x";
  public static final String SUFFIX_Y = "__y";

  private final String fieldNameX;
  private final String fieldNameY;

  public int precisionStep = 8; // same as solr default

  public TwoDoublesStrategy(SpatialContext ctx, String fieldNamePrefix) {
    super(ctx, fieldNamePrefix);
    this.fieldNameX = fieldNamePrefix+SUFFIX_X;
    this.fieldNameY = fieldNamePrefix+SUFFIX_Y;
  }

  public void setPrecisionStep( int p ) {
    precisionStep = p;
    if (precisionStep<=0 || precisionStep>=64)
      precisionStep=Integer.MAX_VALUE;
  }

  String getFieldNameX() {
    return fieldNameX;
  }

  String getFieldNameY() {
    return fieldNameY;
  }

  @Override
  public IndexableField[] createIndexableFields(Shape shape) {
    if( shape instanceof Point ) {
      Point point = (Point)shape;
      FieldType doubleFieldType = new FieldType(DoubleField.TYPE_NOT_STORED);
      doubleFieldType.setNumericPrecisionStep(precisionStep);
      IndexableField[] f = new IndexableField[2];
      f[0] = new DoubleField(fieldNameX, point.getX(), doubleFieldType);
      f[1] = new DoubleField(fieldNameY, point.getY(), doubleFieldType);
      return f;
    }
    if( !ignoreIncompatibleGeometry ) {
      throw new IllegalArgumentException( "TwoDoublesStrategy can not index: "+shape );
    }
    return new IndexableField[0]; // nothing (solr does not support null)
  }

  @Override
  public ValueSource makeValueSource(SpatialArgs args) {
    Point p = args.getShape().getCenter();
    return new DistanceValueSource(this, p, ctx.getDistCalc());
  }

  @Override
  public Filter makeFilter(SpatialArgs args) {
    if( args.getShape() instanceof Circle) {
      if( SpatialOperation.is( args.getOperation(),
          SpatialOperation.Intersects,
          SpatialOperation.IsWithin )) {
        Circle circle = (Circle)args.getShape();
        Query bbox = makeWithin(circle.getBoundingBox());

        // Make the ValueSource
        ValueSource valueSource = makeValueSource(args);

        return new ValueSourceFilter(
            new QueryWrapperFilter( bbox ), valueSource, 0, circle.getDistance() );
      }
    }
    return new QueryWrapperFilter( makeQuery(args) );
  }

  @Override
  public Query makeQuery(SpatialArgs args) {
    // For starters, just limit the bbox
    Shape shape = args.getShape();
    if (!(shape instanceof Rectangle || shape instanceof Circle)) {
      throw new InvalidShapeException("Only Rectangles and Circles are currently supported, " +
          "found [" + shape.getClass() + "]");//TODO
    }

    Rectangle bbox = shape.getBoundingBox();

    if (bbox.getCrossesDateLine()) {
      throw new UnsupportedOperationException( "Crossing dateline not yet supported" );
    }

    ValueSource valueSource = null;

    Query spatial = null;
    SpatialOperation op = args.getOperation();

    if( SpatialOperation.is( op,
        SpatialOperation.BBoxWithin,
        SpatialOperation.BBoxIntersects ) ) {
        spatial = makeWithin(bbox);
    }
    else if( SpatialOperation.is( op,
      SpatialOperation.Intersects,
      SpatialOperation.IsWithin ) ) {
      spatial = makeWithin(bbox);
      if( args.getShape() instanceof Circle) {
        Circle circle = (Circle)args.getShape();

        // Make the ValueSource
        valueSource = makeValueSource(args);

        ValueSourceFilter vsf = new ValueSourceFilter(
            new QueryWrapperFilter( spatial ), valueSource, 0, circle.getDistance() );

        spatial = new FilteredQuery( new MatchAllDocsQuery(), vsf );
      }
    }
    else if( op == SpatialOperation.IsDisjointTo ) {
      spatial =  makeDisjoint(bbox);
    }

    if( spatial == null ) {
      throw new UnsupportedSpatialOperation(args.getOperation());
    }

    if( valueSource != null ) {
      valueSource = new CachingDoubleValueSource(valueSource);
    }
    else {
      valueSource = makeValueSource(args);
    }
    Query spatialRankingQuery = new FunctionQuery(valueSource);
    BooleanQuery bq = new BooleanQuery();
    bq.add(spatial,BooleanClause.Occur.MUST);
    bq.add(spatialRankingQuery,BooleanClause.Occur.MUST);
    return bq;
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   * @return the spatial query
   */
  private Query makeWithin(Rectangle bbox) {
    Query qX = NumericRangeQuery.newDoubleRange(
      fieldNameX,
      precisionStep,
      bbox.getMinX(),
      bbox.getMaxX(),
      true,
      true);
    Query qY = NumericRangeQuery.newDoubleRange(
      fieldNameY,
      precisionStep,
      bbox.getMinY(),
      bbox.getMaxY(),
      true,
      true);

    BooleanQuery bq = new BooleanQuery();
    bq.add(qX,BooleanClause.Occur.MUST);
    bq.add(qY,BooleanClause.Occur.MUST);
    return bq;
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   * @return the spatial query
   */
  Query makeDisjoint(Rectangle bbox) {
    Query qX = NumericRangeQuery.newDoubleRange(
      fieldNameX,
      precisionStep,
      bbox.getMinX(),
      bbox.getMaxX(),
      true,
      true);
    Query qY = NumericRangeQuery.newDoubleRange(
      fieldNameY,
      precisionStep,
      bbox.getMinY(),
      bbox.getMaxY(),
      true,
      true);

    BooleanQuery bq = new BooleanQuery();
    bq.add(qX,BooleanClause.Occur.MUST_NOT);
    bq.add(qY,BooleanClause.Occur.MUST_NOT);
    return bq;
  }

}




