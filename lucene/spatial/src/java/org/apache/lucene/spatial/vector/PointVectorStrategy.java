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
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
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
 * Simple {@link SpatialStrategy} which represents Points in two numeric {@link
 * DoubleField}s.  The Strategy's best feature is decent distance sort.
 *
 * <h4>Characteristics:</h4>
 * <ul>
 * <li>Only indexes points; just one per field value.</li>
 * <li>Can query by a rectangle or circle.</li>
 * <li>{@link
 * org.apache.lucene.spatial.query.SpatialOperation#Intersects} and {@link
 * SpatialOperation#IsWithin} is supported.</li>
 * <li>Uses the FieldCache for
 * {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)} and for
 * searching with a Circle.</li>
 * </ul>
 *
 * <h4>Implementation:</h4>
 * This is a simple Strategy.  Search works with {@link NumericRangeQuery}s on
 * an x & y pair of fields.  A Circle query does the same bbox query but adds a
 * ValueSource filter on
 * {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)}.
 * <p />
 * One performance shortcoming with this strategy is that a scenario involving
 * both a search using a Circle and sort will result in calculations for the
 * spatial distance being done twice -- once for the filter and second for the
 * sort.
 *
 * @lucene.experimental
 */
public class PointVectorStrategy extends SpatialStrategy {

  public static final String SUFFIX_X = "__x";
  public static final String SUFFIX_Y = "__y";

  private final String fieldNameX;
  private final String fieldNameY;

  public int precisionStep = 8; // same as solr default

  public PointVectorStrategy(SpatialContext ctx, String fieldNamePrefix) {
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
  public Field[] createIndexableFields(Shape shape) {
    if (shape instanceof Point)
      return createIndexableFields((Point) shape);
    throw new UnsupportedOperationException("Can only index Point, not " + shape);
  }

  /** @see #createIndexableFields(com.spatial4j.core.shape.Shape) */
  public Field[] createIndexableFields(Point point) {
    FieldType doubleFieldType = new FieldType(DoubleField.TYPE_NOT_STORED);
    doubleFieldType.setNumericPrecisionStep(precisionStep);
    Field[] f = new Field[2];
    f[0] = new DoubleField(fieldNameX, point.getX(), doubleFieldType);
    f[1] = new DoubleField(fieldNameY, point.getY(), doubleFieldType);
    return f;
  }

  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint) {
    return new DistanceValueSource(this, queryPoint);
  }

  @Override
  public Filter makeFilter(SpatialArgs args) {
    //unwrap the CSQ from makeQuery
    ConstantScoreQuery csq = makeQuery(args);
    Filter filter = csq.getFilter();
    if (filter != null)
      return filter;
    else
      return new QueryWrapperFilter(csq.getQuery());
  }

  @Override
  public ConstantScoreQuery makeQuery(SpatialArgs args) {
    if(! SpatialOperation.is( args.getOperation(),
        SpatialOperation.Intersects,
        SpatialOperation.IsWithin ))
      throw new UnsupportedSpatialOperation(args.getOperation());
    Shape shape = args.getShape();
    if (shape instanceof Rectangle) {
      Rectangle bbox = (Rectangle) shape;
      return new ConstantScoreQuery(makeWithin(bbox));
    } else if (shape instanceof Circle) {
      Circle circle = (Circle)shape;
      Rectangle bbox = circle.getBoundingBox();
      ValueSourceFilter vsf = new ValueSourceFilter(
          new QueryWrapperFilter(makeWithin(bbox)),
          makeDistanceValueSource(circle.getCenter()),
          0,
          circle.getRadius() );
      return new ConstantScoreQuery(vsf);
    } else {
      throw new UnsupportedOperationException("Only Rectangles and Circles are currently supported, " +
          "found [" + shape.getClass() + "]");//TODO
    }
  }

  //TODO this is basically old code that hasn't been verified well and should probably be removed
  public Query makeQueryDistanceScore(SpatialArgs args) {
    // For starters, just limit the bbox
    Shape shape = args.getShape();
    if (!(shape instanceof Rectangle || shape instanceof Circle)) {
      throw new UnsupportedOperationException("Only Rectangles and Circles are currently supported, " +
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
        valueSource = makeDistanceValueSource(shape.getCenter());

        ValueSourceFilter vsf = new ValueSourceFilter(
            new QueryWrapperFilter( spatial ), valueSource, 0, circle.getRadius() );

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
      valueSource = makeDistanceValueSource(shape.getCenter());
    }
    Query spatialRankingQuery = new FunctionQuery(valueSource);
    BooleanQuery bq = new BooleanQuery();
    bq.add(spatial,BooleanClause.Occur.MUST);
    bq.add(spatialRankingQuery,BooleanClause.Occur.MUST);
    return bq;
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   */
  private Query makeWithin(Rectangle bbox) {
    BooleanQuery bq = new BooleanQuery();
    BooleanClause.Occur MUST = BooleanClause.Occur.MUST;
    if (bbox.getCrossesDateLine()) {
      //use null as performance trick since no data will be beyond the world bounds
      bq.add(rangeQuery(fieldNameX, null/*-180*/, bbox.getMaxX()), BooleanClause.Occur.SHOULD );
      bq.add(rangeQuery(fieldNameX, bbox.getMinX(), null/*+180*/), BooleanClause.Occur.SHOULD );
      bq.setMinimumNumberShouldMatch(1);//must match at least one of the SHOULD
    } else {
      bq.add(rangeQuery(fieldNameX, bbox.getMinX(), bbox.getMaxX()), MUST);
    }
    bq.add(rangeQuery(fieldNameY, bbox.getMinY(), bbox.getMaxY()), MUST);
    return bq;
  }

  private NumericRangeQuery<Double> rangeQuery(String fieldName, Double min, Double max) {
    return NumericRangeQuery.newDoubleRange(
        fieldName,
        precisionStep,
        min,
        max,
        true,
        true);//inclusive
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   */
  private Query makeDisjoint(Rectangle bbox) {
    if (bbox.getCrossesDateLine())
      throw new UnsupportedOperationException("makeDisjoint doesn't handle dateline cross");
    Query qX = rangeQuery(fieldNameX, bbox.getMinX(), bbox.getMaxX());
    Query qY = rangeQuery(fieldNameY, bbox.getMinY(), bbox.getMaxY());

    BooleanQuery bq = new BooleanQuery();
    bq.add(qX,BooleanClause.Occur.MUST_NOT);
    bq.add(qY,BooleanClause.Occur.MUST_NOT);
    return bq;
  }

}




