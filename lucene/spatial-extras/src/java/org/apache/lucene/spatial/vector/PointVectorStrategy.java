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
package org.apache.lucene.spatial.vector;

import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LegacyDoubleField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.queries.function.FunctionRangeQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Simple {@link SpatialStrategy} which represents Points in two numeric fields.
 * The Strategy's best feature is decent distance sort.
 *
 * <p>
 * <b>Characteristics:</b>
 * <br>
 * <ul>
 * <li>Only indexes points; just one per field value.</li>
 * <li>Can query by a rectangle or circle.</li>
 * <li>{@link
 * org.apache.lucene.spatial.query.SpatialOperation#Intersects} and {@link
 * SpatialOperation#IsWithin} is supported.</li>
 * <li>Requires DocValues for
 * {@link #makeDistanceValueSource(org.locationtech.spatial4j.shape.Point)} and for
 * searching with a Circle.</li>
 * </ul>
 *
 * <p>
 * <b>Implementation:</b>
 * <p>
 * This is a simple Strategy.  Search works with a pair of range queries on two {@link DoublePoint}s representing
 * x &amp; y fields.  A Circle query does the same bbox query but adds a
 * ValueSource filter on
 * {@link #makeDistanceValueSource(org.locationtech.spatial4j.shape.Point)}.
 * <p>
 * One performance shortcoming with this strategy is that a scenario involving
 * both a search using a Circle and sort will result in calculations for the
 * spatial distance being done twice -- once for the filter and second for the
 * sort.
 *
 * @lucene.experimental
 */
public class PointVectorStrategy extends SpatialStrategy {

  // note: we use a FieldType to articulate the options we want on the field.  We don't use it as-is with a Field, we
  //  create more than one Field.

  /**
   * pointValues, docValues, and nothing else.
   */
  public static FieldType DEFAULT_FIELDTYPE;

  @Deprecated
  public static FieldType LEGACY_FIELDTYPE;
  static {
    // Default: pointValues + docValues
    FieldType type = new FieldType();
    type.setDimensions(1, Double.BYTES);//pointValues (assume Double)
    type.setDocValuesType(DocValuesType.NUMERIC);//docValues
    type.setStored(false);
    type.freeze();
    DEFAULT_FIELDTYPE = type;
    // Legacy default: legacyNumerics
    type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS);
    type.setNumericType(FieldType.LegacyNumericType.DOUBLE);
    type.setNumericPrecisionStep(8);// same as solr default
    type.setDocValuesType(DocValuesType.NONE);//no docValues!
    type.setStored(false);
    type.freeze();
    LEGACY_FIELDTYPE = type;
  }

  public static final String SUFFIX_X = "__x";
  public static final String SUFFIX_Y = "__y";

  private final String fieldNameX;
  private final String fieldNameY;

  private final int fieldsLen;
  private final boolean hasStored;
  private final boolean hasDocVals;
  private final boolean hasPointVals;
  // equiv to "hasLegacyNumerics":
  private final FieldType legacyNumericFieldType; // not stored; holds precision step.

  /**
   * Create a new {@link PointVectorStrategy} instance that uses {@link DoublePoint} and {@link DoublePoint#newRangeQuery}
   */
  public static PointVectorStrategy newInstance(SpatialContext ctx, String fieldNamePrefix) {
    return new PointVectorStrategy(ctx, fieldNamePrefix, DEFAULT_FIELDTYPE);
  }

  /**
   * Create a new {@link PointVectorStrategy} instance that uses {@link LegacyDoubleField} for backwards compatibility.
   * However, back-compat is limited; we don't support circle queries or {@link #makeDistanceValueSource(Point, double)}
   * since that requires docValues (the legacy config didn't have that).
   *
   * @deprecated LegacyNumerics will be removed
   */
  @Deprecated
  public static PointVectorStrategy newLegacyInstance(SpatialContext ctx, String fieldNamePrefix) {
    return new PointVectorStrategy(ctx, fieldNamePrefix, LEGACY_FIELDTYPE);
  }

  /**
   * Create a new instance configured with the provided FieldType options. See {@link #DEFAULT_FIELDTYPE}.
   * a field type is used to articulate the desired options (namely pointValues, docValues, stored).  Legacy numerics
   * is configurable this way too.
   */
  public PointVectorStrategy(SpatialContext ctx, String fieldNamePrefix, FieldType fieldType) {
    super(ctx, fieldNamePrefix);
    this.fieldNameX = fieldNamePrefix+SUFFIX_X;
    this.fieldNameY = fieldNamePrefix+SUFFIX_Y;

    int numPairs = 0;
    if ((this.hasStored = fieldType.stored())) {
      numPairs++;
    }
    if ((this.hasDocVals = fieldType.docValuesType() != DocValuesType.NONE)) {
      numPairs++;
    }
    if ((this.hasPointVals = fieldType.pointDimensionCount() > 0)) {
      numPairs++;
    }
    if (fieldType.indexOptions() != IndexOptions.NONE && fieldType.numericType() != null) {
      if (hasPointVals) {
        throw new IllegalArgumentException("pointValues and LegacyNumericType are mutually exclusive");
      }
      if (fieldType.numericType() != FieldType.LegacyNumericType.DOUBLE) {
        throw new IllegalArgumentException(getClass() + " does not support " + fieldType.numericType());
      }
      numPairs++;
      legacyNumericFieldType = new FieldType(LegacyDoubleField.TYPE_NOT_STORED);
      legacyNumericFieldType.setNumericPrecisionStep(fieldType.numericPrecisionStep());
      legacyNumericFieldType.freeze();
    } else {
      legacyNumericFieldType = null;
    }
    this.fieldsLen = numPairs * 2;
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

  /** @see #createIndexableFields(org.locationtech.spatial4j.shape.Shape) */
  public Field[] createIndexableFields(Point point) {
    Field[] fields = new Field[fieldsLen];
    int idx = -1;
    if (hasStored) {
      fields[++idx] = new StoredField(fieldNameX, point.getX());
      fields[++idx] = new StoredField(fieldNameY, point.getY());
    }
    if (hasDocVals) {
      fields[++idx] = new DoubleDocValuesField(fieldNameX, point.getX());
      fields[++idx] = new DoubleDocValuesField(fieldNameY, point.getY());
    }
    if (hasPointVals) {
      fields[++idx] = new DoublePoint(fieldNameX, point.getX());
      fields[++idx] = new DoublePoint(fieldNameY, point.getY());
    }
    if (legacyNumericFieldType != null) {
      fields[++idx] = new LegacyDoubleField(fieldNameX, point.getX(), legacyNumericFieldType);
      fields[++idx] = new LegacyDoubleField(fieldNameY, point.getY(), legacyNumericFieldType);
    }
    assert idx == fields.length - 1;
    return fields;
  }

  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    return new DistanceValueSource(this, queryPoint, multiplier);
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
      Query approxQuery = makeWithin(bbox);
      BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
      FunctionRangeQuery vsRangeQuery =
          new FunctionRangeQuery(makeDistanceValueSource(circle.getCenter()), 0.0, circle.getRadius(), true, true);
      bqBuilder.add(approxQuery, BooleanClause.Occur.FILTER);//should have lowest "cost" value; will drive iteration
      bqBuilder.add(vsRangeQuery, BooleanClause.Occur.FILTER);
      return new ConstantScoreQuery(bqBuilder.build());
    } else {
      throw new UnsupportedOperationException("Only Rectangles and Circles are currently supported, " +
          "found [" + shape.getClass() + "]");//TODO
    }
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   */
  private Query makeWithin(Rectangle bbox) {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
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
    return bq.build();
  }

  /**
   * Returns a numeric range query based on FieldType
   * {@link LegacyNumericRangeQuery} is used for indexes created using {@code FieldType.LegacyNumericType}
   * {@link DoublePoint#newRangeQuery} is used for indexes created using {@link DoublePoint} fields
   */
  private Query rangeQuery(String fieldName, Double min, Double max) {
    if (hasPointVals) {
      if (min == null) {
        min = Double.NEGATIVE_INFINITY;
      }

      if (max == null) {
        max = Double.POSITIVE_INFINITY;
      }

      return DoublePoint.newRangeQuery(fieldName, min, max);

    } else if (legacyNumericFieldType != null) {// todo remove legacy numeric support in 7.0
      return LegacyNumericRangeQuery.newDoubleRange(fieldName, legacyNumericFieldType.numericPrecisionStep(), min, max, true, true);//inclusive
    }
    //TODO try doc-value range query?
    throw new UnsupportedOperationException("An index is required for this operation.");
  }
}
