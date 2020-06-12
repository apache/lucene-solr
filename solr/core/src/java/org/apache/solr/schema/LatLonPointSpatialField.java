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

package org.apache.solr.schema;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.solr.common.SolrException;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import static java.math.RoundingMode.CEILING;

/**
 * A spatial implementation based on Lucene's {@code LatLonPoint} and {@code LatLonDocValuesField}. The
 * first is based on Lucene's "Points" API, which is a BKD Index.  This field type is strictly limited to
 * coordinates in lat/lon decimal degrees.  The accuracy is about a centimeter (1.042cm).
 */
// TODO once LLP & LLDVF are out of Lucene Sandbox, we should be able to javadoc reference them.
@SuppressWarnings({"rawtypes"})
public class LatLonPointSpatialField extends AbstractSpatialFieldType implements SchemaAware {
  private IndexSchema schema;

  // TODO handle polygons

  @Override
  protected void checkSupportsDocValues() { // we support DocValues
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
  }

  @Override
  protected SpatialStrategy newSpatialStrategy(String fieldName) {
    SchemaField schemaField = schema.getField(fieldName); // TODO change AbstractSpatialFieldType so we get schemaField?
    return new LatLonPointSpatialStrategy(ctx, fieldName, schemaField.indexed(), schemaField.hasDocValues());
  }

  @Override
  public String toExternal(IndexableField f) {
    if (f.numericValue() != null) {
      return decodeDocValueToString(f.numericValue().longValue());
    }
    return super.toExternal(f);
  }

  /**
   * Decodes the docValues number into latitude and longitude components, formatting as "lat,lon".
   * The encoding is governed by {@code LatLonDocValuesField}.  The decimal output representation is reflective
   * of the available precision.
   * @param value Non-null; stored location field data
   * @return Non-null; "lat, lon"
   */
  public static String decodeDocValueToString(long value) {
    final double latDouble = GeoEncodingUtils.decodeLatitude((int) (value >> 32));
    final double lonDouble = GeoEncodingUtils.decodeLongitude((int) (value & 0xFFFFFFFFL));
    // This # decimal places gets us close to our available precision to 1.40cm; we have a test for it.
    // CEILING round-trips (decode then re-encode then decode to get identical results). Others did not. It also
    //   reverses the "floor" that occurred when we encoded.
    final int DECIMAL_PLACES = 7;
    final RoundingMode ROUND_MODE = CEILING;
    BigDecimal latitudeDecoded = BigDecimal.valueOf(latDouble).setScale(DECIMAL_PLACES, ROUND_MODE);
    BigDecimal longitudeDecoded = BigDecimal.valueOf(lonDouble).setScale(DECIMAL_PLACES, ROUND_MODE);
    return latitudeDecoded.stripTrailingZeros().toPlainString() + ","
        + longitudeDecoded.stripTrailingZeros().toPlainString();
    // return ((float)latDouble) + "," + ((float)lonDouble);  crude but not quite as accurate
  }

  // TODO move to Lucene-spatial-extras once LatLonPoint & LatLonDocValuesField moves out of sandbox
  public static class LatLonPointSpatialStrategy extends SpatialStrategy {

    private final boolean indexed; // for query/filter
    private final boolean docValues; // for sort. Can be used to query/filter.

    public LatLonPointSpatialStrategy(SpatialContext ctx, String fieldName, boolean indexed, boolean docValues) {
      super(ctx, fieldName);
      if (!ctx.isGeo()) {
        throw new IllegalArgumentException("ctx must be geo=true: " + ctx);
      }
      this.indexed = indexed;
      this.docValues = docValues;
    }

    @Override
    public Field[] createIndexableFields(Shape shape) {
      if (!(shape instanceof Point)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            getClass().getSimpleName() + " only supports indexing points; got: " + shape);
      }
      Point point = (Point) shape;

      int fieldsLen = (indexed ? 1 : 0) + (docValues ? 1 : 0);
      Field[] fields = new Field[fieldsLen];
      int fieldsIdx = 0;
      if (indexed) {
        fields[fieldsIdx++] = new LatLonPoint(getFieldName(), point.getY(), point.getX());
      }
      if (docValues) {
        fields[fieldsIdx++] = new LatLonDocValuesField(getFieldName(), point.getY(), point.getX());
      }
      return fields;
    }

    @Override
    public Query makeQuery(SpatialArgs args) {
      if (args.getOperation() != SpatialOperation.Intersects) {
        throw new UnsupportedSpatialOperation(args.getOperation());
      }
      Shape shape = args.getShape();
      if (indexed && docValues) {
        return new IndexOrDocValuesQuery(makeQueryFromIndex(shape), makeQueryFromDocValues(shape));
      } else if (indexed) {
        return makeQueryFromIndex(shape);
      } else if (docValues) {
        return makeQueryFromDocValues(shape);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            getFieldName() + " needs indexed (preferred) or docValues to support search");
      }
    }

    // Uses LatLonPoint
    protected Query makeQueryFromIndex(Shape shape) {
      // note: latitude then longitude order for LLP's methods
      if (shape instanceof Circle) {
        Circle circle = (Circle) shape;
        double radiusMeters = circle.getRadius() * DistanceUtils.DEG_TO_KM * 1000;
        return LatLonPoint.newDistanceQuery(getFieldName(),
            circle.getCenter().getY(), circle.getCenter().getX(),
            radiusMeters);
      } else if (shape instanceof Rectangle) {
        Rectangle rect = (Rectangle) shape;
        return LatLonPoint.newBoxQuery(getFieldName(),
            rect.getMinY(), rect.getMaxY(), rect.getMinX(), rect.getMaxX());
      } else if (shape instanceof Point) {
        Point point = (Point) shape;
        return LatLonPoint.newDistanceQuery(getFieldName(),
            point.getY(), point.getX(), 0);
      } else {
        throw new UnsupportedOperationException("Shape " + shape.getClass() + " is not supported by " + getClass());
      }
//      } else if (shape instanceof LucenePolygonShape) {
//        // TODO support multi-polygon
//        Polygon poly = ((LucenePolygonShape)shape).lucenePolygon;
//        return LatLonPoint.newPolygonQuery(getFieldName(), poly);
    }

    // Uses DocValuesField  (otherwise identical to above)
    protected Query makeQueryFromDocValues(Shape shape) {
      // note: latitude then longitude order for LLP's methods
      if (shape instanceof Circle) {
        Circle circle = (Circle) shape;
        double radiusMeters = circle.getRadius() * DistanceUtils.DEG_TO_KM * 1000;
        return LatLonDocValuesField.newSlowDistanceQuery(getFieldName(),
            circle.getCenter().getY(), circle.getCenter().getX(),
            radiusMeters);
      } else if (shape instanceof Rectangle) {
        Rectangle rect = (Rectangle) shape;
        return LatLonDocValuesField.newSlowBoxQuery(getFieldName(),
            rect.getMinY(), rect.getMaxY(), rect.getMinX(), rect.getMaxX());
      } else if (shape instanceof Point) {
        Point point = (Point) shape;
        return LatLonDocValuesField.newSlowDistanceQuery(getFieldName(),
            point.getY(), point.getX(), 0);
      } else {
        throw new UnsupportedOperationException("Shape " + shape.getClass() + " is not supported by " + getClass());
      }
//      } else if (shape instanceof LucenePolygonShape) {
//        // TODO support multi-polygon
//        Polygon poly = ((LucenePolygonShape)shape).lucenePolygon;
//        return LatLonPoint.newPolygonQuery(getFieldName(), poly);
    }

    @Override
    public DoubleValuesSource makeDistanceValueSource(Point queryPoint, double multiplier) {
      if (!docValues) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            getFieldName() + " must have docValues enabled to support this feature");
      }
      // Internally, the distance from LatLonPointSortField/Comparator is in meters. So we must also go from meters to
      //  degrees, which is what Lucene spatial-extras is oriented around.
      return new DistanceSortValueSource(getFieldName(), queryPoint,
          DistanceUtils.KM_TO_DEG / 1000.0 * multiplier);
    }

    /**
     * A {@link ValueSource} based around {@code LatLonDocValuesField#newDistanceSort(String, double, double)}.
     */
    private static class DistanceSortValueSource extends DoubleValuesSource {
      private final String fieldName;
      private final Point queryPoint;
      private final double multiplier;

      DistanceSortValueSource(String fieldName, Point queryPoint, double multiplier) {
        this.fieldName = fieldName;
        this.queryPoint = queryPoint;
        this.multiplier = multiplier;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistanceSortValueSource that = (DistanceSortValueSource) o;
        return Double.compare(that.multiplier, multiplier) == 0 &&
            Objects.equals(fieldName, that.fieldName) &&
            Objects.equals(queryPoint, that.queryPoint);
      }

      @Override
      public int hashCode() {
        return Objects.hash(fieldName, queryPoint, multiplier);
      }

      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        return new DoubleValues() {

          @SuppressWarnings("unchecked")
          final FieldComparator<Double> comparator =
              (FieldComparator<Double>) getSortField(false).getComparator(1, 1);
          final LeafFieldComparator leafComparator = comparator.getLeafComparator(ctx);
          final double mult = multiplier; // so it's a local field

          double value = Double.POSITIVE_INFINITY;

          @Override
          public double doubleValue() throws IOException {
            return value;
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            leafComparator.copy(0, doc);
            value = comparator.value(0) * mult;
            return true;
          }
        };
      }

      @Override
      public boolean needsScores() {
        return false;
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, fieldName);
      }

      @Override
      public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
        return this;
      }

      @Override
      public String toString() {
        return "distSort(" + fieldName + ", " + queryPoint + ", mult:" + multiplier + ")";
      }

      @Override
      public SortField getSortField(boolean reverse) {
        if (reverse) {
          return super.getSortField(true); // will use an impl that calls getValues
        }
        return LatLonDocValuesField.newDistanceSort(fieldName, queryPoint.getY(), queryPoint.getX());
      }

    }

  }

}