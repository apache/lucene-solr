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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.locationtech.spatial4j.distance.DistanceUtils;

/**
 * A point type that indexes a point in an n-dimensional space as separate fields and supports range queries.
 * See {@link LatLonType} for geo-spatial queries.
 */
public class PointType extends CoordinateFieldType implements SpatialQueryable {

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    SolrParams p = new MapSolrParams(args);
    dimension = p.getInt(DIMENSION, DEFAULT_DIMENSION);
    if (dimension < 1) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "The dimension must be > 0: " + dimension);
    }
    args.remove(DIMENSION);
    super.init(schema, args);

    // cache suffixes
    createSuffixCache(dimension);
  }


  @Override
  public boolean isPolyField() {
    return true;   // really only true if the field is indexed
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value, float boost) {
    String externalVal = value.toString();
    String[] point = parseCommaSeparatedList(externalVal, dimension);

    // TODO: this doesn't currently support polyFields as sub-field types
    List<IndexableField> f = new ArrayList<>(dimension+1);

    if (field.indexed()) {
      for (int i=0; i<dimension; i++) {
        SchemaField sf = subField(field, i, schema);
        f.add(sf.createField(point[i], sf.indexed() && !sf.omitNorms() ? boost : 1f));
      }
    }

    if (field.stored()) {
      String storedVal = externalVal;  // normalize or not?
      f.add(createField(field.getName(), storedVal, StoredField.TYPE, 1f));
    }
    
    return f;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    ArrayList<ValueSource> vs = new ArrayList<>(dimension);
    for (int i=0; i<dimension; i++) {
      SchemaField sub = subField(field, i, schema);
      vs.add(sub.getType().getValueSource(sub, parser));
    }
    return new PointTypeValueSource(field, vs);
  }


  /**
   * It never makes sense to create a single field, so make it impossible to happen by
   * throwing UnsupportedOperationException
   *
   */
  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    throw new UnsupportedOperationException("PointType uses multiple fields.  field=" + field.getName());
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on PointType " + field.getName());
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  /**
   * Care should be taken in calling this with higher order dimensions for performance reasons.
   */
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    //Query could look like: [x1,y1 TO x2,y2] for 2 dimension, but could look like: [x1,y1,z1 TO x2,y2,z2], and can be extrapolated to n-dimensions
    //thus, this query essentially creates a box, cube, etc.
    String[] p1 = parseCommaSeparatedList(part1, dimension);
    String[] p2 = parseCommaSeparatedList(part2, dimension);

    BooleanQuery.Builder result = new BooleanQuery.Builder();
    for (int i = 0; i < dimension; i++) {
      SchemaField subSF = subField(field, i, schema);
      // points must currently be ordered... should we support specifying any two opposite corner points?
      result.add(subSF.getType().getRangeQuery(parser, subSF, p1[i], p2[i], minInclusive, maxInclusive), BooleanClause.Occur.MUST);
    }
    return result.build();
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    String[] p1 = parseCommaSeparatedList(externalVal, dimension);
    //TODO: should we assert that p1.length == dimension?
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (int i = 0; i < dimension; i++) {
      SchemaField sf = subField(field, i, schema);
      Query tq = sf.getType().getFieldQuery(parser, sf, p1[i]);
      bq.add(tq, BooleanClause.Occur.MUST);
    }
    return bq.build();
  }

  /**
   * Calculates the range and creates a RangeQuery (bounding box) wrapped in a BooleanQuery (unless the dimension is
   * 1, one range for every dimension, AND'd together by a Boolean
   *
   * @param parser  The parser
   * @param options The {@link org.apache.solr.search.SpatialOptions} for this filter.
   * @return The Query representing the bounding box around the point.
   */
  @Override
  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    String[] pointStrs = parseCommaSeparatedList(options.pointStr, dimension);
    double[] point = new double[dimension];
    try {
      for (int i = 0; i < pointStrs.length; i++) {
        point[i] = Double.parseDouble(pointStrs[i]);
      }
    } catch (NumberFormatException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
    IndexSchema schema = parser.getReq().getSchema();

    if (dimension == 1){
      //TODO: Handle distance measures
      String lower = String.valueOf(point[0] - options.distance);
      String upper = String.valueOf(point[0] + options.distance);
      SchemaField subSF = subField(options.field, 0, schema);
      // points must currently be ordered... should we support specifying any two opposite corner points?
      return subSF.getType().getRangeQuery(parser, subSF, lower, upper, true, true);
    } else {
      BooleanQuery.Builder tmp = new BooleanQuery.Builder();
      //TODO: Handle distance measures, as this assumes Euclidean
      double[] ur = vectorBoxCorner(point, null, options.distance, true);
      double[] ll = vectorBoxCorner(point, null, options.distance, false);
      for (int i = 0; i < ur.length; i++) {
        SchemaField subSF = subField(options.field, i, schema);
        Query range = subSF.getType().getRangeQuery(parser, subSF, String.valueOf(ll[i]), String.valueOf(ur[i]), true, true);
        tmp.add(range, BooleanClause.Occur.MUST);
      }
      return tmp.build();
    }
  }

  private static final double SIN_PI_DIV_4 = Math.sin(Math.PI / 4);

  /**
   * Return the coordinates of a vector that is the corner of a box (upper right or lower left), assuming a Rectangular
   * coordinate system.  Note, this does not apply for points on a sphere or ellipse (although it could be used as an
   * approximation).
   *
   * @param center     The center point
   * @param result     Holds the result, potentially resizing if needed.
   * @param distance   The d from the center to the corner
   * @param upperRight If true, return the coords for the upper right corner, else return the lower left.
   * @return The point, either the upperLeft or the lower right
   */
  public static double[] vectorBoxCorner(double[] center, double[] result, double distance, boolean upperRight) {
    if (result == null || result.length != center.length) {
      result = new double[center.length];
    }
    if (upperRight == false) {
      distance = -distance;
    }
    //We don't care about the power here,
    // b/c we are always in a rectangular coordinate system, so any norm can be used by
    //using the definition of sine
    distance = SIN_PI_DIV_4 * distance; // sin(Pi/4) == (2^0.5)/2 == opp/hyp == opp/distance, solve for opp, similarly for cosine
    for (int i = 0; i < center.length; i++) {
      result[i] = center[i] + distance;
    }
    return result;
  }

  /**
   * Given a string containing <i>dimension</i> values encoded in it, separated by commas,
   * return a String array of length <i>dimension</i> containing the values.
   *
   * @param externalVal The value to parse
   * @param dimension   The expected number of values for the point
   * @return An array of the values that make up the point (aka vector)
   * @throws SolrException if the dimension specified does not match the number found
   */
  public static String[] parseCommaSeparatedList(String externalVal, int dimension) throws SolrException {
    //TODO: Should we support sparse vectors?
    String[] out = new String[dimension];
    int idx = externalVal.indexOf(',');
    int end = idx;
    int start = 0;
    int i = 0;
    if (idx == -1 && dimension == 1 && externalVal.length() > 0) {//we have a single point, dimension better be 1
      out[0] = externalVal.trim();
      i = 1;
    } else if (idx > 0) {//if it is zero, that is an error
      //Parse out a comma separated list of values, as in: 73.5,89.2,7773.4
      for (; i < dimension; i++) {
        while (start < end && externalVal.charAt(start) == ' ') start++;
        while (end > start && externalVal.charAt(end - 1) == ' ') end--;
        if (start == end) {
          break;
        }
        out[i] = externalVal.substring(start, end);
        start = idx + 1;
        end = externalVal.indexOf(',', start);
        idx = end;
        if (end == -1) {
          end = externalVal.length();
        }
      }
    }
    if (i != dimension) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "incompatible dimension (" + dimension +
              ") and values (" + externalVal + ").  Only " + i + " values specified");
    }
    return out;
  }

  @Override
  public double getSphereRadius() {
    // This won't likely be used. You should probably be using LatLonType instead if you felt the need for this.
    // This is here just for backward compatibility reasons.
    return DistanceUtils.EARTH_MEAN_RADIUS_KM;
  }

}


class PointTypeValueSource extends VectorValueSource {
  private final SchemaField sf;
  
  public PointTypeValueSource(SchemaField sf, List<ValueSource> sources) {
    super(sources);
    this.sf = sf;
  }

  @Override
  public String name() {
    return "point";
  }

  @Override
  public String description() {
    return name()+"("+sf.getName()+")";
  }
}
