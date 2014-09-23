package org.apache.solr.search.function.distance;
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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.search.IndexSearcher;
import com.spatial4j.core.distance.DistanceUtils;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.util.Map;


/**
 * Calculate the Haversine formula (distance) between any two points on a sphere
 * Takes in four value sources: (latA, lonA); (latB, lonB).
 * <p/>
 * Assumes the value sources are in radians unless
 * <p/>
 * See http://en.wikipedia.org/wiki/Great-circle_distance and
 * http://en.wikipedia.org/wiki/Haversine_formula for the actual formula and
 * also http://www.movable-type.co.uk/scripts/latlong.html
 */
public class HaversineFunction extends ValueSource {

  private MultiValueSource p1;
  private MultiValueSource p2;
  private boolean convertToRadians = false;
  private double radius;

  public HaversineFunction(MultiValueSource p1, MultiValueSource p2, double radius) {
    this(p1, p2, radius, false);
  }

  public HaversineFunction(MultiValueSource p1, MultiValueSource p2, double radius, boolean convertToRads){
    this.p1 = p1;
    this.p2 = p2;
    if (p1.dimension() != 2 || p2.dimension() != 2) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal dimension for value sources");
    }
    this.radius = radius;
    this.convertToRadians = convertToRads;
  }

  protected String name() {
    return "hsin";
  }

  /**
   * @param doc  The doc to score
   * @return The haversine distance formula
   */
  protected double distance(int doc, FunctionValues p1DV, FunctionValues p2DV) {

    double[] p1D = new double[2];
    double[] p2D = new double[2];
    p1DV.doubleVal(doc, p1D);
    p2DV.doubleVal(doc, p2D);
    double y1;
    double x1;
    double y2;
    double x2;
    if (convertToRadians) {
      y1 = p1D[0] * DistanceUtils.DEGREES_TO_RADIANS;
      x1 = p1D[1] * DistanceUtils.DEGREES_TO_RADIANS;
      y2 = p2D[0] * DistanceUtils.DEGREES_TO_RADIANS;
      x2 = p2D[1] * DistanceUtils.DEGREES_TO_RADIANS;
    } else {
      y1 = p1D[0];
      x1 = p1D[1];
      y2 = p2D[0];
      x2 = p2D[1];
    }
    return DistanceUtils.distHaversineRAD(y1,x1,y2,x2)*radius;
  }


  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals1 = p1.getValues(context, readerContext);

    final FunctionValues vals2 = p2.getValues(context, readerContext);
    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        return distance(doc, vals1, vals2);
      }
      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        sb.append(vals1.toString(doc)).append(',').append(vals2.toString(doc));
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    p1.createWeight(context, searcher);
    p2.createWeight(context, searcher);

  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    HaversineFunction other = (HaversineFunction) o;
    return this.name().equals(other.name())
            && p1.equals(other.p1) &&
            p2.equals(other.p2) && radius == other.radius;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = p1.hashCode();
    result = 31 * result + p2.hashCode();
    result = 31 * result + name().hashCode();
    temp = Double.doubleToRawLongBits(radius);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(');
    sb.append(p1).append(',').append(p2);
    sb.append(')');
    return sb.toString();
  }
}
