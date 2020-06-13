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
package org.apache.solr.search.function.distance;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;

import static org.locationtech.spatial4j.distance.DistanceUtils.DEGREES_TO_RADIANS;

/**
 * Haversine function with one point constant
 */
public class HaversineConstFunction extends ValueSource {

  private final double latCenter;
  private final double lonCenter;
  private final VectorValueSource p2;  // lat+lon, just saved for display/debugging
  private final ValueSource latSource;
  private final ValueSource lonSource;

  private final double latCenterRad_cos; // cos(latCenter)
  private static final double EARTH_MEAN_DIAMETER = DistanceUtils.EARTH_MEAN_RADIUS_KM * 2;

  public HaversineConstFunction(double latCenter, double lonCenter, VectorValueSource vs) {
    this.latCenter = latCenter;
    this.lonCenter = lonCenter;
    this.p2 = vs;
    this.latSource = p2.getSources().get(0);
    this.lonSource = p2.getSources().get(1);
    this.latCenterRad_cos = Math.cos(latCenter * DEGREES_TO_RADIANS);
  }

  protected String name() {
    return "geodist";
  }

  @Override
  public FunctionValues getValues(@SuppressWarnings({"rawtypes"})Map context,
                                  LeafReaderContext readerContext) throws IOException {
    @SuppressWarnings({"unchecked"})
    final FunctionValues latVals = latSource.getValues(context, readerContext);
    @SuppressWarnings({"unchecked"})
    final FunctionValues lonVals = lonSource.getValues(context, readerContext);
    final double latCenterRad = this.latCenter * DEGREES_TO_RADIANS;
    final double lonCenterRad = this.lonCenter * DEGREES_TO_RADIANS;
    final double latCenterRad_cos = this.latCenterRad_cos;

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) throws IOException {
        double latRad = latVals.doubleVal(doc) * DEGREES_TO_RADIANS;
        double lonRad = lonVals.doubleVal(doc) * DEGREES_TO_RADIANS;
        double diffX = latCenterRad - latRad;
        double diffY = lonCenterRad - lonRad;
        double hsinX = Math.sin(diffX * 0.5);
        double hsinY = Math.sin(diffY * 0.5);
        double h = hsinX * hsinX +
                (latCenterRad_cos * Math.cos(latRad) * hsinY * hsinY);
        return (EARTH_MEAN_DIAMETER * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h)));
      }
      @Override
      public String toString(int doc) throws IOException {
        return name() + '(' + latVals.toString(doc) + ',' + lonVals.toString(doc) + ',' + latCenter + ',' + lonCenter + ')';
      }
    };
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void createWeight(@SuppressWarnings({"rawtypes"})Map context, IndexSearcher searcher) throws IOException {
    latSource.createWeight(context, searcher);
    lonSource.createWeight(context, searcher);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HaversineConstFunction)) return false;
    HaversineConstFunction other = (HaversineConstFunction) o;
    return this.latCenter == other.latCenter
        && this.lonCenter == other.lonCenter
        && this.p2.equals(other.p2);

  }

  @Override
  public int hashCode() {
    int result = p2.hashCode();
    long temp;
    temp = Double.doubleToRawLongBits(latCenter);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToRawLongBits(lonCenter);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String description() {
    return name() + '(' + p2 + ',' + latCenter + ',' + lonCenter + ')';
  }

}
