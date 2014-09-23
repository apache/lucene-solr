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


import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.distance.GeodesicSphereDistCalc;
import com.spatial4j.core.io.GeohashUtils;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;


/**
 *  Calculate the Haversine distance between two geo hash codes.
 *
 * <p/>
 * Ex: ghhsin(ValueSource, ValueSource, radius)
 * <p/>
 *
 * @see org.apache.solr.search.function.distance.HaversineFunction for more details on the implementation
 *
 **/
public class GeohashHaversineFunction extends ValueSource {

  private final ValueSource geoHash1, geoHash2;
  private final SpatialContext ctx;
  private final double degreesToDist;

  public GeohashHaversineFunction(ValueSource geoHash1, ValueSource geoHash2, double radius) {
    this.geoHash1 = geoHash1;
    this.geoHash2 = geoHash2;
    this.degreesToDist = DistanceUtils.degrees2Dist(1, radius);
    this.ctx = SpatialContext.GEO;
    assert this.ctx.getDistCalc() instanceof GeodesicSphereDistCalc.Haversine;
  }

  protected String name() {
    return "ghhsin";
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues gh1DV = geoHash1.getValues(context, readerContext);
    final FunctionValues gh2DV = geoHash2.getValues(context, readerContext);

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        return distance(doc, gh1DV, gh2DV);
      }
      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        sb.append(gh1DV.toString(doc)).append(',').append(gh2DV.toString(doc));
        sb.append(')');
        return sb.toString();
      }
    };
  }

  protected double distance(int doc, FunctionValues gh1DV, FunctionValues gh2DV) {
    double result = 0;
    String h1 = gh1DV.strVal(doc);
    String h2 = gh2DV.strVal(doc);
    if (h1 != null && h2 != null && h1.equals(h2) == false){
      //TODO: If one of the hashes is a literal value source, seems like we could cache it
      //and avoid decoding every time
      Point p1 = GeohashUtils.decode(h1,ctx);
      Point p2 = GeohashUtils.decode(h2,ctx);
      result = ctx.getDistCalc().distance(p1, p2) * degreesToDist;
    } else if (h1 == null || h2 == null){
      result = Double.MAX_VALUE;
    }
    return result;
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    geoHash1.createWeight(context, searcher);
    geoHash2.createWeight(context, searcher);
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    GeohashHaversineFunction other = (GeohashHaversineFunction) o;
    return this.name().equals(other.name())
            && geoHash1.equals(other.geoHash1) &&
            geoHash2.equals(other.geoHash2) &&
            degreesToDist == other.degreesToDist;
  }

  @Override
  public int hashCode() {
    int result;
    result = geoHash1.hashCode();
    result = 31 * result + geoHash2.hashCode();
    result = 31 * result + name().hashCode();
    long temp =Double.doubleToRawLongBits(degreesToDist);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(');
    sb.append(geoHash1).append(',').append(geoHash2);
    sb.append(')');
    return sb.toString();
  }
}
