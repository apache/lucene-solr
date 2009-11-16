package org.apache.solr.search.function.distance;
/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Searcher;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;

import java.io.IOException;
import java.util.Map;


/**
 * Calculate the Haversine formula (distance) between any two points on a sphere
 * Takes in four value sources: (latA, lonA); (latB, lonB).
 * <p/>
 * Assumes the value sources are in radians
 * <p/>
 * See http://en.wikipedia.org/wiki/Great-circle_distance and
 * http://en.wikipedia.org/wiki/Haversine_formula for the actual formula and
 * also http://www.movable-type.co.uk/scripts/latlong.html
 *
 * @see org.apache.solr.search.function.RadianFunction
 */
public class HaversineFunction extends ValueSource {

  private ValueSource x1;
  private ValueSource y1;
  private ValueSource x2;
  private ValueSource y2;
  private double radius;

  public HaversineFunction(ValueSource x1, ValueSource y1, ValueSource x2, ValueSource y2, double radius) {
    this.x1 = x1;
    this.y1 = y1;
    this.x2 = x2;
    this.y2 = y2;
    this.radius = radius;
  }

  protected String name() {
    return "hsin";
  }

  /**
   * @param doc  The doc to score
   * @param x1DV
   * @param y1DV
   * @param x2DV
   * @param y2DV
   * @return The haversine distance formula
   */
  protected double distance(int doc, DocValues x1DV, DocValues y1DV, DocValues x2DV, DocValues y2DV) {
    double result = 0;
    double x1 = x1DV.doubleVal(doc); //in radians
    double y1 = y1DV.doubleVal(doc);
    double x2 = x2DV.doubleVal(doc);
    double y2 = y2DV.doubleVal(doc);

    //make sure they aren't all the same, as then we can just return 0
    result = DistanceUtils.haversine(x1, y1, x2, y2, radius);

    return result;
  }


  @Override
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues x1DV = x1.getValues(context, reader);
    final DocValues y1DV = y1.getValues(context, reader);
    final DocValues x2DV = x2.getValues(context, reader);
    final DocValues y2DV = y2.getValues(context, reader);
    return new DocValues() {
      public float floatVal(int doc) {
        return (float) doubleVal(doc);
      }

      public int intVal(int doc) {
        return (int) doubleVal(doc);
      }

      public long longVal(int doc) {
        return (long) doubleVal(doc);
      }

      public double doubleVal(int doc) {
        return (double) distance(doc, x1DV, y1DV, x2DV, y2DV);
      }

      public String strVal(int doc) {
        return Double.toString(doubleVal(doc));
      }

      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        sb.append(x1DV.toString(doc)).append(',').append(y1DV.toString(doc)).append(',')
                .append(x2DV.toString(doc)).append(',').append(y2DV.toString(doc));
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public void createWeight(Map context, Searcher searcher) throws IOException {
    x1.createWeight(context, searcher);
    x2.createWeight(context, searcher);
    y1.createWeight(context, searcher);
    y2.createWeight(context, searcher);
  }

  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    HaversineFunction other = (HaversineFunction) o;
    return this.name().equals(other.name())
            && x1.equals(other.x1) &&
            y1.equals(other.y1) &&
            x2.equals(other.x2) &&
            y2.equals(other.y2);
  }

  public int hashCode() {

    return x1.hashCode() + x2.hashCode() + y1.hashCode() + y2.hashCode() + name().hashCode();
  }

  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name() + '(');
    sb.append(x1).append(',').append(y1).append(',').append(x2).append(',').append(y2);
    sb.append(')');
    return sb.toString();
  }
}
