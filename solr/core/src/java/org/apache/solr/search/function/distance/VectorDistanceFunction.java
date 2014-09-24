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
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.util.Map;


/**
 * Calculate the p-norm for a Vector.  See http://en.wikipedia.org/wiki/Lp_space
 * <p/>
 * Common cases:
 * <ul>
 * <li>0 = Sparseness calculation</li>
 * <li>1 = Manhattan distance</li>
 * <li>2 = Euclidean distance</li>
 * <li>Integer.MAX_VALUE = infinite norm</li>
 * </ul>
 *
 * @see SquaredEuclideanFunction for the special case
 */
public class VectorDistanceFunction extends ValueSource {
  protected MultiValueSource source1, source2;
  protected float power;
  protected float oneOverPower;

  public VectorDistanceFunction(float power, MultiValueSource source1, MultiValueSource source2) {
    if ((source1.dimension() != source2.dimension())) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal number of sources");
    }
    this.power = power;
    this.oneOverPower = 1 / power;
    this.source1 = source1;
    this.source2 = source2;
  }

  protected String name() {
    return "dist";
  }

  /**
   * Calculate the distance
   *
   * @param doc The current doc
   * @param dv1 The values from the first MultiValueSource
   * @param dv2 The values from the second MultiValueSource
   * @return The distance
   */
  protected double distance(int doc, FunctionValues dv1, FunctionValues dv2) {
    //Handle some special cases:
    double[] vals1 = new double[source1.dimension()];
    double[] vals2 = new double[source1.dimension()];
    dv1.doubleVal(doc, vals1);
    dv2.doubleVal(doc, vals2);
    return vectorDistance(vals1, vals2, power, oneOverPower);
  }

  /**
   * Calculate the p-norm (i.e. length) between two vectors.
   * <p/>
   * See <a href="http://en.wikipedia.org/wiki/Lp_space">Lp space</a>
   *
   * @param vec1  The first vector
   * @param vec2  The second vector
   * @param power The power (2 for cartesian distance, 1 for manhattan, etc.)
   * @return The length.
   *
   * @see #vectorDistance(double[], double[], double, double)
   *
   */
  public static double vectorDistance(double[] vec1, double[] vec2, double power) {
    //only calc oneOverPower if it's needed
    double oneOverPower = (power == 0 || power == 1.0 || power == 2.0) ? Double.NaN : 1.0 / power;
    return vectorDistance(vec1, vec2, power, oneOverPower);
  }

  /**
   * Calculate the p-norm (i.e. length) between two vectors.
   *
   * @param vec1         The first vector
   * @param vec2         The second vector
   * @param power        The power (2 for cartesian distance, 1 for manhattan, etc.)
   * @param oneOverPower If you've pre-calculated oneOverPower and cached it, use this method to save
   *                     one division operation over {@link #vectorDistance(double[], double[], double)}.
   * @return The length.
   */
  public static double vectorDistance(double[] vec1, double[] vec2, double power, double oneOverPower) {
    double result = 0;

    if (power == 0) {
      for (int i = 0; i < vec1.length; i++) {
        result += vec1[i] - vec2[i] == 0 ? 0 : 1;
      }
    } else if (power == 1.0) { // Manhattan
      for (int i = 0; i < vec1.length; i++) {
        result += Math.abs(vec1[i] - vec2[i]);
      }
    } else if (power == 2.0) { // Cartesian
      result = Math.sqrt(distSquaredCartesian(vec1, vec2));
    } else if (power == Integer.MAX_VALUE || Double.isInfinite(power)) {//infinite norm?
      for (int i = 0; i < vec1.length; i++) {
        result = Math.max(result, Math.max(vec1[i], vec2[i]));
      }
    } else {
      for (int i = 0; i < vec1.length; i++) {
        result += Math.pow(vec1[i] - vec2[i], power);
      }
      result = Math.pow(result, oneOverPower);
    }
    return result;
  }

  /**
   * The square of the cartesian Distance.  Not really a distance, but useful if all that matters is
   * comparing the result to another one.
   *
   * @param vec1 The first point
   * @param vec2 The second point
   * @return The squared cartesian distance
   */
  public static double distSquaredCartesian(double[] vec1, double[] vec2) {
    double result = 0;
    for (int i = 0; i < vec1.length; i++) {
      double v = vec1[i] - vec2[i];
      result += v * v;
    }
    return result;
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

    final FunctionValues vals1 = source1.getValues(context, readerContext);

    final FunctionValues vals2 = source2.getValues(context, readerContext);


    return new DoubleDocValues(this) {

      @Override
      public double doubleVal(int doc) {
        return distance(doc, vals1, vals2);
      }

      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(').append(power).append(',');
        boolean firstTime = true;
        sb.append(vals1.toString(doc)).append(',');
        sb.append(vals2.toString(doc));
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    source1.createWeight(context, searcher);
    source2.createWeight(context, searcher);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof VectorDistanceFunction)) return false;

    VectorDistanceFunction that = (VectorDistanceFunction) o;

    if (Float.compare(that.power, power) != 0) return false;
    if (!source1.equals(that.source1)) return false;
    if (!source2.equals(that.source2)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = source1.hashCode();
    result = 31 * result + source2.hashCode();
    result = 31 * result + Float.floatToRawIntBits(power);
    return result;
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(').append(power).append(',');
    sb.append(source1).append(',');
    sb.append(source2);
    sb.append(')');
    return sb.toString();
  }

}
