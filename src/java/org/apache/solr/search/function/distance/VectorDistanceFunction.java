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
import org.apache.solr.common.SolrException;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;

import java.io.IOException;
import java.util.List;
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
  protected List<ValueSource> sources1, sources2;
  protected float power;
  protected float oneOverPower;

  public VectorDistanceFunction(float power, List<ValueSource> sources1, List<ValueSource> sources2) {
    this.power = power;
    this.oneOverPower = 1 / power;
    this.sources1 = sources1;
    this.sources2 = sources2;
    if ((sources1.size() != sources2.size())) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal number of sources");
    }
  }

  protected String name() {
    return "dist";
  };

  /**
   * Calculate the distance
   *
   * @param doc        The current doc
   * @param docValues1 The values from the first set of value sources
   * @param docValues2 The values from the second set of value sources
   * @return The distance
   */
  protected double distance(int doc, DocValues[] docValues1, DocValues[] docValues2) {
    double result = 0;
    //Handle some special cases:
    if (power == 0) {
      for (int i = 0; i < docValues1.length; i++) {
        //sparseness measure
        result += docValues1[i].doubleVal(doc) - docValues2[i].doubleVal(doc) == 0 ? 0 : 1;
      }
    } else if (power == 1.0) {
      for (int i = 0; i < docValues1.length; i++) {
        result += docValues1[i].doubleVal(doc) - docValues2[i].doubleVal(doc);
      }
    } else if (power == 2.0) {
      for (int i = 0; i < docValues1.length; i++) {
        double v = docValues1[i].doubleVal(doc) - docValues2[i].doubleVal(doc);
        result += v * v;
      }
      result = Math.sqrt(result);
    } else if (power == Integer.MAX_VALUE || Double.isInfinite(power)) {//infininte norm?
      for (int i = 0; i < docValues1.length; i++) {
        //TODO: is this the correct infinite norm?
        result = Math.max(docValues1[i].doubleVal(doc) - docValues2[i].doubleVal(doc), result);
      }

    } else {
      for (int i = 0; i < docValues1.length; i++) {
        result += Math.pow(docValues1[i].doubleVal(doc) - docValues2[i].doubleVal(doc), power);
      }
      result = Math.pow(result, oneOverPower);
    }

    return result;
  }

  @Override
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues[] valsArr1 = new DocValues[sources1.size()];
    int i = 0;
    for (ValueSource source : sources1) {
      valsArr1[i++] = source.getValues(context, reader);
    }
    final DocValues[] valsArr2 = new DocValues[sources2.size()];
    i = 0;
    for (ValueSource source : sources2) {
      valsArr2[i++] = source.getValues(context, reader);
    }


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
        return (double) distance(doc, valsArr1, valsArr2);
      }

      public String strVal(int doc) {
        return Double.toString(doubleVal(doc));
      }

      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(').append(power).append(',');
        boolean firstTime = true;
        for (DocValues vals : valsArr1) {
          if (firstTime) {
            firstTime = false;
          } else {
            sb.append(',');
          }
          sb.append(vals.toString(doc));
        }
        for (DocValues vals : valsArr2) {
          sb.append(',');//we will always have valsArr1, else there is an error
          sb.append(vals.toString(doc));
        }
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public void createWeight(Map context, Searcher searcher) throws IOException {
    for (ValueSource source : sources1) {
      source.createWeight(context, searcher);
    }
    for (ValueSource source : sources2) {
      source.createWeight(context, searcher);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof VectorDistanceFunction)) return false;

    VectorDistanceFunction that = (VectorDistanceFunction) o;

    if (Float.compare(that.power, power) != 0) return false;
    if (!sources1.equals(that.sources1)) return false;
    if (!sources2.equals(that.sources2)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = sources1.hashCode();
    result = 31 * result + sources2.hashCode();
    result = 31 * result + (power != +0.0f ? Float.floatToIntBits(power) : 0);
    return result;
  }

  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(').append(power).append(',');
    boolean firstTime = true;
    for (ValueSource source : sources1) {
      if (firstTime) {
        firstTime = false;
      } else {
        sb.append(',');
      }
      sb.append(source);
    }
    for (ValueSource source : sources2) {
      sb.append(',');//we will always have sources1, else there is an error
      sb.append(source);
    }
    sb.append(')');
    return sb.toString();
  }

}
