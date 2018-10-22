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
import java.io.IOException;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;

/**
 * While not strictly a distance, the Sq. Euclidean Distance is often all that is needed in many applications
 * that require a distance, thus saving a sq. rt. calculation
 */
public class SquaredEuclideanFunction extends VectorDistanceFunction {
  protected String name = "sqedist";

  public SquaredEuclideanFunction(MultiValueSource source1, MultiValueSource source2) {
    super(-1, source1, source2);//overriding distance, so power doesn't matter here
  }


  @Override
  protected String name() {

    return name;
  }

  /**
   * @param doc The doc to score
   */
  @Override
  protected double distance(int doc, FunctionValues dv1, FunctionValues dv2) throws IOException {

    double[] vals1 = new double[source1.dimension()];
    double[] vals2 = new double[source1.dimension()];
    dv1.doubleVal(doc, vals1);
    dv2.doubleVal(doc, vals2);

    return distSquaredCartesian(vals1, vals2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SquaredEuclideanFunction)) return false;
    if (!super.equals(o)) return false;

    SquaredEuclideanFunction that = (SquaredEuclideanFunction) o;

    if (!name.equals(that.name)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }
}
