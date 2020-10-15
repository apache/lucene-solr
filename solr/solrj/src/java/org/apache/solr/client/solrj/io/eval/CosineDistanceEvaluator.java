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

package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.List;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.util.Precision;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CosineDistanceEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;

  public CosineDistanceEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  public CosineDistanceEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException{
    super(expression, factory, ignoredNamedParameters);
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {
    return new CosineDistance();
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    // Nothing to do here
    throw new IOException("This call should never occur");
  }

  public static class CosineDistance implements DistanceMeasure {

    private static final long serialVersionUID = -9108154600539125566L;

    public double compute(double[] v1, double[] v2) throws DimensionMismatchException {
      return Precision.round(1-Math.abs(CosineSimilarityEvaluator.cosineSimilarity(v1, v2)), 8);
    }
  }
}