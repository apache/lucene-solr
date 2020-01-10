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
import java.util.Locale;

import org.apache.commons.math3.util.Precision;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CosineSimilarityEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public CosineSimilarityEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException{
    if(null == first){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }
    if(null == second){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the second value",toExpression(constructingFactory)));
    }
    if(!(first instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the first value, expecting a list of numbers",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
    if(!(second instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the second value, expecting a list of numbers",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }

    double[] d1 = ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();
    double[] d2 = ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray();

    return cosineSimilarity(d1, d2);
  }

  public static double cosineSimilarity(double[] vectorA, double[] vectorB) {
    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += Math.pow(vectorA[i], 2);
      normB += Math.pow(vectorB[i], 2);
    }
    double d = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    return Precision.round(d, 8);
  }

}
