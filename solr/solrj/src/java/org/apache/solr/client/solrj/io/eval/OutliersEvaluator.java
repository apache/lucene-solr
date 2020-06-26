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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.Tuple;

public class OutliersEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public OutliersEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object doWork(Object... values) throws IOException{

    if(values.length < 4) {
      throw new IOException("The outliers function requires 4 parameters");
    }

    Object dist = values[0];
    List<Number> vec = null;
    if(values[1] instanceof List) {
      vec = (List<Number>)values[1];
    } else {
      throw new IOException("The second parameter of the outliers function is the numeric array to be tested for outliers.");
    }

    double low = 0.0;

    if(values[2] instanceof Number) {
      low = ((Number)values[2]).doubleValue();
    } else {
      throw new IOException("The third parameter of the outliers function is a number for the low outlier threshold.");
    }

    double hi = 0.0;

    if(values[3] instanceof Number) {
      hi = ((Number)values[3]).doubleValue();
    } else {
      throw new IOException("The fourth parameter of the outliers function is a number for the high outlier threshold");
    }

    List<Tuple> tuples = null;

    if(values.length ==5) {
      if(values[4] instanceof List) {
        tuples = (List<Tuple>) values[4];
      } else {
        throw new IOException("The optional fifth parameter of the outliers function is an array of Tuples that are paired with the numeric array of values to be tested.");
      }
    } else {
      tuples = new ArrayList<>();
      for(int i=0; i<vec.size(); i++) {
        tuples.add(new Tuple());
      }
    }

    List<Tuple> outliers = new ArrayList<>();

    if(dist instanceof IntegerDistribution) {

      IntegerDistribution d = (IntegerDistribution) dist;

      for(int i=0; i<vec.size(); i++) {

        Number n = vec.get(i);
        Tuple t = tuples.get(i);

        double cumProb = d.cumulativeProbability(n.intValue());
        if(low >= 0 && cumProb <= low) {
          t.put("lowOutlierValue_d", n);
          t.put("cumulativeProbablity_d", cumProb);
          outliers.add(t);
        }

        if(hi >= 0 && cumProb >= hi) {
          t.put("highOutlierValue_d", n);
          t.put("cumulativeProbablity_d", cumProb);
          outliers.add(t);
        }
      }

      return outliers;

    } else if(dist instanceof AbstractRealDistribution) {

      AbstractRealDistribution d = (AbstractRealDistribution)dist;
      for(int i=0; i<vec.size(); i++) {

        Number n = vec.get(i);
        Tuple t = tuples.get(i);

        double cumProb = d.cumulativeProbability(n.doubleValue());
        if(low >= 0 && cumProb <= low) {
          t.put("lowOutlierValue_d", n);
          t.put("cumulativeProbablity_d", cumProb);
          outliers.add(t);

        }

        if(hi >= 0 && cumProb >= hi) {
          t.put("highOutlierValue_d", n);
          t.put("cumulativeProbablity_d", cumProb);
          outliers.add(t);
        }
      }

      return outliers;
    } else {
      throw new IOException("The first parameter of the outliers function must be a real or integer probability distribution");
    }
  }
}
