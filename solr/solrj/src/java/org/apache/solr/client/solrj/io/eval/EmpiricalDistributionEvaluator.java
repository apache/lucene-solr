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

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class EmpiricalDistributionEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  private int bins = 99;
  
  public EmpiricalDistributionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(2 != containedEvaluators.size() && 1 != containedEvaluators.size()) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one or two values but found %d",expression,containedEvaluators.size()));
    }
  }
  
  @Override
  public Object doWork(Object... values) throws IOException {

    if(!(values[0] instanceof List<?>)){
      throw new StreamEvaluatorException("List value expected but found type %s for value %s", values[0].getClass().getName(), values[0].toString());
    }

    if(values.length == 2) {
      Number n = (Number)values[1];
      bins = n.intValue();
    }

    EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution(bins);
    
    double[] backingValues = ((List<?>)values[0]).stream().mapToDouble(innerValue -> ((Number)innerValue).doubleValue()).sorted().toArray();

    empiricalDistribution.load(backingValues);

    return empiricalDistribution;
  }
}

