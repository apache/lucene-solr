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
import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class EmpiricalDistributionEvaluator extends RecursiveNumericEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public EmpiricalDistributionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(1 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly one value but found %d",expression,containedEvaluators.size()));
    }
  }
  
  @Override
  public Object doWork(Object value) throws IOException {

    if(!(value instanceof List<?>)){
      throw new StreamEvaluatorException("List value expected but found type %s for value %s", value.getClass().getName(), value.toString());
    }

    EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution();
    
    double[] backingValues = ((List<?>)value).stream().mapToDouble(innerValue -> ((BigDecimal)innerValue).doubleValue()).sorted().toArray();
    empiricalDistribution.load(backingValues);

    return empiricalDistribution;
  }
}

