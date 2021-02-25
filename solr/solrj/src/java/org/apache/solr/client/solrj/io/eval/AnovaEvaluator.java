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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.inference.OneWayAnova;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.StreamParams;

public class AnovaEvaluator extends RecursiveNumericListEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public AnovaEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    
    // at this point we know every incoming value is an array of BigDecimals
    
    @SuppressWarnings({"unchecked"})
    List<double[]> anovaInput = Arrays.stream(values)
        // for each List, convert to double[]
        .map(value -> ((List<Number>)value).stream().mapToDouble(Number::doubleValue).toArray())
        // turn into List<double[]>
        .collect(Collectors.toList());
    
    OneWayAnova anova = new OneWayAnova();
    double p = anova.anovaPValue(anovaInput);
    double f = anova.anovaFValue(anovaInput);
    Tuple tuple = new Tuple();
    tuple.put(StreamParams.P_VALUE, p);
    tuple.put("f-ratio", f);
    return tuple;
  }
  

}
