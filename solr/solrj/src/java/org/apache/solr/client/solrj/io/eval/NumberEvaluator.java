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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class NumberEvaluator extends ComplexEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public NumberEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }
  
  // restrict result to a Number
  public abstract Number evaluate(Tuple tuple) throws IOException;
  
  public List<BigDecimal> evaluateAll(final Tuple tuple) throws IOException {
    // evaluate each and confirm they are all either null or numeric
    List<BigDecimal> results = new ArrayList<BigDecimal>();
    for(StreamEvaluator subEvaluator : subEvaluators){
      Object result = subEvaluator.evaluate(tuple);
      
      if(null == result){
        results.add(null);
      }
      else if(result instanceof Number){
        results.add(new BigDecimal(result.toString()));
      } else if(result instanceof List) {
        List l = (List) result;
        for(Object o : l) {
          if(o instanceof Number) {
            results.add(new BigDecimal(o.toString()));
          } else {
            String message = String.format(Locale.ROOT,"Failed to evaluate to a numeric value - evaluator '%s' resulted in type '%s' and value '%s'",
                                           subEvaluator.toExpression(constructingFactory),
                                           o.getClass().getName(),
                                           o.toString());
            throw new IOException(message);
          }
        }
      }
      else{
        String message = String.format(Locale.ROOT,"Failed to evaluate to a numeric value - evaluator '%s' resulted in type '%s' and value '%s'", 
                                        subEvaluator.toExpression(constructingFactory),
                                        result.getClass().getName(),
                                        result.toString());
        throw new IOException(message);
      }
    }
    
    return results;
  }
  
  public Number normalizeType(BigDecimal value){
    if(null == value){
      return null;
    }
    
    if(value.signum() == 0 || value.scale() <= 0 || value.stripTrailingZeros().scale() <= 0){
      return value.longValue();
    }
    
    return value.doubleValue();

  }

}
