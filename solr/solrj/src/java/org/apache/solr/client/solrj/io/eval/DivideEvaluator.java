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
import java.math.MathContext;
import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DivideEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public DivideEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two values but found %d",expression, containedEvaluators.size()));
    }
  }

  // override to give a slightly better error message than the default
  @Override
  public Object doWork(Object ... values) throws IOException{
    if(2 != values.length){
      String message = null;
      if(1 == values.length){
        message = String.format(Locale.ROOT,"%s(...) only works with a 2 values (numerator,denominator) but 1 was provided", constructingFactory.getFunctionName(getClass())); 
      }
      else{
        message = String.format(Locale.ROOT,"%s(...) only works with a 2 values (numerator,denominator) but %d were provided", constructingFactory.getFunctionName(getClass()), values.length);
      }
      throw new IOException(message);
    }
    
    return doWork(values[0], values[1]);
  }


  @Override
  public Object doWork(Object first, Object second) throws IOException{
    if(null == first){
      throw new IOException(String.format(Locale.ROOT,"Unable to %s(...) with a null numerator", constructingFactory.getFunctionName(getClass())));
    }
    
    if(null == second){
      throw new IOException(String.format(Locale.ROOT,"Unable to %s(...) with a null denominator", constructingFactory.getFunctionName(getClass())));
    }

    BigDecimal numerator = (BigDecimal)first;
    BigDecimal denominator = (BigDecimal)second;
        
    if(0 == denominator.compareTo(BigDecimal.ZERO)){
      throw new IOException(String.format(Locale.ROOT,"Unable to %s(...) with a 0 denominator", constructingFactory.getFunctionName(getClass())));
    }
    
    return numerator.divide(denominator, MathContext.DECIMAL64);
  }
}
