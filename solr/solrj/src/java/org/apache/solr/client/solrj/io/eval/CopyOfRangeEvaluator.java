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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CopyOfRangeEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public CopyOfRangeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if(Arrays.stream(values).anyMatch(item -> null == item)){
      return null;
    }
    
    List<?> sourceValues;
    Integer startIdx;
    Integer endIdx;
    
    if(values.length >= 1){
      sourceValues = values[0] instanceof List<?> ? (List<?>)values[0] : Arrays.asList(values[0]); 
      
      // default to full array
      startIdx = 0;
      endIdx = sourceValues.size() - 1;
      
      if(values.length >= 2){
        if(values[1] instanceof Number){
          startIdx = ((Number)values[1]).intValue();
        }
        else{
          throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - if second parameter is provided then it must be a valid number but found %s instead",toExpression(constructingFactory), values[1].getClass().getSimpleName()));
        }
        
        if(values.length >= 3){
          if(values[2] instanceof Number){
            endIdx = ((Number)values[2]).intValue();
          }
          else{
            throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - if third parameter is provided then it must be a valid number but found %s instead",toExpression(constructingFactory), values[2].getClass().getSimpleName()));
          }
        }
      }      
    }
    else{
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",toExpression(constructingFactory),containedEvaluators.size()));
    }

    if(startIdx > endIdx){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - startIdx (%d) must be less than endIdx (%d)", toExpression(constructingFactory), startIdx, endIdx));
    }

    if(endIdx > sourceValues.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - endIdx (%d) must not be greater then size of source array (%d)", toExpression(constructingFactory), endIdx, sourceValues.size()));
    }

    return Arrays.stream(Arrays.copyOfRange(sourceValues.toArray(), startIdx, endIdx)).collect(Collectors.toList());
  }
    
}
