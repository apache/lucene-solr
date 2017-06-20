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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class AddEvaluator extends NumberEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public AddEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(subEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least 1 value but found %d",expression,subEvaluators.size()));
    }
  }

  @Override
  public Number evaluate(Tuple tuple) throws IOException {
    
    List<BigDecimal> results = evaluateAll(tuple);
    
    if(results.stream().anyMatch(item -> null == item)){
      return null;
    }
    
    BigDecimal result = null;
    if(results.size() > 0){
      result = results.get(0);
      for(int idx = 1; idx < results.size(); ++idx){
        result = result.add(results.get(idx));
      }
    }
    
    return normalizeType(result);
  }
}
