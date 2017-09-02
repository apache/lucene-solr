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
import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class EqualToEvaluator extends RecursiveBooleanEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public EqualToEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two values but found %d",expression,containedEvaluators.size()));
    }
  }
  
  protected Checker constructChecker(Object fromValue) throws IOException{
    if(null == fromValue){
      return new NullChecker() {
        @Override
        public boolean test(Object left, Object right) {
          return null == left && null == right;
        }
      };
    }
    else if(fromValue instanceof Boolean){
      return new BooleanChecker(){
        @Override
        public boolean test(Object left, Object right) {
          return (boolean)left == (boolean)right;
        }
      };
    }
    else if(fromValue instanceof Number){
      return new NumberChecker(){
        @Override
        public boolean test(Object left, Object right) {
          return 0 == (new BigDecimal(left.toString())).compareTo(new BigDecimal(right.toString()));
        }
      };
    }
    else if(fromValue instanceof String){
      return new StringChecker(){
        @Override
        public boolean test(Object left, Object right) {
          return left.equals(right);
        }
      };
    }
    
    throw new IOException(String.format(Locale.ROOT,"Unable to check %s(...) for values of type '%s'", constructingFactory.getFunctionName(getClass()), fromValue.getClass().getSimpleName()));
  }
}
