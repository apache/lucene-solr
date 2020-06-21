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
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class AscEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  private static final long serialVersionUID = 1;

  public AscEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object value) throws IOException {
    if(null == value){
      return value;
    }
    else if(!(value instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for value, expecting a List",toExpression(constructingFactory), value.getClass().getSimpleName()));
    }
    
    List<?> list = (List<?>)value;
    
    if(0 == list.size()){
      return list;
    }

    // Validate all of same type and are comparable
    Object checkingObject = list.get(0);
    for(int idx = 0; idx < list.size(); ++idx){
      Object item = list.get(0);
      
      if(null == item){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found null value",toExpression(constructingFactory)));
      }
      else if(!(item instanceof Comparable<?>)){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found non-comparable value %s of type %s",toExpression(constructingFactory), item.toString(), item.getClass().getSimpleName()));
      }
      else if(!item.getClass().getCanonicalName().equals(checkingObject.getClass().getCanonicalName())){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - value %s is of type %s but we are expeting type %s",toExpression(constructingFactory), item.toString(), item.getClass().getSimpleName(), checkingObject.getClass().getCanonicalName()));
      }
    }

    return list.stream().sorted((left,right) -> ((Comparable)left).compareTo((Comparable)right)).collect(Collectors.toList());
  }
}