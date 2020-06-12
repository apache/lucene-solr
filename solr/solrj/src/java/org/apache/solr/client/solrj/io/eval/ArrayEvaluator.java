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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ArrayEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  @SuppressWarnings({"rawtypes"})
  private Comparator<Comparable> sortComparator;
  
  @SuppressWarnings({"unchecked"})
  public ArrayEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory, Arrays.asList("sort"));
    
    StreamExpressionNamedParameter sortParam = factory.getNamedOperand(expression, "sort");
    if(null != sortParam && sortParam.getParameter() instanceof StreamExpressionValue){
      String sortOrder = ((StreamExpressionValue)sortParam.getParameter()).getValue().trim().toLowerCase(Locale.ROOT);
      if("asc".equals(sortOrder) || "desc".equals(sortOrder)){
        sortComparator = "asc".equals(sortOrder) ? (left,right) -> left.compareTo(right) : (left,right) -> right.compareTo(left);
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - invalid 'sort' parameter - expecting either 'asc' or 'desc'", expression));
      }
    }    
  }

  @Override
  public Object doWork(Object ... values) throws IOException{
  
    List<Object> newList = Arrays.stream(values).collect(Collectors.toList());
    
    if(null != sortComparator){
      // validate everything is comparable
      for(Object value : newList){
        if(!(value instanceof Comparable<?>)){
          throw new IOException(String.format(Locale.ROOT, "Unable to evaluate because a non-Comparable value ('%s') was found and sorting was requested", value.toString()));
        }
      }
      
      newList = newList.stream().map(value -> (Comparable)value).sorted(sortComparator).collect(Collectors.toList());
    }
    
    return newList;
  }
}
