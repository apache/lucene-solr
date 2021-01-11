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
package org.apache.solr.client.solrj.io.comp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;


/**
 *  Wraps multiple Comparators to provide sub-sorting.
 **/

public class MultipleFieldComparator implements StreamComparator {

  private static final long serialVersionUID = 1;
  private UUID comparatorNodeId = UUID.randomUUID();

  private StreamComparator[] comps;

  public MultipleFieldComparator(StreamComparator... comps) {
    this.comps = comps;
  }

  public StreamComparator[] getComps(){
    return comps;
  }
  
  public int compare(Tuple t1, Tuple t2) {
    for(StreamComparator comp : comps) {
      int i = comp.compare(t1, t2);
      if(i != 0) {
        return i;
      }
    }

    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MultipleFieldComparator that = (MultipleFieldComparator) o;
    return Arrays.equals(comps, that.comps);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(comps);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StringBuilder sb = new StringBuilder();
    for(StreamComparator comp : comps){
      if(comp instanceof Expressible){
        if(sb.length() > 0){ sb.append(","); }
        sb.append(((Expressible)comp).toExpression(factory));
      }
      else{
        throw new IOException("This MultiComp contains a non-expressible comparator - it cannot be converted to an expression");
      }
    }
    
    return new StreamExpressionValue(sb.toString());
  }
  
  @Override
  public boolean isDerivedFrom(StreamComparator base){
    if(null == base){ return false; }
    if(base instanceof MultipleFieldComparator){
      MultipleFieldComparator baseComp = (MultipleFieldComparator)base;
      
      if(baseComp.comps.length >= comps.length){
        for(int idx = 0; idx < comps.length; ++idx){
          if(!comps[idx].isDerivedFrom(baseComp.comps[idx])){
            return false;
          }
        }
        
        return true;
      }
    }
    
    return false;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(comparatorNodeId.toString())
      .withExpressionType(ExpressionType.SORTER)
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }
  
  @Override
  public MultipleFieldComparator copyAliased(Map<String,String> aliases){
    StreamComparator[] aliasedComps = new StreamComparator[comps.length];
    
    for(int idx = 0; idx < comps.length; ++idx){
      aliasedComps[idx] = comps[idx].copyAliased(aliases);
    }
    
    return new MultipleFieldComparator(aliasedComps);
  }
  
  @Override
  public StreamComparator append(StreamComparator other){
    List<StreamComparator> newComps = new ArrayList<>();
    
    for(StreamComparator comp : comps){
      newComps.add(comp);
    }
    
    if(other instanceof FieldComparator){
      newComps.add(other);
    }
    else if(other instanceof MultipleFieldComparator){
      for(StreamComparator comp : ((MultipleFieldComparator)other).comps){
        newComps.add(comp);
      }
    }
    
    return new MultipleFieldComparator(newComps.toArray(new StreamComparator[newComps.size()]));
  }
}