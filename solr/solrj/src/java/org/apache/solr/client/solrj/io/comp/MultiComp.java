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
import java.io.Serializable;
import java.util.Comparator;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


/**
 *  Wraps multiple Comparators to provide sub-sorting.
 **/

public class MultiComp implements Comparator<Tuple>, Expressible, Serializable {

  private static final long serialVersionUID = 1;

  private Comparator<Tuple>[] comps;

  public MultiComp(Comparator<Tuple>... comps) {
    this.comps = comps;
  }

  public int compare(Tuple t1, Tuple t2) {
    for(Comparator<Tuple> comp : comps) {
      int i = comp.compare(t1, t2);
      if(i != 0) {
        return i;
      }
    }

    return 0;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StringBuilder sb = new StringBuilder();
    for(Comparator<Tuple> comp : comps){
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
}