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

import java.util.Enumeration;
import java.util.Locale;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import java.util.List;
import java.util.ArrayList;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ListCacheEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public ListCacheEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(containedEvaluators.size() > 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at most 1 values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object... values) throws IOException {
    @SuppressWarnings({"rawtypes"})
    ConcurrentMap objectCache = this.streamContext.getObjectCache();
    @SuppressWarnings({"rawtypes"})
    List list = new ArrayList();

    if(values.length == 0) {
      @SuppressWarnings({"rawtypes"})
      ConcurrentHashMap m = (ConcurrentHashMap)objectCache;
      @SuppressWarnings({"rawtypes"})
      Enumeration en = m.keys();
      while(en.hasMoreElements()) {
        list.add(en.nextElement());
      }
      return list;
    } else if(values.length == 1) {
      String space = (String)values[0];
      space = space.replace("\"", "");
      @SuppressWarnings({"rawtypes"})
      ConcurrentMap spaceCache = (ConcurrentMap)objectCache.get(space);
      if(spaceCache != null) {
        @SuppressWarnings({"rawtypes"})
        ConcurrentHashMap spaceMap = (ConcurrentHashMap)objectCache.get(space);
        @SuppressWarnings({"rawtypes"})
        Enumeration en = spaceMap.keys();
        while(en.hasMoreElements()) {
          list.add(en.nextElement());
        }
        return list;
      } else {
        return list;
      }
    } else {
      throw new IOException("The listCache function requires two parameters: workspace and key");
    }
  }
}
