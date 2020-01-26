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

import java.util.Locale;
import java.util.Map;
import java.util.HashMap;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.Tuple;

public class SetValueEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public SetValueEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(3 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 3 values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if(values[0] instanceof Tuple) {
      Tuple tuple = (Tuple)values[0];
      String key = (String)values[1];
      Object value = values[2];
      if(value instanceof String) {
        value = ((String)value).replace("\"", "");
      }
      key = key.replace("\"", "");
      Map map = new HashMap(tuple.fields);
      map.put(key, value);
      return new Tuple(map);
    } else {
      throw new IOException("The setValue function expects a Tuple as the first parameter");
    }
  }
}
