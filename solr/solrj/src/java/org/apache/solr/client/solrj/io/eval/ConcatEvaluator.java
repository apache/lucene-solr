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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ConcatEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  private String delim = "";

  public ConcatEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(namedParam.getName().equals("delim")){
        this.delim = namedParam.getParameter().toString().trim();
      } else {
        throw new IOException("Unexpected named parameter:"+namedParam.getName());
      }
    }

  }

  @Override
  public Object doWork(Object... values) throws IOException {

    StringBuilder buff = new StringBuilder();

    for(Object o : values) {
        if(buff.length() > 0) {
          buff.append(delim);
        }
        String s = o.toString();
        if(s.startsWith("\"") && s.endsWith("\"")) {
          s = s.substring(1, s.length()-1);
        }
        buff.append(s.toString());
    }

    return buff.toString();
  }
}
