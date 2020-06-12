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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.commons.math3.stat.StatUtils;

public class MeanEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  public MeanEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(1 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 1 value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value) throws IOException{
    if(null == value){
      throw new IOException(String.format(Locale.ROOT, "Unable to find %s(...) because the value is null", constructingFactory.getFunctionName(getClass())));
    }
    else if(value instanceof List){
      @SuppressWarnings({"unchecked"})
      List<Number> c = (List<Number>) value;
      double[] data = new double[c.size()];
      for(int i=0; i< c.size(); i++) {
        data[i] = c.get(i).doubleValue();
      }

      return StatUtils.mean(data);
    }
    else{
      throw new IOException(String.format(Locale.ROOT, "Unable to find %s(...) because the value is not a collection, instead a %s was found", constructingFactory.getFunctionName(getClass()), value.getClass().getSimpleName()));
    }
  }
}
