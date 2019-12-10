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
import org.apache.commons.math3.util.Precision;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PrecisionEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public PrecisionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 2 value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value, Object value2) {
    if(null == value){
      return null;
    }
    else if(value instanceof List){
      return ((List<?>)value).stream().map(innerValue -> doWork(innerValue, ((Number)value2).intValue())).collect(Collectors.toList());
    } else if(value instanceof Matrix) {
      int p = ((Number)value2).intValue();
      Matrix matrix = (Matrix)value;
      double[][] data = matrix.getData();
      for(int i=0; i<data.length; ++i) {
        for(int j=0; j < data[i].length; j++) {
          data[i][j] = Precision.round(data[i][j], p);
        }
      }

      return matrix;
    }
    else{
      return Precision.round(((Number)value).doubleValue(), ((Number)value2).intValue());
    }
  }
}
