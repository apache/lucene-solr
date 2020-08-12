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
import java.util.ArrayList;
import java.util.Locale;
import java.util.List;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PowerEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public PowerEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two values but found %d",expression, containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException {
    
    if(null == first || null == second) {
      return null;
    }

    if(first instanceof Number) {
      Number value = (Number) first;
      if(second instanceof Number) {
        Number exponent = (Number) second;
        return Math.pow(value.doubleValue(), exponent.doubleValue());
      } else if(second instanceof List)  {
        @SuppressWarnings({"unchecked"})
        List<Number> exponents = (List<Number>) second;
        List<Number> pows = new ArrayList<>();
        for(Number exponent : exponents) {
          pows.add(Math.pow(value.doubleValue(), exponent.doubleValue()));
        }
        return pows;
      } else {
        throw new IOException("The second parameter to the pow function must either be a scalar or list of scalars");
      }
    } else if(first instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> values = (List<Number>) first;
      if(second instanceof Number) {
        Number exponent = (Number) second;

        List<Number> out = new ArrayList<>(values.size());
        for (Number value : values) {
          out.add(Math.pow(value.doubleValue(), exponent.doubleValue()));
        }

        return out;
      } else if(second instanceof List) {

        List<Number> out = new ArrayList<>(values.size());
        @SuppressWarnings({"unchecked"})
        List<Number> exponents = (List<Number>)second;
        if(values.size() != exponents.size()) {
          throw new IOException("The pow function requires vectors of equal size if two vectors are provided.");
        }

        for(int i=0; i<exponents.size(); i++) {
          out.add(Math.pow(values.get(i).doubleValue(), exponents.get(i).doubleValue()));
        }

        return out;
      } else {
        throw new IOException("The second parameter to the pow function must either be a scalar or list of scalars");
      }
    } else {
      throw new IOException("The first parameter to the pow function must either be a scalar or list of scalars");
    }
  }
}
