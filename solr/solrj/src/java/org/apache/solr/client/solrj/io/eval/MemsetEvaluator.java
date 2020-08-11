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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


/**
 * The MemsetEvaluator reads a TupleStream and copies the values from specific
 * fields into arrays that are bound to variable names in a map. The LetStream looks specifically
 * for the MemsetEvaluator and makes the variables visible to other functions.
 **/

public class MemsetEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;

  private TupleStream in;
  private String[] cols;
  private String[] vars;
  private int size = -1;

  public MemsetEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    /*
    * Instantiate and validate all the parameters
    */

    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter colsExpression = factory.getNamedOperand(expression, "cols");
    StreamExpressionNamedParameter varsExpression = factory.getNamedOperand(expression, "vars");
    StreamExpressionNamedParameter sizeExpression = factory.getNamedOperand(expression, "size");

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    if(null == colsExpression || !(colsExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'cols' parameter listing fields to sort over but didn't find one",expression));
    }

    if(null == varsExpression || !(varsExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'vars' parameter listing fields to sort over but didn't find one",expression));
    }

    if(null != sizeExpression) {
      StreamExpressionValue sizeExpressionValue = (StreamExpressionValue)sizeExpression.getParameter();
      String sizeString = sizeExpressionValue.getValue();
      size = Integer.parseInt(sizeString);
    }

    in = factory.constructStream(streamExpressions.get(0));

    StreamExpressionValue colsExpressionValue = (StreamExpressionValue)colsExpression.getParameter();
    StreamExpressionValue varsExpressionValue = (StreamExpressionValue)varsExpression.getParameter();
    String colsString = colsExpressionValue.getValue();
    String varsString = varsExpressionValue.getValue();

    vars = varsString.split(",");
    cols = colsString.split(",");

    if(cols.length != vars.length) {
      throw new IOException("The cols and vars lists must be the same size");
    }

    for(int i=0; i<cols.length; i++) {
      cols[i]  = cols[i].trim();
      vars[i]  = vars[i].trim();
    }
  }

  public MemsetEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException {
    super(expression, factory, ignoredNamedParameters);
  }

  public void setStreamContext(StreamContext streamContext) {
    this.streamContext = streamContext;
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {

    /*
    * Read all the tuples from the underlying stream and
    * load specific fields into arrays. Then return
    * a map with the variables names bound to the arrays.
    */

    try {
      in.setStreamContext(streamContext);
      in.open();
      Map<String, List<Number>> arrays = new HashMap<>();

      //Initialize the variables
      for(String var : vars) {
        if(size > -1) {
          arrays.put(var, new ArrayList<>(size));
        } else {
          arrays.put(var, new ArrayList<>());
        }
      }

      int count = 0;

      while (true) {
        Tuple t = in.read();
        if (t.EOF) {
          break;
        }

        if(size == -1 || count < size) {
          for (int i = 0; i < cols.length; i++) {
            String col = cols[i];
            String var = vars[i];
            List<Number> array = arrays.get(var);
            Number number = (Number) t.get(col);
            array.add(number);
          }
        }
        ++count;
      }

      return arrays;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    } finally {
      in.close();
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    // Nothing to do here
    throw new IOException("This call should never occur");
  }
}

