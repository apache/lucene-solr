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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MonteCarloEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;

  @SuppressWarnings({"rawtypes"})
  private Map variables = new LinkedHashMap();

  @SuppressWarnings({"unchecked"})
  public MonteCarloEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    //Get all the named params
    Set<String> echo = null;
    boolean echoAll = false;
    for(StreamExpressionParameter np : namedParams) {
      String name = ((StreamExpressionNamedParameter)np).getName();

      StreamExpressionParameter param = ((StreamExpressionNamedParameter)np).getParameter();
      if(factory.isEvaluator((StreamExpression)param)) {
        StreamEvaluator evaluator = factory.constructEvaluator((StreamExpression) param);
        variables.put(name, evaluator);
      } else {
        TupleStream tupleStream = factory.constructStream((StreamExpression) param);
        variables.put(name, tupleStream);
      }
    }

    init();
  }

  public MonteCarloEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException{
    super(expression, factory, ignoredNamedParameters);

    init();
  }

  private void init() throws IOException{
    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 2 parameters but found %d", toExpression(constructingFactory), containedEvaluators.size()));
    }
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {
    try {

      StreamEvaluator function = containedEvaluators.get(0);
      StreamEvaluator iterationsEvaluator = containedEvaluators.get(1);
      Number itNum = (Number)iterationsEvaluator.evaluate(tuple);
      int it = itNum.intValue();
      List<Number> results = new ArrayList<>();
      for(int i=0; i<it; i++) {
        populateVariables(tuple);
        Number result = (Number)function.evaluate(tuple);
        results.add(result);
      }

      return results;
    }
    catch(UncheckedIOException e){
      throw e.getCause();
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    // Nothing to do here
    throw new IOException("This call should never occur");
  }


  private void populateVariables(Tuple contextTuple) throws IOException {

    @SuppressWarnings({"unchecked"})
    Set<Map.Entry<String, Object>> entries = variables.entrySet();

    for(Map.Entry<String, Object> entry : entries) {
      String name = entry.getKey();
      Object o = entry.getValue();
      if(o instanceof TupleStream) {
        List<Tuple> tuples = new ArrayList<>();
        TupleStream tStream = (TupleStream)o;
        tStream.setStreamContext(streamContext);
        try {
          tStream.open();
          TUPLES:
          while(true) {
            Tuple tuple = tStream.read();
            if (tuple.EOF) {
              break TUPLES;
            } else {
              tuples.add(tuple);
            }
          }
          contextTuple.put(name, tuples);
        } finally {
          tStream.close();
        }
      } else {
        StreamEvaluator evaluator = (StreamEvaluator)o;
        Object eo = evaluator.evaluate(contextTuple);
        contextTuple.put(name, eo);
      }
    }
  }
}
