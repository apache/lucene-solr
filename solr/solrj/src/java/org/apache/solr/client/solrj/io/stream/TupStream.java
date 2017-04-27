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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class TupStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private StreamContext streamContext;
  private Map tupleParams = new HashMap();
  private boolean finished;

  public TupStream(StreamExpression expression, StreamFactory factory) throws IOException {

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    //Get all the named params
    for(StreamExpressionParameter np : namedParams) {
      String name = ((StreamExpressionNamedParameter)np).getName();
      StreamExpressionParameter param = ((StreamExpressionNamedParameter)np).getParameter();

      if(param instanceof StreamExpressionValue) {
        tupleParams.put(name, ((StreamExpressionValue)param).getValue());
      } else {
        if (factory.isEvaluator((StreamExpression) param)) {
          StreamEvaluator evaluator = factory.constructEvaluator((StreamExpression) param);
          tupleParams.put(name, evaluator);
        } else {
          TupleStream tupleStream = factory.constructStream((StreamExpression) param);
          tupleParams.put(name, tupleStream);
        }
      }
    }
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    return l;
  }

  public Tuple read() throws IOException {

    if(finished) {
      Map m = new HashMap();
      m.put("EOF", true);
      return new Tuple(m);
    } else {
      finished = true;
      Map<String, Object> map = new HashMap();
      Set<Map.Entry<String, Object>> entries = tupleParams.entrySet();

      for (Map.Entry<String, Object> entry : entries) {
        String name = entry.getKey();
        Object o = entry.getValue();
        if (o instanceof TupleStream) {
          List<Tuple> tuples = new ArrayList();
          TupleStream tStream = (TupleStream) o;
          tStream.setStreamContext(streamContext);
          try {
            tStream.open();
            TUPLES:
            while (true) {
              Tuple tuple = tStream.read();
              if (tuple.EOF) {
                break TUPLES;
              } else {
                tuples.add(tuple);
              }
            }
            map.put(name, tuples);
          } finally {
            tStream.close();
          }
        } else if ((o instanceof StreamEvaluator))  {
          Tuple eTuple = new Tuple(streamContext.getLets());
          StreamEvaluator evaluator = (StreamEvaluator) o;
          Object eo = evaluator.evaluate(eTuple);
          map.put(name, eo);
        } else {
          map.put(name, streamContext.getLets().get(o.toString()));
        }
      }
      return new Tuple(map);
    }
  }

  public void close() throws IOException {
  }

  public void open() throws IOException {

  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}