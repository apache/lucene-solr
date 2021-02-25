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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;


import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.MemsetEvaluator;
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

/**
 * @since 6.6.0
 */
public class LetStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private TupleStream stream;
  private StreamContext streamContext;
  @SuppressWarnings({"rawtypes"})
  private Map letParams = new LinkedHashMap();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public LetStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    //Get all the named params
    Set<String> echo = null;
    boolean echoAll = false;
    String currentName = null;
    for(StreamExpressionParameter np : namedParams) {
      String name = ((StreamExpressionNamedParameter)np).getName();
      currentName = name;

      if(name.equals("echo")) {
        echo = new HashSet();
        String echoString = ((StreamExpressionNamedParameter) np).getParameter().toString().trim();
        if(echoString.equalsIgnoreCase("true")) {
          echoAll = true;
        } else {
          String[] echoVars = echoString.split(",");
          for (String echoVar : echoVars) {
            echo.add(echoVar.trim());
          }
        }

        continue;
      }

      StreamExpressionParameter param = ((StreamExpressionNamedParameter)np).getParameter();

      if(param instanceof StreamExpressionValue) {
        String paramValue = ((StreamExpressionValue) param).getValue();
        letParams.put(name, factory.constructPrimitiveObject(paramValue));
      } else if(factory.isEvaluator((StreamExpression)param)) {
        StreamEvaluator evaluator = factory.constructEvaluator((StreamExpression) param);
        letParams.put(name, evaluator);
      } else {
        TupleStream tupleStream = factory.constructStream((StreamExpression) param);
        letParams.put(name, tupleStream);
      }
    }

    if(streamExpressions.size() > 0) {
      stream = factory.constructStream(streamExpressions.get(0));
    } else {
      StreamExpression tupleExpression = new StreamExpression("tuple");
      if(!echoAll && echo == null) {
        tupleExpression.addParameter(new StreamExpressionNamedParameter(currentName, currentName));
      } else {
        Set<String> names = letParams.keySet();
        for(String name : names) {
          if(echoAll) {
            tupleExpression.addParameter(new StreamExpressionNamedParameter(name, name));
          } else {
            if(echo.contains(name)) {
              tupleExpression.addParameter(new StreamExpressionNamedParameter(name, name));
            }
          }
        }
      }

      stream = factory.constructStream(tupleExpression);
    }
  }


  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(((Expressible) stream).toExpression(factory));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());
    explanation.addChild(stream.toExplanation(factory));

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(stream);

    return l;
  }

  public Tuple read() throws IOException {
    return stream.read();
  }

  public void close() throws IOException {
    stream.close();
  }

  @SuppressWarnings({"unchecked"})
  public void open() throws IOException {
    Map<String, Object> lets = streamContext.getLets();
    Set<Map.Entry<String, Object>> entries = letParams.entrySet();

    //Load up the StreamContext with the data created by the letParams.
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
          lets.put(name, tuples);
        } finally {
          tStream.close();
        }
      } else if(o instanceof StreamEvaluator) {
        //Add the data from the StreamContext to a tuple.
        //Let the evaluator works from this tuple.
        //This will allow columns to be created from tuples already in the StreamContext.
        Tuple eTuple = new Tuple(lets);
        StreamEvaluator evaluator = (StreamEvaluator)o;
        evaluator.setStreamContext(streamContext);
        Object eo = evaluator.evaluate(eTuple);
        if(evaluator instanceof MemsetEvaluator) {
          @SuppressWarnings({"rawtypes"})
          Map mem = (Map)eo;
          lets.putAll(mem);
        } else {
          lets.put(name, eo);
        }
      } else {
        lets.put(name, o);
      }
    }
    stream.open();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}
