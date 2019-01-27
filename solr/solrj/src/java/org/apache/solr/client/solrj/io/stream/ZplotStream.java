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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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

public class ZplotStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private StreamContext streamContext;
  private Map letParams = new LinkedHashMap();
  private Iterator<Tuple> out;

  public ZplotStream(StreamExpression expression, StreamFactory factory) throws IOException {

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    //Get all the named params

    for(StreamExpressionParameter np : namedParams) {
      String name = ((StreamExpressionNamedParameter)np).getName();
      StreamExpressionParameter param = ((StreamExpressionNamedParameter)np).getParameter();
      if(param instanceof StreamExpressionValue) {
        String paramValue = ((StreamExpressionValue) param).getValue();
        letParams.put(name, factory.constructPrimitiveObject(paramValue));
      } else if(factory.isEvaluator((StreamExpression)param)) {
        StreamEvaluator evaluator = factory.constructEvaluator((StreamExpression) param);
        letParams.put(name, evaluator);
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
    if(out.hasNext()) {
      return out.next();
    } else {
      Map m = new HashMap();
      m.put("EOF", true);
      Tuple t = new Tuple(m);
      return t;
    }
  }

  public void close() throws IOException {
  }

  public void open() throws IOException {
    Map<String, Object> lets = streamContext.getLets();
    Set<Map.Entry<String, Object>> entries = letParams.entrySet();
    Map<String, Object> evaluated = new HashMap();

    //Load up the StreamContext with the data created by the letParams.
    int numTuples = -1;
    int columns = 0;
    boolean table = false;
    for(Map.Entry<String, Object> entry : entries) {
      ++columns;

      String name = entry.getKey();
      if(name.equals("table")) {
        table = true;
      }

      Object o = entry.getValue();
      if(o instanceof StreamEvaluator) {
        Tuple eTuple = new Tuple(lets);
        StreamEvaluator evaluator = (StreamEvaluator)o;
        evaluator.setStreamContext(streamContext);
        Object eo = evaluator.evaluate(eTuple);
        if(eo instanceof List) {
          List l = (List)eo;
          if(numTuples == -1) {
            numTuples = l.size();
          } else {
            if(l.size() != numTuples) {
              throw new IOException("All lists provided to the zplot function must be the same length.");
            }
          }
          evaluated.put(name, l);
        } else if (eo instanceof Tuple) {
          evaluated.put(name, eo);
        }
      } else {
        Object eval = lets.get(o);
        if(eval instanceof List) {
          List l = (List)eval;
          if(numTuples == -1) {
            numTuples = l.size();
          } else {
            if(l.size() != numTuples) {
              throw new IOException("All lists provided to the zplot function must be the same length.");
            }
          }
          evaluated.put(name, l);
        } else if(eval instanceof Tuple) {
          evaluated.put(name, eval);
        }
      }
    }

    if(columns > 1 && table) {
      throw new IOException("If the table parameter is set there can only be one parameter.");
    }
    //Load the values into tuples

    List<Tuple> outTuples = new ArrayList();
    if(!table) {
      //Handle the vectors
      for (int i = 0; i < numTuples; i++) {
        Tuple tuple = new Tuple(new HashMap());
        for (String key : evaluated.keySet()) {
          List l = (List) evaluated.get(key);
          tuple.put(key, l.get(i));
        }

        outTuples.add(tuple);
      }
    } else {
      //Handle the Tuple and List of Tuples
      Object o = evaluated.get("table");
      if(o instanceof List) {
        List<Tuple> tuples = (List<Tuple>)o;
        outTuples.addAll(tuples);
      } else if(o instanceof Tuple) {
        outTuples.add((Tuple)o);
      }
    }

    this.out = outTuples.iterator();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}
