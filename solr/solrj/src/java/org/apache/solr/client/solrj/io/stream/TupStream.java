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
import java.util.Map.Entry;
import java.util.Iterator;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.SingleValueComparator;
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

/**
 * @since 6.6.0
 */
public class TupStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private StreamContext streamContext;

  private Map<String,String> stringParams = new HashMap<>();
  private Map<String,StreamEvaluator> evaluatorParams = new HashMap<>();
  private Map<String,TupleStream> streamParams = new HashMap<>();
  private List<String> fieldNames = new ArrayList<>();
  private Map<String, String> fieldLabels = new HashMap<>();
  private Tuple tup = null;
  private Tuple unnestedTuple = null;
  private Iterator<Tuple>  unnestedTuples = null;
  
  private boolean finished;


  public TupStream(StreamExpression expression, StreamFactory factory) throws IOException {

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    //Get all the named params
    for(StreamExpressionNamedParameter np : namedParams) {
      String name = np.getName();
      fieldNames.add(name);
      fieldLabels.put(name, name);
      StreamExpressionParameter param = np.getParameter();

      // we're going to split these up here so we only make the choice once
      // order of these in read() doesn't matter
      if(param instanceof StreamExpressionValue) {
        stringParams.put(name, ((StreamExpressionValue)param).getValue());
      } else if (factory.isEvaluator((StreamExpression) param)) {
        StreamEvaluator evaluator = factory.constructEvaluator((StreamExpression) param);
        evaluatorParams.put(name, evaluator);
      } else if(factory.isStream((StreamExpression)param)) {
        TupleStream tupleStream = factory.constructStream((StreamExpression) param);
        streamParams.put(name, tupleStream);
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - only string, evaluator, or stream named parameters are supported, but param %d is none of those",expression, name));
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
    
    // add string based params
    for(Entry<String,String> param : stringParams.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue()));
    }
    
    // add evaluator based params
    for(Entry<String,StreamEvaluator> param : evaluatorParams.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue().toExpression(factory)));
    }

    // add stream based params
    for(Entry<String,TupleStream> param : streamParams.entrySet()){
      if(includeStreams){
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), ((Expressible)param.getValue()).toExpression(factory)));
      }
      else{
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), "<stream>"));
      }
    }

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
    
    // also set in evalators and streams
    for(StreamEvaluator evaluator : evaluatorParams.values()){
      evaluator.setStreamContext(context);
    }
    
    for(TupleStream stream : streamParams.values()){
      stream.setStreamContext(context);
    }
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    return l;
  }

  public Tuple read() throws IOException {

    if(unnestedTuples == null) {
      if (finished) {
        return Tuple.EOF();
      } else {
        finished = true;
        if(unnestedTuple != null) {
          return unnestedTuple;
        } else {
          return tup;
        }
      }
    } else {
      if(unnestedTuples.hasNext()) {
        return unnestedTuples.next();
      } else {
        return Tuple.EOF();
      }
    }
  }

  public void close() throws IOException {
    // Nothing to do here
  }

  @SuppressWarnings({"unchecked"})
  public void open() throws IOException {
    Map<String, Object> values = new HashMap<>();

    // add all string based params
    // these could come from the context, or they will just be treated as straight strings
    for(Entry<String,String> param : stringParams.entrySet()){
      if(streamContext.getLets().containsKey(param.getValue())){
        values.put(param.getKey(), streamContext.getLets().get(param.getValue()));
      }
      else{
        values.put(param.getKey(), param.getValue());
      }
    }

    // add all evaluators
    for(Entry<String,StreamEvaluator> param : evaluatorParams.entrySet()){
      values.put(param.getKey(), param.getValue().evaluateOverContext());
    }

    // Add all streams
    for(Entry<String,TupleStream> param : streamParams.entrySet()){

      try{
        List<Tuple> streamTuples = new ArrayList<>();
        // open the stream, closed in finally block
        param.getValue().open();

        // read all values from stream (memory expensive)
        Tuple streamTuple = param.getValue().read();
        while(!streamTuple.EOF){
          streamTuples.add(streamTuple);
          streamTuple = param.getValue().read();
        }

        values.put(param.getKey(), streamTuples);
      }
      finally{
        // safely close the stream
        param.getValue().close();
      }
    }

    if(values.size() == 1) {
      for(Object o :values.values()) {
        if(o instanceof Tuple) {
          unnestedTuple = (Tuple)o;
        } else if(o instanceof List) {
          @SuppressWarnings({"rawtypes"})
          List l = (List)o;
          if(l.size() > 0 && l.get(0) instanceof Tuple) {
            List<Tuple> tl = (List<Tuple>)l;
            unnestedTuples = tl.iterator();
          }
        }
      }
    }
    this.tup = new Tuple(values);
    tup.setFieldNames(fieldNames);
    tup.setFieldLabels(fieldLabels);
    // nothing to do here
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return new SingleValueComparator();
  }

  public int getCost() {
    return 0;
  }


}
