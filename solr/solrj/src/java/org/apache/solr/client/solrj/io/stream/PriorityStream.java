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
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * The priority function wraps two topics that represent high priority and low priority task queues.
 * Each time the priority function is called it will check to see if there are any high priority tasks in the queue. If there
 * are high priority tasks, then the high priority queue will be read until it returns the EOF Tuple.
 *
 * If there are no tasks in the high priority queue, then the lower priority task queue will be opened and read until the EOF Tuple is
 * returned.
 *
 * The scheduler is designed to be wrapped by the executor function and a daemon function can be used to call the executor iteratively.
 * @since 6.4.0
 **/

public class PriorityStream extends TupleStream implements Expressible {

  private PushBackStream highPriorityTasks;
  private PushBackStream tasks;
  private TupleStream currentStream;

  public PriorityStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);


    if(2 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    TupleStream stream1 = factory.constructStream(streamExpressions.get(0));
    TupleStream stream2 = factory.constructStream(streamExpressions.get(1));

    if(!(stream1 instanceof TopicStream) || !(stream2 instanceof TopicStream)) {
      throw new IOException("The scheduler expects both stream parameters to be topics.");
    }

    init(new PushBackStream(stream1), new PushBackStream(stream2));
  }

  private void init(PushBackStream stream1, PushBackStream stream2) throws IOException{
    this.highPriorityTasks = stream1;
    this.tasks = stream2;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // stream
    if(includeStreams) {
      if (highPriorityTasks instanceof Expressible) {
        expression.addParameter(((Expressible) highPriorityTasks).toExpression(factory));
      } else {
        throw new IOException("The SchedulerStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }

      if (tasks instanceof Expressible) {
        expression.addParameter(((Expressible) tasks).toExpression(factory));
      } else {
        throw new IOException("The SchedulerStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
            highPriorityTasks.toExplanation(factory), tasks.toExplanation(factory)
        })
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory, false).toString());
  }

  public void setStreamContext(StreamContext streamContext) {
    this.highPriorityTasks.setStreamContext(streamContext);
    tasks.setStreamContext(streamContext);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<>();
    l.add(highPriorityTasks);
    l.add(tasks);
    return l;
  }

  public void open() throws IOException {
    highPriorityTasks.open();
    Tuple tuple = highPriorityTasks.read();
    if(tuple.EOF) {
      highPriorityTasks.close();
      tasks.open();
      currentStream = tasks;
    } else {
      highPriorityTasks.pushBack(tuple);
      currentStream = highPriorityTasks;
    }
  }

  public void close() throws IOException {
      currentStream.close();
  }

  public Tuple read() throws IOException {
    return currentStream.read();
  }

  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }
}
