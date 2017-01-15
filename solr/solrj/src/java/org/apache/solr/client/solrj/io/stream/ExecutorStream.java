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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  The executor function wraps a stream with Tuples containing Streaming Expressions
 *  and executes them in parallel. Sample syntax:
 *
 *  executor(thread=10, topic(storedExpressions, q="*:*", fl="expr_s, id", id="topic1"))
 *
 *  The Streaming Expression to execute is taken from the expr field in the Tuples.
 */

public class ExecutorStream extends TupleStream implements Expressible {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private TupleStream stream;

  private int threads;

  private ExecutorService executorService;
  private StreamFactory streamFactory;
  private StreamContext streamContext;

  public ExecutorStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter threadsParam = factory.getNamedOperand(expression, "threads");

    int threads = 6;

    if(threadsParam != null)  {
      threads = Integer.parseInt(((StreamExpressionValue)threadsParam.getParameter()).getValue());
    }

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    TupleStream stream = factory.constructStream(streamExpressions.get(0));
    init(stream, threads, factory);
  }

  private void init(TupleStream tupleStream, int threads, StreamFactory factory) throws IOException{
    this.threads = threads;
    this.stream = tupleStream;
    this.streamFactory = factory;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(new StreamExpressionNamedParameter("threads", Integer.toString(threads)));

    // stream
    if(includeStreams) {
      if (stream instanceof Expressible) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      } else {
        throw new IOException("The ExecuteStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
            stream.toExplanation(factory)
        })
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory, false).toString());
  }

  public void setStreamContext(StreamContext streamContext) {
    this.streamContext = streamContext;
    this.stream.setStreamContext(streamContext);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    executorService = ExecutorUtil.newMDCAwareFixedThreadPool(threads, new SolrjNamedThreadFactory("ExecutorStream"));
    stream.open();
  }

  public void close() throws IOException {
    stream.close();
    executorService.shutdown();
    try {
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch(InterruptedException e) {
      logger.error("Interrupted while waiting for termination", e);
    }
  }

  public Tuple read() throws IOException {
    ArrayBlockingQueue<Tuple> queue = new ArrayBlockingQueue(10000);
    while(true) {
      Tuple tuple = stream.read();
      if (!tuple.EOF) {
        try {
          queue.put(tuple);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        executorService.execute(new StreamTask(queue, streamFactory, streamContext));
      } else {
        return tuple;
      }
    }
  }

  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }

  public static class StreamTask implements Runnable {

    private ArrayBlockingQueue<Tuple> queue;
    private StreamFactory streamFactory;
    private StreamContext streamContext;

    public StreamTask(ArrayBlockingQueue queue, StreamFactory streamFactory, StreamContext streamContext) {
      this.queue = queue;
      this.streamFactory = streamFactory;
      this.streamContext = new StreamContext();
      this.streamContext.setSolrClientCache(streamContext.getSolrClientCache());
      this.streamContext.setModelCache(streamContext.getModelCache());
    }

    public void run() {
      Tuple tuple = null;
      try {
        tuple = queue.take();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      String expr = tuple.getString("expr_s");
      Object id = tuple.get("id");
      TupleStream stream = null;

      try {
        stream = streamFactory.constructStream(expr);
        stream.setStreamContext(streamContext);
        stream.open();
        while (true) {
          Tuple t = stream.read();
          if (t.EOF) {
            break;
          }
        }
      } catch (Exception e) {
        logger.error("Executor Error: id="+id+" expr_s="+expr, e);
      } finally {
        try {
          stream.close();
        } catch (Exception e1) {
          logger.error("Executor Error", e1);
        }
      }
    }
  }
}