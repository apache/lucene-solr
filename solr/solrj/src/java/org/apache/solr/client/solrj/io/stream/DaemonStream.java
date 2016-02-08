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
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DaemonStream extends TupleStream implements Expressible {

  private TupleStream tupleStream;
  private StreamRunner streamRunner;
  private ArrayBlockingQueue<Tuple> queue;
  private int queueSize;
  private boolean eatTuples;
  private long iterations;
  private long startTime;
  private long stopTime;
  private Exception exception;
  private long runInterval;
  private String id;
  private boolean closed = false;
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DaemonStream(StreamExpression expression, StreamFactory factory) throws IOException{

    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    TupleStream tupleStream = factory.constructStream(streamExpressions.get(0));

    StreamExpressionNamedParameter idExpression = factory.getNamedOperand(expression, "id");
    StreamExpressionNamedParameter runExpression = factory.getNamedOperand(expression, "runInterval");
    StreamExpressionNamedParameter queueExpression = factory.getNamedOperand(expression, "queueSize");

    String id = null;
    long runInterval = 0L;
    int queueSize = 0;

    if(idExpression == null) {
      throw new IOException("Invalid expression id parameter expected");
    } else {
      id = ((StreamExpressionValue) idExpression.getParameter()).getValue();
    }

    if(runExpression == null) {
      throw new IOException("Invalid expression runInterval parameter expected");
    } else {
      runInterval = Long.parseLong(((StreamExpressionValue) runExpression.getParameter()).getValue());
    }

    if(queueExpression != null) {
       queueSize= Integer.parseInt(((StreamExpressionValue)queueExpression.getParameter()).getValue());
    }

    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 2 &&
        expression.getParameters().size() != streamExpressions.size() + 3) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    init(tupleStream, id, runInterval, queueSize);
  }

  public DaemonStream(TupleStream tupleStream, String id, long runInterval, int queueSize) {
    init(tupleStream, id, runInterval, queueSize);
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // streams
    if(tupleStream instanceof Expressible){
      expression.addParameter(((Expressible)tupleStream).toExpression(factory));
    } else {
      throw new IOException("This UniqueStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }

    expression.addParameter(new StreamExpressionNamedParameter("id", id));
    expression.addParameter(new StreamExpressionNamedParameter("runInterval", Long.toString(runInterval)));
    expression.addParameter(new StreamExpressionNamedParameter("queueSize", Integer.toString(queueSize)));

    return expression;
  }

  public int remainingCapacity() {
    return this.queue.remainingCapacity();
  }

  public void init(TupleStream tupleStream, String id, long runInterval, int queueSize) {
    this.tupleStream = tupleStream;
    this.id = id;
    this.runInterval = runInterval;
    this.queueSize = queueSize;
    if(queueSize > 0) {
      queue = new ArrayBlockingQueue(queueSize);
      eatTuples = false;
    } else {
      eatTuples = true;
    }
  }

  public int hashCode() {
    return id.hashCode();
  }

  public boolean equals(Object o) {
    if(o instanceof DaemonStream) {
      return id.equals(((DaemonStream)o).id);
    }
    return false;
  }

  public String getId() {
    return id;
  }

  public void open() {
    this.streamRunner = new StreamRunner(runInterval);
    this.streamRunner.start();
  }

  public Tuple read() throws IOException {
    try {
      return queue.take();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public StreamComparator getStreamSort() {
    return tupleStream.getStreamSort();
  }

  public void setStreamContext(StreamContext streamContext) {
    this.tupleStream.setStreamContext(streamContext);
  }

  public void close() {
    if(closed) {
      return;
    }
    streamRunner.setShutdown(true);
  }

  public List<TupleStream> children() {
    List<TupleStream> children = new ArrayList();
    children.add(tupleStream);
    return children;
  }

  public synchronized Tuple getInfo() {
    Tuple tuple = new Tuple(new HashMap());
    tuple.put("id", id);
    tuple.put("startTime", startTime);
    tuple.put("stopTime", stopTime);
    tuple.put("iterations", iterations);
    tuple.put("state", streamRunner.getState().toString());
    if(exception != null) {
      tuple.put("exception", exception.getMessage());
    }

    return tuple;
  }

  private synchronized void incrementIterations() {
    ++iterations;
  }

  private synchronized void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  private synchronized void setStopTime(long stopTime) {
    this.stopTime = stopTime;
  }

  private class StreamRunner extends Thread {

    private long sleepMillis = 1000;
    private long runInterval;
    private long lastRun;

    private boolean shutdown;

    public StreamRunner(long runInterval) {
      this.runInterval = runInterval;
    }

    public synchronized void setShutdown(boolean shutdown) {
      this.shutdown = shutdown;
      interrupt(); //We could be blocked on the queue or sleeping
    }

    public synchronized boolean getShutdown() {
      return shutdown;
    }

    public void run() {
      setStartTime(new Date().getTime());
      OUTER:
      while (!getShutdown()) {
        long now = new Date().getTime();
        if((now-lastRun) > this.runInterval) {
          lastRun = now;
          try {
            tupleStream.open();
            INNER:
            while (true) {
              Tuple tuple = tupleStream.read();
              if (tuple.EOF) {
                break INNER;
              } else if (!eatTuples) {
                try {
                  queue.put(tuple);
                } catch(InterruptedException e) {
                  break OUTER;
                }
              }
            }
          } catch (IOException e) {
            exception = e;
            logger.error("Error in DaemonStream", e);
            break OUTER;
          } finally {
            try {
              tupleStream.close();
            } catch (IOException e1) {
              if (exception == null) {
                exception = e1;
                logger.error("Error in DaemonStream", e1);
                break OUTER;
              }
            }
          }
        }
        incrementIterations();
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
          logger.error("Error in DaemonStream", e);
          break OUTER;
        }
      }

      if(!eatTuples) {
        Map m = new HashMap();
        m.put("EOF", true);
        Tuple tuple = new Tuple(m);
        try {
          queue.put(tuple);
        } catch (InterruptedException e) {
          logger.error("Error in DaemonStream", e);
        }
      }
      setStopTime(new Date().getTime());
    }
  }
}