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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

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
  private Map<String, DaemonStream> daemons;
  private boolean terminate;
  private boolean closed = false;
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DaemonStream(StreamExpression expression, StreamFactory factory) throws IOException{

    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    TupleStream tupleStream = factory.constructStream(streamExpressions.get(0));

    StreamExpressionNamedParameter idExpression = factory.getNamedOperand(expression, "id");
    StreamExpressionNamedParameter runExpression = factory.getNamedOperand(expression, "runInterval");
    StreamExpressionNamedParameter queueExpression = factory.getNamedOperand(expression, "queueSize");
    StreamExpressionNamedParameter terminateExpression = factory.getNamedOperand(expression, "terminate");


    String id = null;
    long runInterval = 0L;
    int queueSize = 0;
    boolean terminate = false;

    if(idExpression == null) {
      throw new IOException("Invalid expression id parameter expected");
    } else {
      id = ((StreamExpressionValue) idExpression.getParameter()).getValue();
    }

    if(runExpression == null) {
      runInterval = 2000;
    } else {
      runInterval = Long.parseLong(((StreamExpressionValue) runExpression.getParameter()).getValue());
    }

    if(queueExpression != null) {
       queueSize= Integer.parseInt(((StreamExpressionValue) queueExpression.getParameter()).getValue());
    }

    if(terminateExpression != null) {
      terminate = Boolean.parseBoolean(((StreamExpressionValue) terminateExpression.getParameter()).getValue());
    }

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    init(tupleStream, id, runInterval, queueSize, terminate);
  }

  public DaemonStream(TupleStream tupleStream, String id, long runInterval, int queueSize, boolean terminate) {
    init(tupleStream, id, runInterval, queueSize, terminate);
  }

  public DaemonStream(TupleStream tupleStream, String id, long runInterval, int queueSize) {
    this(tupleStream, id, runInterval, queueSize, false);
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    if(includeStreams){
      // streams
      if(tupleStream instanceof Expressible){
        expression.addParameter(((Expressible)tupleStream).toExpression(factory));
      } else {
        throw new IOException("This UniqueStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }

    expression.addParameter(new StreamExpressionNamedParameter("id", id));
    expression.addParameter(new StreamExpressionNamedParameter("runInterval", Long.toString(runInterval)));
    expression.addParameter(new StreamExpressionNamedParameter("queueSize", Integer.toString(queueSize)));
    expression.addParameter(new StreamExpressionNamedParameter("terminate", Boolean.toString(terminate)));

    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[] {
        tupleStream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());
  }

  public int remainingCapacity() {
    return this.queue.remainingCapacity();
  }

  public void init(TupleStream tupleStream, String id, long runInterval, int queueSize) {
    init(tupleStream, id, runInterval, queueSize, false);
  }

  public void init(TupleStream tupleStream, String id, long runInterval, int queueSize, boolean terminate) {
    this.tupleStream = tupleStream;
    this.id = id;
    this.runInterval = runInterval;
    this.queueSize = queueSize;
    this.terminate = terminate;

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
    this.streamRunner = new StreamRunner(runInterval, id);
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

  public void shutdown() {
    streamRunner.setShutdown(true);
  }

  public void close() {
    if(closed) {
      return;
    }
    streamRunner.setShutdown(true);
    this.closed = true;
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

  public void setDaemons(Map<String, DaemonStream> daemons) {
    this.daemons = daemons;
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
    private String id;

    private boolean shutdown;

    public StreamRunner(long runInterval, String id) {
      this.runInterval = runInterval;
      this.id = id;
    }

    public synchronized void setShutdown(boolean shutdown) {
      this.shutdown = shutdown;
    }

    public synchronized boolean getShutdown() {
      return shutdown;
    }

    public void run() {
      int errors = 0;
      setStartTime(new Date().getTime());
      OUTER:
      while (!getShutdown()) {
        long now = new Date().getTime();
        if ((now - lastRun) > this.runInterval) {
          lastRun = now;
          try {
            tupleStream.open();
            INNER:
            while (true) {
              Tuple tuple = tupleStream.read();
              if (tuple.EOF) {
                errors = 0; // Reset errors on successful run.
                if (tuple.fields.containsKey("sleepMillis")) {
                  this.sleepMillis = tuple.getLong("sleepMillis");

                  if(terminate && sleepMillis > 0) {
                    //TopicStream provides sleepMillis > 0 if the last run had no Tuples.
                    //This means the topic queue is empty. Time to terminate.
                    //Remove ourselves from the daemons map.
                    if(daemons != null) {
                      daemons.remove(id);
                    }
                    //Break out of the thread loop and end the run.
                    break OUTER;
                  }

                  this.runInterval = -1;
                }
                break INNER;
              } else if (!eatTuples) {
                try {
                  queue.put(tuple);
                } catch (InterruptedException e) {
                  break OUTER;
                }
              }
            }
          } catch (IOException e) {
            exception = e;
            logger.error("Error in DaemonStream:" + id, e);
            ++errors;
            if (errors > 100) {
              logger.error("Too many consectutive errors. Stopping DaemonStream:" + id);
              break OUTER;
            }
          } catch (Throwable t) {
            logger.error("Fatal Error in DaemonStream:" + id, t);
            //For anything other then IOException break out of the loop and shutdown the thread.
            break OUTER;
          } finally {
            try {
              tupleStream.close();
            } catch (IOException e1) {
              if (exception == null) {
                exception = e1;
                logger.error("Error in DaemonStream:" + id, e1);
                break OUTER;
              }
            }
          }
        }
        incrementIterations();

        if (sleepMillis > 0) {
          try {
            Thread.sleep(sleepMillis);
          } catch (InterruptedException e) {
            logger.error("Error in DaemonStream:" + id, e);
            break OUTER;
          }
        }
      }

      if(!eatTuples) {
        Map m = new HashMap();
        m.put("EOF", true);
        Tuple tuple = new Tuple(m);
        try {
          queue.put(tuple);
        } catch (InterruptedException e) {
          logger.error("Error in DaemonStream:"+id, e);
        }
      }
      setStopTime(new Date().getTime());
    }
  }
}