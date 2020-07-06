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
package org.apache.solr.handler.export;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream implementation that helps supporting 'expr' streaming in export writer.
 * <p>Note: this class is made public only to allow access from {@link org.apache.solr.handler.ExportHandler},
 * it should be treated as an internal detail of implementation.</p>
 * @lucene.experimental
 */
public class ExportWriterStream extends TupleStream implements Expressible {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final TupleEntryWriter tupleEntryWriter = new TupleEntryWriter();
  private StreamContext context;
  private StreamComparator streamComparator;
  private int pos = -1;
  private ExportBuffers exportBuffers;
  private ExportBuffers.Buffer buffer;
  private Timer.Context writeOutputTimerContext;

  private static final class TupleEntryWriter implements EntryWriter {
    Tuple tuple;

    @Override
    public EntryWriter put(CharSequence k, Object v) throws IOException {
      if (v instanceof IteratorWriter) {
        List<Object> lst = new ArrayList<>();
        ((IteratorWriter)v).toList(lst);
        v = lst;
      } else if (v instanceof MapWriter) {
        Map<String, Object> map = new HashMap<>();
        ((MapWriter)v).toMap(map);
        v = map;
      }
      tuple.put(k.toString(), v);
      return this;
    }
  }

  public ExportWriterStream(StreamExpression expression, StreamFactory factory) throws IOException {
    streamComparator = parseComp(factory.getDefaultSort());
  }

  /**
   * NOTE: this context must contain an instance of {@link ExportBuffers} under the
   * {@link ExportBuffers#EXPORT_BUFFERS_KEY} key.
   */
  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
  }

  @Override
  public List<TupleStream> children() {
    return null;
  }

  private StreamComparator parseComp(String sort) throws IOException {

    String[] sorts = sort.split(",");
    StreamComparator[] comps = new StreamComparator[sorts.length];
    for (int i = 0; i < sorts.length; i++) {
      String s = sorts[i];

      String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.

      if (spec.length != 2) {
        throw new IOException("Invalid sort spec:" + s);
      }

      String fieldName = spec[0].trim();
      String order = spec[1].trim();

      comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
    }

    if (comps.length > 1) {
      return new MultipleFieldComparator(comps);
    } else {
      return comps[0];
    }
  }

  @Override
  public void open() throws IOException {
    exportBuffers = (ExportBuffers) context.get(ExportBuffers.EXPORT_BUFFERS_KEY);
    buffer = exportBuffers.getOutputBuffer();
  }

  @Override
  public void close() throws IOException {
    if (writeOutputTimerContext != null) {
      writeOutputTimerContext.stop();
    }
    exportBuffers = null;
  }

  @Override
  public Tuple read() throws IOException {
    Tuple res = null;
    if (pos < 0) {
      if (writeOutputTimerContext != null) {
        writeOutputTimerContext.stop();
        writeOutputTimerContext = null;
      }
      try {
        buffer.outDocsIndex = ExportBuffers.Buffer.EMPTY;
        log.debug("--- ews exchange empty buffer {}", buffer);
        boolean exchanged = false;
        while (!exchanged) {
          Timer.Context timerContext = exportBuffers.getWriterWaitTimer().time();
          try {
            exportBuffers.exchangeBuffers();
            exchanged = true;
          } catch (TimeoutException e) {
            log.debug("--- ews timeout loop");
            if (exportBuffers.isShutDown()) {
              log.debug("--- ews - the other end is shutdown, returning EOF");
              res = Tuple.EOF();
              break;
            }
            continue;
          } catch (InterruptedException e) {
            log.debug("--- ews interrupted");
            exportBuffers.error(e);
            res = Tuple.EXCEPTION(e, true);
            break;
          } catch (BrokenBarrierException e) {
            if (exportBuffers.getError() != null) {
              res = Tuple.EXCEPTION(exportBuffers.getError(), true);
            } else {
              res = Tuple.EXCEPTION(e, true);
            }
            break;
          } finally {
            timerContext.stop();
          }
        }
      } catch (InterruptedException e) {
        log.debug("--- ews interrupt");
        exportBuffers.error(e);
        res = Tuple.EXCEPTION(e, true);
      } catch (Exception e) {
        log.debug("--- ews exception", e);
        exportBuffers.error(e);
        res = Tuple.EXCEPTION(e, true);
      }
      buffer = exportBuffers.getOutputBuffer();
      if (buffer == null) {
        res = Tuple.EOF();
      }
      if (buffer.outDocsIndex == ExportBuffers.Buffer.NO_MORE_DOCS) {
        log.debug("--- ews EOF");
        res = Tuple.EOF();
      } else {
        pos = buffer.outDocsIndex;
        log.debug("--- ews new pos=" + pos);
      }
    }
    if (pos < 0) {
      log.debug("--- ews EOF?");
      res = Tuple.EOF();
    }
    if (res != null) {
      // only errors or EOF assigned result so far
      if (writeOutputTimerContext != null) {
        writeOutputTimerContext.stop();
      }
      return res;
    }
    if (writeOutputTimerContext == null) {
      writeOutputTimerContext = exportBuffers.getWriteOutputBufferTimer().time();
    }
    SortDoc sortDoc = buffer.outDocs[pos];
    tupleEntryWriter.tuple = new Tuple();
    exportBuffers.exportWriter.writeDoc(sortDoc, exportBuffers.leaves, tupleEntryWriter, exportBuffers.exportWriter.fieldWriters);
    pos--;
    return tupleEntryWriter.tuple;
  }

  @Override
  public StreamComparator getStreamSort() {
    return streamComparator;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("input")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
        .withExpression("--non-expressible--");
  }
}
