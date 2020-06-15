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
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class implementing a "double buffering" producer / consumer.
 */
class ExportBuffers {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final long EXCHANGE_TIMEOUT_SECONDS = 600;
  static final String EXPORT_BUFFERS_KEY = "__eb__";

  final Buffer bufferOne;
  final Buffer bufferTwo;
  final Exchanger<Buffer> exchanger = new Exchanger<>();
  final SortDoc[] outDocs;
  final List<LeafReaderContext> leaves;
  final ExportWriter exportWriter;
  final OutputStream os;
  final IteratorWriter.ItemWriter rawWriter;
  final IteratorWriter.ItemWriter writer;
  final int totalHits;
  Buffer fillBuffer;
  Buffer outputBuffer;
  Runnable filler;
  ExecutorService service;
  LongAdder outputCounter = new LongAdder();

  ExportBuffers(ExportWriter exportWriter, List<LeafReaderContext> leaves, SolrIndexSearcher searcher,
                OutputStream os, IteratorWriter.ItemWriter rawWriter, Sort sort, int queueSize, int totalHits) {
    this.exportWriter = exportWriter;
    this.leaves = leaves;
    this.os = os;
    this.rawWriter = rawWriter;
    this.writer = new IteratorWriter.ItemWriter() {
      @Override
      public IteratorWriter.ItemWriter add(Object o) throws IOException {
        rawWriter.add(o);
        outputCounter.increment();
        return this;
      }
    };
    this.outDocs = new SortDoc[queueSize];
    this.bufferOne = new Buffer();
    this.bufferTwo = new Buffer();
    this.totalHits = totalHits;
    fillBuffer = bufferOne;
    outputBuffer = bufferTwo;
    filler = () -> {
      try {
        SortDoc sortDoc = exportWriter.getSortDoc(searcher, sort.getSort());
        SortQueue queue = new SortQueue(queueSize, sortDoc);
        long lastOutputCounter = 0;
        for (int count = 0; count < totalHits; ) {
          log.info("--- filler fillOutDocs in " + fillBuffer);
          exportWriter.fillOutDocs(leaves, sortDoc, queue, outDocs, fillBuffer);
          count += (fillBuffer.outDocsIndex + 1);
          log.info("--- filler count=" + count + ", exchange buffer from " + fillBuffer);
          fillBuffer = exchange(fillBuffer);
          if (outputCounter.longValue() > lastOutputCounter) {
            lastOutputCounter = outputCounter.longValue();
            flushOutput();
          }
          log.info("--- filler got empty buffer " + fillBuffer);
        }
        fillBuffer.outDocsIndex = Buffer.NO_MORE_DOCS;
        log.info("--- filler final exchange buffer from " + fillBuffer);
        fillBuffer = exchange(fillBuffer);
        log.info("--- filler final got buffer " + fillBuffer);
      } catch (Exception e) {
        log.error("filler", e);
        shutdownNow();
      }
    };
  }

  private void flushOutput() throws IOException {
    os.flush();
  }

  // initial output buffer
  public Buffer getOutputBuffer() {
    return outputBuffer;
  }

  // decorated writer that keeps track of number of writes
  public IteratorWriter.ItemWriter getWriter() {
    return writer;
  }

  public Buffer exchange(Buffer buffer) throws InterruptedException, TimeoutException {
    return exchanger.exchange(buffer, EXCHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  public void shutdownNow() {
    if (service != null) {
      log.info("--- shutting down buffers");
      service.shutdownNow();
    }
  }

  /**
   * Start processing and block until complete or Exception is thrown.
   *
   * @param writer writer that exchanges and processes buffers received from a producer.
   * @throws IOException on errors
   */
  public void run(Callable<Boolean> writer) throws IOException {
    service = ExecutorUtil.newMDCAwareFixedThreadPool(1, new SolrNamedThreadFactory("ExportBuffers"));
    try {
      CompletableFuture.runAsync(filler, service);
      writer.call();

      // alternatively we could run the writer in a separate thread:
//        CompletableFuture<Void> allDone = CompletableFuture.allOf(
//            CompletableFuture.runAsync(filler, service),
//            CompletableFuture.runAsync(() -> {
//              try {
//                writer.call();
//              } catch (Exception e) {
//                log.error("writer", e);
//                shutdownNow();
//              }
//            }, service)
//        );
//        allDone.join();
      log.info("-- finished.");
    } catch (Exception e) {
      log.error("Exception running filler / writer", e);
      //
    } finally {
      log.info("--- all done, shutting down buffers");
      service.shutdownNow();
    }
  }

  public static final class Buffer implements MapWriter.EntryWriter {
    static final int EMPTY = -1;
    static final int NO_MORE_DOCS = -2;

    int outDocsIndex = EMPTY;
    // use array-of-arrays instead of Map to conserve space
    Object[][] outDocs;
    int pos = EMPTY;

    MapWriter.EntryWriter getEntryWriter(int pos, int numFields) {
      if (outDocs == null) {
        outDocs = new Object[outDocsIndex + 1][];
      }
      this.pos = pos;
      Object[] fields = new Object[numFields << 1];
      outDocs[pos] = fields;
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
      if (pos < 0) {
        throw new IOException("Invalid entry position");
      }
      Object[] fields = outDocs[pos];
      boolean putOk = false;
      for (int i = 0; i < fields.length; i += 2) {
        if (fields[i] == null || fields[i].equals(k)) {
          fields[i] = k;
          // convert everything complex into POJOs at this point
          // to avoid accessing docValues or termEnums from other threads
          if (v instanceof IteratorWriter) {
            List lst = new ArrayList();
            ((IteratorWriter)v).toList(lst);
            v = lst;
          } else if (v instanceof MapWriter) {
            Map<String, Object> map = new HashMap<>();
            ((MapWriter)v).toMap(map);
            v = map;
          }
          fields[i + 1] = v;
          putOk = true;
          break;
        }
      }
      if (!putOk) {
        throw new IOException("should not happen! pos=" + pos + " ran out of space for field " + k + "=" + v
            +  " - already full: " + Arrays.toString(fields));
      }
      return this;
    }

    // helper method to make it easier to write our internal key-value array as if it were a map
    public void writeItem(int pos, IteratorWriter.ItemWriter itemWriter) throws IOException {
      final Object[] fields = outDocs[pos];
      if (fields == null) {
        return;
      }
      itemWriter.add((MapWriter) ew -> {
        for (int i = 0; i < fields.length; i += 2) {
          if (fields[i] == null) {
            continue;
          }
          ew.put((CharSequence)fields[i], fields[i + 1]);
        }
      });
    }

    public void reset() {
      outDocsIndex = EMPTY;
      pos = EMPTY;
      outDocs = null;
    }

    @Override
    public String toString() {
      return "Buffer@" + Integer.toHexString(hashCode()) + "{" +
          "outDocsIndex=" + outDocsIndex +
          '}';
    }
  }
}
