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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.Timer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.IteratorWriter;
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
  final List<LeafReaderContext> leaves;
  final ExportWriter exportWriter;
  final OutputStream os;
  final Timer writeOutputBufferTimer;
  final Timer fillerWaitTimer;
  final Timer writerWaitTimer;
  final IteratorWriter.ItemWriter rawWriter;
  final IteratorWriter.ItemWriter writer;
  final CyclicBarrier barrier;
  final int totalHits;
  Buffer fillBuffer;
  Buffer outputBuffer;
  Runnable filler;
  ExecutorService service;
  Throwable error;
  LongAdder outputCounter = new LongAdder();
  volatile boolean shutDown = false;

  ExportBuffers(ExportWriter exportWriter, List<LeafReaderContext> leaves, SolrIndexSearcher searcher,
                OutputStream os, IteratorWriter.ItemWriter rawWriter, Sort sort, int queueSize, int totalHits,
                Timer writeOutputBufferTimer, Timer fillerWaitTimer, Timer writerWaitTimer) throws IOException {
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
    this.writeOutputBufferTimer = writeOutputBufferTimer;
    this.fillerWaitTimer = fillerWaitTimer;
    this.writerWaitTimer = writerWaitTimer;
    this.bufferOne = new Buffer(queueSize);
    this.bufferTwo = new Buffer(queueSize);
    this.totalHits = totalHits;
    fillBuffer = bufferOne;
    outputBuffer = bufferTwo;
    SortDoc writerSortDoc = exportWriter.getSortDoc(searcher, sort.getSort());
    bufferOne.initialize(writerSortDoc);
    bufferTwo.initialize(writerSortDoc);
    barrier = new CyclicBarrier(2, () -> swapBuffers());
    filler = () -> {
      try {
        log.debug("--- filler start {}", Thread.currentThread());
        SortDoc sortDoc = exportWriter.getSortDoc(searcher, sort.getSort());
        Buffer buffer = getFillBuffer();
        SortQueue queue = new SortQueue(queueSize, sortDoc);
        long lastOutputCounter = 0;
        for (int count = 0; count < totalHits; ) {
          log.debug("--- filler fillOutDocs in {}", fillBuffer);
          exportWriter.fillOutDocs(leaves, sortDoc, queue, buffer);
          count += (buffer.outDocsIndex + 1);
          log.debug("--- filler count={}, exchange buffer from {}", count, buffer);
          Timer.Context timerContext = getFillerWaitTimer().time();
          try {
            exchangeBuffers();
          } finally {
            timerContext.stop();
          }
          buffer = getFillBuffer();
          if (outputCounter.longValue() > lastOutputCounter) {
            lastOutputCounter = outputCounter.longValue();
            flushOutput();
          }
          log.debug("--- filler got empty buffer {}", buffer);
        }
        buffer.outDocsIndex = Buffer.NO_MORE_DOCS;
        log.debug("--- filler final exchange buffer from {}", buffer);
        Timer.Context timerContext = getFillerWaitTimer().time();
        try {
          exchangeBuffers();
        } finally {
          timerContext.stop();
        }
        buffer = getFillBuffer();
        log.debug("--- filler final got buffer {}", buffer);
      } catch (Throwable e) {
        log.error("filler", e);
        error(e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        shutdownNow();
      }
    };
  }

  public void exchangeBuffers() throws Exception {
    log.debug("---- wait exchangeBuffers from {}", Thread.currentThread());
    barrier.await(EXCHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  public void error(Throwable t) {
    error = t;
    // break the lock on the other thread too
    barrier.reset();
  }

  public Throwable getError() {
    return error;
  }

  private void swapBuffers() {
    log.debug("--- swap buffers");
    Buffer one = fillBuffer;
    fillBuffer = outputBuffer;
    outputBuffer = one;
  }

  private void flushOutput() throws IOException {
    //os.flush();
  }

  // initial output buffer
  public Buffer getOutputBuffer() {
    return outputBuffer;
  }

  public Buffer getFillBuffer() {
    return fillBuffer;
  }

  public Timer getWriteOutputBufferTimer() {
    return writeOutputBufferTimer;
  }

  public Timer getFillerWaitTimer() {
    return fillerWaitTimer;
  }

  public Timer getWriterWaitTimer() {
    return writerWaitTimer;
  }

  // decorated writer that keeps track of number of writes
  public IteratorWriter.ItemWriter getWriter() {
    return writer;
  }

  public void shutdownNow() {
    if (service != null) {
      log.debug("--- shutting down buffers");
      service.shutdownNow();
      service = null;
    }
    shutDown = true;
  }

  public boolean isShutDown() {
    return shutDown;
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
      log.debug("-- finished.");
    } catch (Exception e) {
      log.error("Exception running filler / writer", e);
      error(e);
      //
    } finally {
      log.debug("--- all done, shutting down buffers");
      shutdownNow();
    }
  }

  /**
   * Buffer used for transporting documents from the filler to the writer thread.
   */
  static final class Buffer {
    static final int EMPTY = -1;
    static final int NO_MORE_DOCS = -2;

    int outDocsIndex = EMPTY;
    SortDoc[] outDocs;

    public Buffer(int size) {
      outDocs = new SortDoc[size];
    }

    public void initialize(SortDoc proto) {
      outDocsIndex = EMPTY;
      for (int i = 0; i < outDocs.length; i++) {
        outDocs[i] = proto.copy();
      }
    }

    @Override
    public String toString() {
      return "Buffer@" + Integer.toHexString(hashCode()) + "{" +
          "outDocsIndex=" + outDocsIndex +
          '}';
    }
  }
}
