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
package org.apache.solr.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables adding batches of documents to an EmbeddedSolrServer.
 */
class BatchWriter {
  
  private final EmbeddedSolrServer solr;
  private volatile Exception batchWriteException = null;
  
  private static final Logger LOG = LoggerFactory.getLogger(BatchWriter.class);

  public Exception getBatchWriteException() {
    return batchWriteException;
  }

  public void setBatchWriteException(Exception batchWriteException) {
    this.batchWriteException = batchWriteException;
  }

  /** The number of writing threads. */
  final int writerThreads;

  /** Queue Size */
  final int queueSize;

  private final ThreadPoolExecutor batchPool;

  private TaskID taskId = null;

  /**
   * The number of in progress batches, must be zero before the close can
   * actually start closing
   */
  AtomicInteger executingBatches = new AtomicInteger(0);

  /**
   * Create the batch writer object, set the thread to daemon mode, and start
   * it.
   * 
   */

  final class Batch implements Runnable {
    
    private List<SolrInputDocument> documents;
    private UpdateResponse result;

    public Batch(Collection<SolrInputDocument> batch) {
      documents = new ArrayList<SolrInputDocument>(batch);
    }

    public void run() {
      try {
        executingBatches.getAndIncrement();
        result = runUpdate(documents);
      } finally {
        executingBatches.getAndDecrement();
      }
    }

    protected List<SolrInputDocument> getDocuments() {
      return documents;
    }

    protected void setDocuments(List<SolrInputDocument> documents) {
      this.documents = documents;
    }

    protected UpdateResponse getResult() {
      return result;
    }

    protected void setResult(UpdateResponse result) {
      this.result = result;
    }

    protected void reset(List<SolrInputDocument> documents) {
      if (this.documents == null) {
        this.documents = new ArrayList<SolrInputDocument>(documents);
      } else {
        this.documents.clear();
        this.documents.addAll(documents);
      }
      result = null;
    }

    protected void reset(SolrInputDocument document) {
      if (this.documents == null) {
        this.documents = new ArrayList<SolrInputDocument>();
      } else {
        this.documents.clear();
      }
      this.documents.add(document);
      result = null;
    }
  }

  protected UpdateResponse runUpdate(List<SolrInputDocument> batchToWrite) {
    try {
      UpdateResponse result = solr.add(batchToWrite);
      SolrRecordWriter.incrementCounter(taskId, SolrCounters.class.getName(), SolrCounters.BATCHES_WRITTEN.toString(), 1);      
      SolrRecordWriter.incrementCounter(taskId, SolrCounters.class.getName(), SolrCounters.DOCUMENTS_WRITTEN.toString(), batchToWrite.size());
      if (LOG.isDebugEnabled()) {
        SolrRecordWriter.incrementCounter(taskId, SolrCounters.class.getName(), SolrCounters.BATCH_WRITE_TIME.toString(), result.getElapsedTime());
      }
      return result;
    } catch (Throwable e) {
      if (e instanceof Exception) {
        setBatchWriteException((Exception) e);
      } else {
        setBatchWriteException(new Exception(e));
      }
      SolrRecordWriter.incrementCounter(taskId, getClass().getName() + ".errors", e.getClass().getName(), 1);
      LOG.error("Unable to process batch", e);
      return null;
    }
  }


  public BatchWriter(EmbeddedSolrServer solr, int batchSize, TaskID tid,
      int writerThreads, int queueSize) {
    this.solr = solr;
    this.writerThreads = writerThreads;
    this.queueSize = queueSize;
    taskId = tid;

    // we need to obtain the settings before the constructor
    if (writerThreads != 0) {
      batchPool = new ThreadPoolExecutor(writerThreads, writerThreads, 5,
          TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(queueSize),
          new ThreadPoolExecutor.CallerRunsPolicy());
    } else { // single threaded case
      batchPool = null;
    }
  }

  public void queueBatch(Collection<SolrInputDocument> batch)
      throws IOException, SolrServerException {

    throwIf();
    Batch b = new Batch(batch);
    if (batchPool != null) {
      batchPool.execute(b);
    } else { // single threaded case
      b.run();
      throwIf();
    }
  }

  public synchronized void close(TaskAttemptContext context)
      throws InterruptedException, SolrServerException, IOException {

    if (batchPool != null) {
      context.setStatus("Waiting for batches to complete");
      batchPool.shutdown();
  
      while (!batchPool.isTerminated()) {
        LOG.info(String.format(Locale.ENGLISH, 
            "Waiting for %d items and %d threads to finish executing", batchPool
                .getQueue().size(), batchPool.getActiveCount()));
        batchPool.awaitTermination(5, TimeUnit.SECONDS);
      }
    }
    context.setStatus("Committing Solr Phase 1");
    solr.commit(true, false);
    context.setStatus("Optimizing Solr");
    int maxSegments = context.getConfiguration().getInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS, 1);
    LOG.info("Optimizing Solr: forcing merge down to {} segments", maxSegments);
    long start = System.currentTimeMillis();
    solr.optimize(true, false, maxSegments);
    context.getCounter(SolrCounters.class.getName(), SolrCounters.PHYSICAL_REDUCER_MERGE_TIME.toString()).increment(System.currentTimeMillis() - start);
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    LOG.info("Optimizing Solr: done forcing merge down to {} segments in {} secs", maxSegments, secs);
    context.setStatus("Committing Solr Phase 2");
    solr.commit(true, false);
    context.setStatus("Shutting down Solr");
    solr.shutdown();
  }

  /**
   * Throw a legal exception if a previous batch write had an exception. The
   * previous state is cleared. Uses {@link #batchWriteException} for the state
   * from the last exception.
   * 
   * This will loose individual exceptions if the exceptions happen rapidly.
   * 
   * @throws IOException On low level IO error
   * @throws SolrServerException On Solr Exception
   */
  private void throwIf() throws IOException, SolrServerException {

    final Exception last = batchWriteException;
    batchWriteException = null;

    if (last == null) {
      return;
    }
    if (last instanceof SolrServerException) {
      throw (SolrServerException) last;
    }
    if (last instanceof IOException) {
      throw (IOException) last;
    }
    throw new IOException("Batch Write Failure", last);
  }
}
