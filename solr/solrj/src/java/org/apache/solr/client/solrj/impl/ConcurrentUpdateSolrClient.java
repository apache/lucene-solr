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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * ConcurrentUpdateSolrClient buffers all added documents and writes
 * them into open HTTP connections. This class is thread safe.
 * 
 * Params from {@link UpdateRequest} are converted to http request
 * parameters. When params change between UpdateRequests a new HTTP
 * request is started.
 * 
 * Although any SolrClient request can be made with this implementation, it is
 * only recommended to use ConcurrentUpdateSolrClient with /update
 * requests. The class {@link HttpSolrClient} is better suited for the
 * query interface.
 */
public class ConcurrentUpdateSolrClient extends SolrClient {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private HttpSolrClient client;
  final BlockingQueue<Update> queue;
  final ExecutorService scheduler;
  final Queue<Runner> runners;
  volatile CountDownLatch lock = null; // used to block everything
  final int threadCount;
  boolean shutdownExecutor = false;
  int pollQueueTime = 250;
  int stallTime;
  private final boolean streamDeletes;
  private boolean internalHttpClient;
  private volatile Integer connectionTimeout;
  private volatile Integer soTimeout;
  private volatile boolean closed;
  
  AtomicInteger pollInterrupts;
  AtomicInteger pollExits;
  AtomicInteger blockLoops;
  AtomicInteger emptyQueueLoops;
  
  /**
   * Uses the supplied HttpClient to send documents to the Solr server.
   * 
   * @deprecated use {@link ConcurrentUpdateSolrClient#ConcurrentUpdateSolrClient(Builder)} instead, as it is a more extension/subclassing-friendly alternative
   */
  @Deprecated
  protected ConcurrentUpdateSolrClient(String solrServerUrl,
                                       HttpClient client, int queueSize, int threadCount,
                                       ExecutorService es, boolean streamDeletes) {
    this((streamDeletes) ?
        new Builder(solrServerUrl)
        .withHttpClient(client)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .withExecutorService(es)
        .alwaysStreamDeletes() :
          new Builder(solrServerUrl)
          .withHttpClient(client)
          .withQueueSize(queueSize)
          .withThreadCount(threadCount)
          .withExecutorService(es)
          .neverStreamDeletes());
  }
  
  protected ConcurrentUpdateSolrClient(Builder builder) {
    this.internalHttpClient = (builder.httpClient == null);
    this.client = new HttpSolrClient.Builder(builder.baseSolrUrl)
        .withHttpClient(builder.httpClient)
        .withConnectionTimeout(builder.connectionTimeoutMillis)
        .withSocketTimeout(builder.socketTimeoutMillis)
        .build();
    this.client.setFollowRedirects(false);
    this.queue = new LinkedBlockingQueue<>(builder.queueSize);
    this.threadCount = builder.threadCount;
    this.runners = new LinkedList<>();
    this.streamDeletes = builder.streamDeletes;
    this.connectionTimeout = builder.connectionTimeoutMillis;
    this.soTimeout = builder.socketTimeoutMillis;
    this.stallTime = Integer.getInteger("solr.cloud.client.stallTime", 15000);
    if (stallTime < pollQueueTime * 2) {
      throw new RuntimeException("Invalid stallTime: " + stallTime + "ms, must be 2x > pollQueueTime " + pollQueueTime);
    }

    if (builder.executorService != null) {
      this.scheduler = builder.executorService;
      this.shutdownExecutor = false;
    } else {
      this.scheduler = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("concurrentUpdateScheduler"));
      this.shutdownExecutor = true;
    }
    
    if (log.isDebugEnabled()) {
      this.pollInterrupts = new AtomicInteger();
      this.pollExits = new AtomicInteger();
      this.blockLoops = new AtomicInteger();
      this.emptyQueueLoops = new AtomicInteger();
    }
  }

  public Set<String> getQueryParams() {
    return this.client.getQueryParams();
  }

  /**
   * Expert Method.
   * @param queryParams set of param keys to only send via the query string
   */
  public void setQueryParams(Set<String> queryParams) {
    this.client.setQueryParams(queryParams);
  }
  
  /**
   * Opens a connection and sends everything...
   */
  @SuppressWarnings({"unchecked"})
  class Runner implements Runnable {
    volatile Thread thread = null;
    volatile boolean inPoll = false;
    
    public Thread getThread() {
      return thread;
    }
    
    @Override
    public void run() {
      this.thread = Thread.currentThread();
      log.debug("starting runner: {}", this);
      // This loop is so we can continue if an element was added to the queue after the last runner exited.
      for (;;) {
        try {

          sendUpdateStream();

        } catch (Throwable e) {
          if (e instanceof OutOfMemoryError) {
            throw (OutOfMemoryError) e;
          }
          handleError(e);
        } finally {
          synchronized (runners) {
            // check to see if anything else was added to the queue
            if (runners.size() == 1 && !queue.isEmpty() && !scheduler.isShutdown()) {
              // If there is something else to process, keep last runner alive by staying in the loop.
            } else {
              runners.remove(this);
              if (runners.isEmpty()) {
                // notify anyone waiting in blockUntilFinished
                runners.notifyAll();
              }
              break;
            }
          }
        }
      }

      log.debug("finished: {}", this);
    }

    public void interruptPoll() {
      Thread lthread = thread;
      if (inPoll && lthread != null) {
        lthread.interrupt();
      }
    }
    
    //
    // Pull from the queue multiple times and streams over a single connection.
    // Exits on exception, interruption, or an empty queue to pull from.
    //
    @SuppressWarnings({"unchecked"})
    void sendUpdateStream() throws Exception {
    
      while (!queue.isEmpty()) {
        HttpPost method = null;
        HttpResponse response = null;
        
        InputStream rspBody = null;
        try {
          Update update;
          notifyQueueAndRunnersIfEmptyQueue();
          try {
            inPoll = true;
            update = queue.poll(pollQueueTime, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            if (log.isDebugEnabled()) pollInterrupts.incrementAndGet();
            continue;
          } finally {
            inPoll = false;
          }
          if (update == null)
            break;

          String contentType = client.requestWriter.getUpdateContentType();
          final boolean isXml = ClientUtils.TEXT_XML.equals(contentType);

          final ModifiableSolrParams origParams = new ModifiableSolrParams(update.getRequest().getParams());
          final String origTargetCollection = update.getCollection();

          EntityTemplate template = new EntityTemplate(new ContentProducer() {
            
            @Override
            public void writeTo(OutputStream out) throws IOException {

              if (isXml) {
                out.write("<stream>".getBytes(StandardCharsets.UTF_8)); // can be anything
              }
              Update upd = update;
              while (upd != null) {
                UpdateRequest req = upd.getRequest();
                SolrParams currentParams = new ModifiableSolrParams(req.getParams());
                if (!origParams.toNamedList().equals(currentParams.toNamedList()) || !StringUtils.equals(origTargetCollection, upd.getCollection())) {
                  queue.add(upd); // Request has different params or destination core/collection, return to queue
                  break;
                }

                client.requestWriter.write(req, out);
                if (isXml) {
                  // check for commit or optimize
                  SolrParams params = req.getParams();
                  if (params != null) {
                    String fmt = null;
                    if (params.getBool(UpdateParams.OPTIMIZE, false)) {
                      fmt = "<optimize waitSearcher=\"%s\" />";
                    } else if (params.getBool(UpdateParams.COMMIT, false)) {
                      fmt = "<commit waitSearcher=\"%s\" />";
                    }
                    if (fmt != null) {
                      byte[] content = String.format(Locale.ROOT,
                          fmt, params.getBool(UpdateParams.WAIT_SEARCHER, false)
                              + "")
                          .getBytes(StandardCharsets.UTF_8);
                      out.write(content);
                    }
                  }
                }
                out.flush();

                notifyQueueAndRunnersIfEmptyQueue();
                inPoll = true;
                try {
                  while (true) {
                    try {
                      upd = queue.poll(pollQueueTime, TimeUnit.MILLISECONDS);
                      break;
                    } catch (InterruptedException e) {
                      if (log.isDebugEnabled()) pollInterrupts.incrementAndGet();
                      if (!queue.isEmpty()) {
                        continue;
                      }
                      if (log.isDebugEnabled()) pollExits.incrementAndGet();
                      upd = null;
                      break;
                    } finally {
                      inPoll = false;
                    }
                  }
                }finally {
                  inPoll = false;
                }
              }

              if (isXml) {
                out.write("</stream>".getBytes(StandardCharsets.UTF_8));
              }
            
            
            }
          });

          // The parser 'wt=' and 'version=' params are used instead of the
          // original params
          ModifiableSolrParams requestParams = new ModifiableSolrParams(origParams);
          requestParams.set(CommonParams.WT, client.parser.getWriterType());
          requestParams.set(CommonParams.VERSION, client.parser.getVersion());

          String basePath = client.getBaseURL();
          if (update.getCollection() != null)
            basePath += "/" + update.getCollection();

          method = new HttpPost(basePath + "/update"
              + requestParams.toQueryString());
          
          org.apache.http.client.config.RequestConfig.Builder requestConfigBuilder = HttpClientUtil.createDefaultRequestConfigBuilder();
          if (soTimeout != null) {
            requestConfigBuilder.setSocketTimeout(soTimeout);
          }
          if (connectionTimeout != null) {
            requestConfigBuilder.setConnectTimeout(connectionTimeout);
          }
  
          method.setConfig(requestConfigBuilder.build());
          
          method.setEntity(template);
          method.addHeader("User-Agent", HttpSolrClient.AGENT);
          method.addHeader("Content-Type", contentType);
          
       
          response = client.getHttpClient()
              .execute(method, HttpClientUtil.createNewHttpClientRequestContext());
          
          rspBody = response.getEntity().getContent();
            
          int statusCode = response.getStatusLine().getStatusCode();
          if (statusCode != HttpStatus.SC_OK) {
            StringBuilder msg = new StringBuilder();
            msg.append(response.getStatusLine().getReasonPhrase());
            msg.append("\n\n\n\n");
            msg.append("request: ").append(method.getURI());

            SolrException solrExc;
            NamedList<String> metadata = null;
            // parse out the metadata from the SolrException
            try {
              String encoding = "UTF-8"; // default
              if (response.getEntity().getContentType().getElements().length > 0) {
                NameValuePair param = response.getEntity().getContentType().getElements()[0].getParameterByName("charset");
                if (param != null)  {
                  encoding = param.getValue();
                }
              }
              NamedList<Object> resp = client.parser.processResponse(rspBody, encoding);
              NamedList<Object> error = (NamedList<Object>) resp.get("error");
              if (error != null) {
                metadata = (NamedList<String>) error.get("metadata");
                String remoteMsg = (String) error.get("msg");
                if (remoteMsg != null) {
                  msg.append("\nRemote error message: ");
                  msg.append(remoteMsg);
                }
              }
            } catch (Exception exc) {
              // don't want to fail to report error if parsing the response fails
              log.warn("Failed to parse error response from {} due to: ", client.getBaseURL(), exc);
            } finally {
              solrExc = new HttpSolrClient.RemoteSolrException(client.getBaseURL(), statusCode, msg.toString(), null);
              if (metadata != null) {
                solrExc.setMetadata(metadata);
              }
            }

            handleError(solrExc);
          } else {
            onSuccess(response);
          }
          
        } finally {
          try {
            if (response != null) {
              Utils.consumeFully(response.getEntity());
            }
          } catch (Exception e) {
            log.error("Error consuming and closing http response stream.", e);
          }
          notifyQueueAndRunnersIfEmptyQueue();
        }
      }
    }
  }
  
  private void notifyQueueAndRunnersIfEmptyQueue() {
    if (queue.size() == 0) {
      synchronized (queue) {
        // queue may be empty
        queue.notifyAll();
      }
      synchronized (runners) {
        // we notify runners too - if there is a high queue poll time and this is the update
        // that emptied the queue, we make an attempt to avoid the 250ms timeout in blockUntilFinished
        runners.notifyAll();
      }
    }
  }

  // *must* be called with runners monitor held, e.g. synchronized(runners){ addRunner() }
  private void addRunner() {
    MDC.put("ConcurrentUpdateSolrClient.url", client.getBaseURL());
    try {
      Runner r = new Runner();
      runners.add(r);
      try {
        scheduler.execute(r);  // this can throw an exception if the scheduler has been shutdown, but that should be fine.
      } catch (RuntimeException e) {
        runners.remove(r);
        throw e;
      }
    } finally {
      MDC.remove("ConcurrentUpdateSolrClient.url");
    }
  }

  /**
   * Class representing an UpdateRequest and an optional collection.
   */
  static class Update {
    UpdateRequest request;
    String collection;
    /**
     * 
     * @param request the update request.
     * @param collection The collection, can be null.
     */
    public Update(UpdateRequest request, String collection) {
      this.request = request;
      this.collection = collection;
    }
    /**
     * @return the update request.
     */
    public UpdateRequest getRequest() {
      return request;
    }
    public void setRequest(UpdateRequest request) {
      this.request = request;
    }
    /**
     * @return the collection, can be null.
     */
    public String getCollection() {
      return collection;
    }
    public void setCollection(String collection) {
      this.collection = collection;
    }
  }

  @Override
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"})final SolrRequest request, String collection)
      throws SolrServerException, IOException {
    if (!(request instanceof UpdateRequest)) {
      return client.request(request, collection);
    }
    UpdateRequest req = (UpdateRequest) request;

    // this happens for commit...
    if (streamDeletes) {
      if ((req.getDocuments() == null || req.getDocuments().isEmpty())
          && (req.getDeleteById() == null || req.getDeleteById().isEmpty())
          && (req.getDeleteByIdMap() == null || req.getDeleteByIdMap().isEmpty())) {
        if (req.getDeleteQuery() == null) {
          blockUntilFinished();
          return client.request(request, collection);
        }
      }
    } else {
      if ((req.getDocuments() == null || req.getDocuments().isEmpty())) {
        blockUntilFinished();
        return client.request(request, collection);
      }
    }


    SolrParams params = req.getParams();
    if (params != null) {
      // check if it is waiting for the searcher
      if (params.getBool(UpdateParams.WAIT_SEARCHER, false)) {
        log.info("blocking for commit/optimize");
        blockUntilFinished(); // empty the queue
        return client.request(request, collection);
      }
    }

    try {
      CountDownLatch tmpLock = lock;
      if (tmpLock != null) {
        tmpLock.await();
      }

      Update update = new Update(req, collection);
      boolean success = queue.offer(update);

      long lastStallTime = -1;
      int lastQueueSize = -1;
      for (;;) {
        synchronized (runners) {
          // see if queue is half full and we can add more runners
          // special case: if only using a threadCount of 1 and the queue
          // is filling up, allow 1 add'l runner to help process the queue
          if (runners.isEmpty() || (queue.remainingCapacity() < queue.size() && runners.size() < threadCount))
          {
            // We need more runners, so start a new one.
            addRunner();
          } else {
            // break out of the retry loop if we added the element to the queue
            // successfully, *and*
            // while we are still holding the runners lock to prevent race
            // conditions.
            if (success)
              break;
          }
        }

        // Retry to add to the queue w/o the runners lock held (else we risk
        // temporary deadlock)
        // This retry could also fail because
        // 1) existing runners were not able to take off any new elements in the
        // queue
        // 2) the queue was filled back up since our last try
        // If we succeed, the queue may have been completely emptied, and all
        // runners stopped.
        // In all cases, we should loop back to the top to see if we need to
        // start more runners.
        //
        if (!success) {
          success = queue.offer(update, 100, TimeUnit.MILLISECONDS);
        }
        if (!success) {
          // stall prevention
          int currentQueueSize = queue.size();
          if (currentQueueSize != lastQueueSize) {
            // there's still some progress in processing the queue - not stalled
            lastQueueSize = currentQueueSize;
            lastStallTime = -1;
          } else {
            if (lastStallTime == -1) {
              // mark a stall but keep trying
              lastStallTime = System.nanoTime();
            } else {
              long currentStallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastStallTime);
              if (currentStallTime > stallTime) {
                throw new IOException("Request processing has stalled for " + currentStallTime + "ms with " + queue.size() + " remaining elements in the queue.");
              }
            }
          }
        }
      }
    } catch (InterruptedException e) {
      log.error("interrupted", e);
      throw new IOException(e.getLocalizedMessage());
    }

    // RETURN A DUMMY result
    NamedList<Object> dummy = new NamedList<>();
    dummy.add("NOTE", "the request is processed in a background stream");
    return dummy;
  }

  public synchronized void blockUntilFinished() throws IOException {
    lock = new CountDownLatch(1);
    try {

      waitForEmptyQueue();
      interruptRunnerThreadsPolling();

      long lastStallTime = -1;
      int lastQueueSize = -1;

      synchronized (runners) {

        // NOTE: if the executor is shut down, runners may never become empty (a scheduled task may never be run,
        // which means it would never remove itself from the runners list. This is why we don't wait forever
        // and periodically check if the scheduler is shutting down.
        int loopCount = 0;
        while (!runners.isEmpty()) {
          
          if (log.isDebugEnabled()) blockLoops.incrementAndGet();
          
          if (scheduler.isShutdown())
            break;
          
          loopCount++;
          
          // Need to check if the queue is empty before really considering this is finished (SOLR-4260)
          int queueSize = queue.size();
          // stall prevention
          if (lastQueueSize != queueSize) {
            // init, or no stall
            lastQueueSize = queueSize;
            lastStallTime = -1;
          } else {
            if (lastStallTime == -1) {
              lastStallTime = System.nanoTime();
            } else {
              long currentStallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastStallTime);
              if (currentStallTime > stallTime) {
                throw new IOException("Task queue processing has stalled for " + currentStallTime + " ms with " + queueSize + " remaining elements to process.");
//                Thread.currentThread().interrupt();
//                break;
              }
            }
          }
          if (queueSize > 0 && runners.isEmpty()) {
            // TODO: can this still happen?
            log.warn("No more runners, but queue still has {} adding more runners to process remaining requests on queue"
                , queueSize);
            addRunner();
          }
          
          interruptRunnerThreadsPolling();
          
          // try to avoid the worst case wait timeout
          // without bad spin
          int timeout;
          if (loopCount < 3) {
            timeout = 10;
          } else if (loopCount < 10) {
            timeout = 25;
          } else {
            timeout = 250;
          }
          
          try {
            runners.wait(timeout);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    } finally {
      lock.countDown();
      lock = null;
    }
  }

  private void waitForEmptyQueue() throws IOException {
    boolean threadInterrupted = Thread.currentThread().isInterrupted();

    long lastStallTime = -1;
    int lastQueueSize = -1;
    while (!queue.isEmpty()) {
      if (log.isDebugEnabled()) emptyQueueLoops.incrementAndGet();
      if (scheduler.isTerminated()) {
        log.warn("The task queue still has elements but the update scheduler {} is terminated. Can't process any more tasks. Queue size: {}, Runners: {}. Current thread Interrupted? {}"
            , scheduler, queue.size(), runners.size(), threadInterrupted);
        break;
      }

      synchronized (runners) {
        int queueSize = queue.size();
        if (queueSize > 0 && runners.isEmpty()) {
          log.warn("No more runners, but queue still has {} adding more runners to process remaining requests on queue"
              , queueSize);
          addRunner();
        }
      }
      synchronized (queue) {
        try {
          queue.wait(250);
        } catch (InterruptedException e) {
          // If we set the thread as interrupted again, the next time the wait it's called it's going to return immediately
          threadInterrupted = true;
          log.warn("Thread interrupted while waiting for update queue to be empty. There are still {} elements in the queue.", 
              queue.size());
        }
      }
      int currentQueueSize = queue.size();
      // stall prevention
      if (currentQueueSize != lastQueueSize) {
        lastQueueSize = currentQueueSize;
        lastStallTime = -1;
      } else {
        lastQueueSize = currentQueueSize;
        if (lastStallTime == -1) {
          lastStallTime = System.nanoTime();
        } else {
          long currentStallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastStallTime);
          if (currentStallTime > stallTime) {
            throw new IOException("Task queue processing has stalled for " + currentStallTime + " ms with " + currentQueueSize + " remaining elements to process.");
//            threadInterrupted = true;
//            break;
          }
        }
      }
    }
    if (threadInterrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public void handleError(Throwable ex) {
    log.error("error", ex);
  }
  
  /**
   * Intended to be used as an extension point for doing post processing after a request completes.
   */
  public void onSuccess(HttpResponse resp) {
    // no-op by design, override to add functionality
  }

  @Override
  public synchronized void close() {
    if (closed) {
      interruptRunnerThreadsPolling();
      return;
    }
    closed = true;
    
    try {
      if (shutdownExecutor) {
        scheduler.shutdown();
        interruptRunnerThreadsPolling();
        try {
          if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) log
                .error("ExecutorService did not terminate");
          }
        } catch (InterruptedException ie) {
          scheduler.shutdownNow();
          Thread.currentThread().interrupt();
        }
      } else {
        interruptRunnerThreadsPolling();
      }
    } finally {
      if (internalHttpClient) IOUtils.closeQuietly(client);
      if (log.isDebugEnabled()) {
        log.debug("STATS pollInteruppts={} pollExists={} blockLoops={} emptyQueueLoops={}", pollInterrupts.get(), pollExits.get(), blockLoops.get(), emptyQueueLoops.get());
      }
    }
  }

  private void interruptRunnerThreadsPolling() {
    synchronized (runners) {
      for (Runner runner : runners) {
        runner.interruptPoll();
      }
    }
  }
  
  /**
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setConnectionTimeout(int timeout) {
    this.connectionTimeout = timeout;
  }

  /**
   * set soTimeout (read timeout) on the underlying HttpConnectionManager. This is desirable for queries, but probably
   * not for indexing.
   * 
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setSoTimeout(int timeout) {
    this.soTimeout = timeout;
  }

  public void shutdownNow() {
    if (closed) {
      return;
    }
    closed = true;
    try {

      if (shutdownExecutor) {
        scheduler.shutdown();
        interruptRunnerThreadsPolling();
        scheduler.shutdownNow(); // Cancel currently executing tasks
        try {
          if (!scheduler.awaitTermination(30, TimeUnit.SECONDS))
            log.error("ExecutorService did not terminate");
        } catch (InterruptedException ie) {
          scheduler.shutdownNow();
          Thread.currentThread().interrupt();
        }
      } else {
        interruptRunnerThreadsPolling();
      }
    } finally {
      if (internalHttpClient) IOUtils.closeQuietly(client);
    }
  }
  
  public void setParser(ResponseParser responseParser) {
    client.setParser(responseParser);
  }
  
  
  /**
   * @param pollQueueTime time for an open connection to wait for updates when
   * the queue is empty. 
   */
  public void setPollQueueTime(int pollQueueTime) {
    this.pollQueueTime = pollQueueTime;
    // make sure the stall time is larger than the polling time
    // to give a chance for the queue to change
    int minimalStallTime = pollQueueTime * 2;
    if (minimalStallTime > this.stallTime) {
      this.stallTime = minimalStallTime;
    }
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    client.setRequestWriter(requestWriter);
  }
  
  /**
   * Constructs {@link ConcurrentUpdateSolrClient} instances from provided configuration.
   */
  public static class Builder extends SolrClientBuilder<Builder> {
    protected String baseSolrUrl;
    protected int queueSize = 10;
    protected int threadCount;
    protected ExecutorService executorService;
    protected boolean streamDeletes;

    /**
     * Create a Builder object, based on the provided Solr URL.
     * 
     * Two different paths can be specified as a part of this URL:
     * 
     * 1) A path pointing directly at a particular core
     * <pre>
     *   SolrClient client = new ConcurrentUpdateSolrClient.Builder("http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     * Note that when a core is provided in the base URL, queries and other requests can be made without mentioning the
     * core explicitly.  However, the client can only send requests to that core.
     * 
     * 2) The path of the root Solr path ("/solr")
     * <pre>
     *   SolrClient client = new ConcurrentUpdateSolrClient.Builder("http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     * In this case the client is more flexible and can be used to send requests to any cores.  This flexibility though
     * requires that the core be specified on all requests. 
     */
    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }
    
    /**
     * The maximum number of requests buffered by the SolrClient's internal queue before being processed by background threads.
     *
     * This value should be carefully paired with the number of queue-consumer threads.  A queue with a maximum size
     * set too high may require more memory.  A queue with a maximum size set too low may suffer decreased throughput
     * as {@link ConcurrentUpdateSolrClient#request(SolrRequest)} calls block waiting to add requests to the queue.
     *
     * If not set, this defaults to 10.
     *
     * @see #withThreadCount(int)
     */
    public Builder withQueueSize(int queueSize) {
      if (queueSize <= 0) {
        throw new IllegalArgumentException("queueSize must be a positive integer.");
      }
      this.queueSize = queueSize;
      return this;
    }
    
    /**
     * The maximum number of threads used to empty {@link ConcurrentUpdateSolrClient}s queue.
     *
     * Threads are created when documents are added to the client's internal queue and exit when no updates remain in
     * the queue.
     * <p>
     * This value should be carefully paired with the maximum queue capacity.  A client with too few threads may suffer
     * decreased throughput as the queue fills up and {@link ConcurrentUpdateSolrClient#request(SolrRequest)} calls
     * block waiting to add requests to the queue.
     */
    public Builder withThreadCount(int threadCount) {
      if (threadCount <= 0) {
        throw new IllegalArgumentException("threadCount must be a positive integer.");
      }
      
      this.threadCount = threadCount;
      return this;
    }
    
    /**
     * Provides the {@link ExecutorService} for the created client to use when servicing the update-request queue.
     */
    public Builder withExecutorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }
    
    /**
     * Configures created clients to always stream delete requests.
     *
     * Streamed deletes are put into the update-queue and executed like any other update request.
     */
    public Builder alwaysStreamDeletes() {
      this.streamDeletes = true;
      return this;
    }
    
    /**
     * Configures created clients to not stream delete requests.
     *
     * With this option set when the created ConcurrentUpdateSolrClient sents a delete request it will first will lock
     * the queue and block until all queued updates have been sent, and then send the delete request.
     */
    public Builder neverStreamDeletes() {
      this.streamDeletes = false;
      return this;
    }
    
    /**
     * Create a {@link ConcurrentUpdateSolrClient} based on the provided configuration options.
     */
    public ConcurrentUpdateSolrClient build() {
      if (baseSolrUrl == null) {
        throw new IllegalArgumentException("Cannot create HttpSolrClient without a valid baseSolrUrl!");
      }
      
      return new ConcurrentUpdateSolrClient(this);
    }

    @Override
    public Builder getThis() {
      return this;
    }
  }
}
