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
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient.Update;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @lucene.experimental
 */
public class ConcurrentUpdateHttp2SolrClient extends SolrClient {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Update END_UPDATE = new Update(null, null);

  private Http2SolrClient client;
  private final String basePath;
  private final CustomBlockingQueue<Update> queue;
  private final ExecutorService scheduler;
  private final Queue<Runner> runners;
  private final int threadCount;

  private boolean shutdownClient;
  private boolean shutdownExecutor;
  private int pollQueueTime = 250;
  private final boolean streamDeletes;
  private volatile boolean closed;
  private volatile CountDownLatch lock = null; // used to block everything

  private static class CustomBlockingQueue<E> implements Iterable<E>{
    private final BlockingQueue<E> queue;
    private final Semaphore available;
    private final int queueSize;
    private final E backdoorE;

    public CustomBlockingQueue(int queueSize, int maxConsumers, E backdoorE) {
      queue = new LinkedBlockingQueue<>();
      available = new Semaphore(queueSize);
      this.queueSize = queueSize;
      this.backdoorE = backdoorE;
    }

    public boolean offer(E e) {
      boolean success = available.tryAcquire();
      if (success) {
        queue.offer(e);
      }
      return success;
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
      boolean success = available.tryAcquire(timeout, unit);
      if (success) {
        queue.offer(e, timeout, unit);
      }
      return success;
    }

    public boolean isEmpty() {
      return size() == 0;
    }

    public E poll(int timeout, TimeUnit unit) throws InterruptedException {
      E e = queue.poll(timeout, unit);
      if (e == null) {
        return null;
      }
      if (e == backdoorE)
        return null;
      available.release();
      return e;
    }

    public boolean add(E e) {
      boolean success = available.tryAcquire();
      if (success) {
        queue.add(e);
      } else {
        throw new IllegalStateException("Queue is full");
      }
      return true;
    }

    public int size() {
      return queueSize - available.availablePermits();
    }

    public int remainingCapacity() {
      return available.availablePermits();
    }

    @Override
    public Iterator<E> iterator() {
      return queue.iterator();
    }

    public void backdoorOffer() {
      queue.offer(backdoorE);
    }
  }

  protected ConcurrentUpdateHttp2SolrClient(Builder builder) {
    this.client = builder.client;
    this.shutdownClient = builder.closeHttp2Client;
    this.threadCount = builder.threadCount;
    this.queue = new CustomBlockingQueue<>(builder.queueSize, threadCount, END_UPDATE);
    this.runners = new LinkedList<>();
    this.streamDeletes = builder.streamDeletes;
    this.basePath = builder.baseSolrUrl;

    if (builder.executorService != null) {
      this.scheduler = builder.executorService;
      this.shutdownExecutor = false;
    } else {
      this.scheduler = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("concurrentUpdateScheduler"));
      this.shutdownExecutor = true;
    }

  }

  /**
   * Opens a connection and sends everything...
   */
  class Runner implements Runnable {

    @Override
    public void run() {
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

    //
    // Pull from the queue multiple times and streams over a single connection.
    // Exits on exception, interruption, or an empty queue to pull from.
    //
    void sendUpdateStream() throws Exception {

      try {
        while (!queue.isEmpty()) {
          InputStream rspBody = null;
          try {
            Update update;
            notifyQueueAndRunnersIfEmptyQueue();
            update = queue.poll(pollQueueTime, TimeUnit.MILLISECONDS);

            if (update == null) {
              break;
            }

            InputStreamResponseListener responseListener = null;
            try (Http2SolrClient.OutStream out = client.initOutStream(basePath, update.getRequest(),
                update.getCollection())) {
              Update upd = update;
              while (upd != null) {
                UpdateRequest req = upd.getRequest();
                if (!out.belongToThisStream(req, upd.getCollection())) {
                  queue.add(upd); // Request has different params or destination core/collection, return to queue
                  break;
                }
                client.send(out, upd.getRequest(), upd.getCollection());
                out.flush();

                notifyQueueAndRunnersIfEmptyQueue();
                upd = queue.poll(pollQueueTime, TimeUnit.MILLISECONDS);
              }
              responseListener = out.getResponseListener();
            }

            Response response = responseListener.get(client.getIdleTimeout(), TimeUnit.MILLISECONDS);
            rspBody = responseListener.getInputStream();

            int statusCode = response.getStatus();
            if (statusCode != HttpStatus.OK_200) {
              StringBuilder msg = new StringBuilder();
              msg.append(response.getReason());
              msg.append("\n\n\n\n");
              msg.append("request: ").append(basePath);

              SolrException solrExc;
              NamedList<String> metadata = null;
              // parse out the metadata from the SolrException
              try {
                String encoding = "UTF-8"; // default
                NamedList<Object> resp = client.getParser().processResponse(rspBody, encoding);
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
                log.warn("Failed to parse error response from " + basePath + " due to: " + exc);
              } finally {
                solrExc = new HttpSolrClient.RemoteSolrException(basePath , statusCode, msg.toString(), null);
                if (metadata != null) {
                  solrExc.setMetadata(metadata);
                }
              }

              handleError(solrExc);
            } else {
              onSuccess(response, rspBody);
            }

          } finally {
            try {
              consumeFully(rspBody);
            } catch (Exception e) {
              log.error("Error consuming and closing http response stream.", e);
            }
            notifyQueueAndRunnersIfEmptyQueue();
          }
        }
      } catch (InterruptedException e) {
        log.error("Interrupted on polling from queue", e);
      }

    }
  }

  private void consumeFully(InputStream is) {
    if (is != null) {
      try (is) {
        // make sure the stream is full read
        is.skip(is.available());
        while (is.read() != -1) {
        }
      } catch (UnsupportedOperationException e) {
        // nothing to do then
      } catch (IOException e) {
        // quiet
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
    MDC.put("ConcurrentUpdateHttp2SolrClient.url", client.getBaseURL());
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
      MDC.remove("ConcurrentUpdateHttp2SolrClient.url");
    }
  }

  @Override
  public NamedList<Object> request(final SolrRequest request, String collection)
      throws SolrServerException, IOException {
    if (!(request instanceof UpdateRequest)) {
      request.setBasePath(basePath);
      return client.request(request, collection);
    }
    UpdateRequest req = (UpdateRequest) request;
    req.setBasePath(basePath);
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

  public synchronized void blockUntilFinished() {
    lock = new CountDownLatch(1);
    try {

      waitForEmptyQueue();
      interruptRunnerThreadsPolling();

      synchronized (runners) {

        // NOTE: if the executor is shut down, runners may never become empty (a scheduled task may never be run,
        // which means it would never remove itself from the runners list. This is why we don't wait forever
        // and periodically check if the scheduler is shutting down.
        int loopCount = 0;
        while (!runners.isEmpty()) {

          if (scheduler.isShutdown())
            break;

          loopCount++;

          // Need to check if the queue is empty before really considering this is finished (SOLR-4260)
          int queueSize = queue.size();
          if (queueSize > 0 && runners.isEmpty()) {
            // TODO: can this still happen?
            log.warn("No more runners, but queue still has " +
                queueSize + " adding more runners to process remaining requests on queue");
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

  private void waitForEmptyQueue() {
    boolean threadInterrupted = Thread.currentThread().isInterrupted();

    while (!queue.isEmpty()) {
      if (scheduler.isTerminated()) {
        log.warn("The task queue still has elements but the update scheduler {} is terminated. Can't process any more tasks. "
            + "Queue size: {}, Runners: {}. Current thread Interrupted? {}", scheduler, queue.size(), runners.size(), threadInterrupted);
        break;
      }

      synchronized (runners) {
        int queueSize = queue.size();
        if (queueSize > 0 && runners.isEmpty()) {
          log.warn("No more runners, but queue still has " +
              queueSize + " adding more runners to process remaining requests on queue");
          addRunner();
        }
      }
      synchronized (queue) {
        try {
          queue.wait(250);
        } catch (InterruptedException e) {
          // If we set the thread as interrupted again, the next time the wait it's called i t's going to return immediately
          threadInterrupted = true;
          log.warn("Thread interrupted while waiting for update queue to be empty. There are still {} elements in the queue.",
              queue.size());
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
   * @param respBody the body of the response, subclasses must not close this stream.
   */
  public void onSuccess(Response resp, InputStream respBody) {
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
      if (shutdownClient)
        client.close();
    }
  }

  private void interruptRunnerThreadsPolling() {
    synchronized (runners) {
      for (Runner ignored : runners) {
        queue.backdoorOffer();
      }
    }
  }

  public void shutdownNow() {
    if (closed) {
      return;
    }
    closed = true;

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
  }

  /**
   * @param pollQueueTime time for an open connection to wait for updates when
   * the queue is empty.
   */
  public void setPollQueueTime(int pollQueueTime) {
    this.pollQueueTime = pollQueueTime;
  }

  /**
   * Constructs {@link ConcurrentUpdateHttp2SolrClient} instances from provided configuration.
   */
  public static class Builder {
    protected Http2SolrClient client;
    protected String baseSolrUrl;
    protected int queueSize = 10;
    protected int threadCount;
    protected ExecutorService executorService;
    protected boolean streamDeletes;
    protected boolean closeHttp2Client;

    public Builder(String baseSolrUrl, Http2SolrClient client) {
      this(baseSolrUrl, client, false);
    }

    public Builder(String baseSolrUrl, Http2SolrClient client, boolean closeHttp2Client) {
      this.baseSolrUrl = baseSolrUrl;
      this.client = client;
      this.closeHttp2Client = closeHttp2Client;
    }

    /**
     * The maximum number of requests buffered by the SolrClient's internal queue before being processed by background threads.
     *
     * This value should be carefully paired with the number of queue-consumer threads.  A queue with a maximum size
     * set too high may require more memory.  A queue with a maximum size set too low may suffer decreased throughput
     * as {@link ConcurrentUpdateHttp2SolrClient#request(SolrRequest)} calls block waiting to add requests to the queue.
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
     * The maximum number of threads used to empty {@link ConcurrentUpdateHttp2SolrClient}s queue.
     *
     * Threads are created when documents are added to the client's internal queue and exit when no updates remain in
     * the queue.
     * <p>
     * This value should be carefully paired with the maximum queue capacity.  A client with too few threads may suffer
     * decreased throughput as the queue fills up and {@link ConcurrentUpdateHttp2SolrClient#request(SolrRequest)} calls
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
     * With this option set when the created ConcurrentUpdateHttp2SolrClient sents a delete request it will first will lock
     * the queue and block until all queued updates have been sent, and then send the delete request.
     */
    public Builder neverStreamDeletes() {
      this.streamDeletes = false;
      return this;
    }

    /**
     * Create a {@link ConcurrentUpdateHttp2SolrClient} based on the provided configuration options.
     */
    public ConcurrentUpdateHttp2SolrClient build() {
      if (baseSolrUrl == null) {
        throw new IllegalArgumentException("Cannot create HttpSolrClient without a valid baseSolrUrl!");
      }

      return new ConcurrentUpdateHttp2SolrClient(this);
    }

  }
}
