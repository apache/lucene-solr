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
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConcurrentUpdateSolrServer buffers all added documents and writes
 * them into open HTTP connections. This class is thread safe.
 * 
 * Params from {@link UpdateRequest} are converted to http request
 * parameters. When params change between UpdateRequests a new HTTP
 * request is started.
 * 
 * Although any SolrServer request can be made with this implementation, it is
 * only recommended to use ConcurrentUpdateSolrServer with /update
 * requests. The class {@link HttpSolrServer} is better suited for the
 * query interface.
 */
public class ConcurrentUpdateSolrServer extends SolrServer {
  private static final long serialVersionUID = 1L;
  static final Logger log = LoggerFactory
      .getLogger(ConcurrentUpdateSolrServer.class);
  private HttpSolrServer server;
  final BlockingQueue<UpdateRequest> queue;
  final ExecutorService scheduler = Executors.newCachedThreadPool(
      new SolrjNamedThreadFactory("concurrentUpdateScheduler"));
  final Queue<Runner> runners;
  volatile CountDownLatch lock = null; // used to block everything
  final int threadCount;

  /**
   * Uses an internal ThreadSafeClientConnManager to manage http
   * connections.
   * 
   * @param solrServerUrl
   *          The Solr server URL
   * @param queueSize
   *          The buffer size before the documents are sent to the server
   * @param threadCount
   *          The number of background threads used to empty the queue
   */
  public ConcurrentUpdateSolrServer(String solrServerUrl, int queueSize,
      int threadCount) {
    this(solrServerUrl, null, queueSize, threadCount);
  }

  /**
   * Uses the supplied HttpClient to send documents to the Solr server, the
   * HttpClient should be instantiated using a 
   * ThreadSafeClientConnManager.
   */
  public ConcurrentUpdateSolrServer(String solrServerUrl,
      HttpClient client, int queueSize, int threadCount) {
    this.server = new HttpSolrServer(solrServerUrl, client);
    this.server.setFollowRedirects(false);
    queue = new LinkedBlockingQueue<UpdateRequest>(queueSize);
    this.threadCount = threadCount;
    runners = new LinkedList<Runner>();
  }

  /**
   * Opens a connection and sends everything...
   */
  class Runner implements Runnable {
    final Lock runnerLock = new ReentrantLock();

    public void run() {
      runnerLock.lock();

      // info is ok since this should only happen once for each thread
      log.info("starting runner: {}", this);
      HttpPost method = null;
      HttpResponse response = null;
      try {
        while (!queue.isEmpty()) {
          try {
            final UpdateRequest updateRequest = queue.poll(250,
                TimeUnit.MILLISECONDS);
            if (updateRequest == null)
              break;

            String contentType = server.requestWriter.getUpdateContentType();
            final boolean isXml = ClientUtils.TEXT_XML.equals(contentType);

            final ModifiableSolrParams origParams = new ModifiableSolrParams(updateRequest.getParams());

            EntityTemplate template = new EntityTemplate(new ContentProducer() {

              public void writeTo(OutputStream out) throws IOException {
                try {
                  if (isXml) {
                    out.write("<stream>".getBytes("UTF-8")); // can be anything
                  }
                  UpdateRequest req = updateRequest;
                  while (req != null) {
                    SolrParams currentParams = new ModifiableSolrParams(req.getParams());
                    if (!origParams.toNamedList().equals(currentParams.toNamedList())) {
                      queue.add(req); // params are different, push back to queue
                      break;
                    }
                    
                    server.requestWriter.write(req, out);
                    if (isXml) {
                      // check for commit or optimize
                      SolrParams params = req.getParams();
                      if (params != null) {
                        String fmt = null;
                        if (params.getBool(UpdateParams.OPTIMIZE, false)) {
                          fmt = "<optimize waitSearcher=\"%s\" waitFlush=\"%s\" />";
                        } else if (params.getBool(UpdateParams.COMMIT, false)) {
                          fmt = "<commit waitSearcher=\"%s\" waitFlush=\"%s\" />";
                        }
                        if (fmt != null) {
                          byte[] content = String.format(Locale.ROOT,
                              fmt,
                              params.getBool(UpdateParams.WAIT_SEARCHER, false)
                                  + "").getBytes("UTF-8");
                          out.write(content);
                        }
                      }
                    }
                    out.flush();
                    req = queue.poll(250, TimeUnit.MILLISECONDS);
                  }
                  if (isXml) {
                    out.write("</stream>".getBytes("UTF-8"));
                  }

                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            });
            
            // The parser 'wt=' and 'version=' params are used instead of the
            // original params
            ModifiableSolrParams requestParams = new ModifiableSolrParams(origParams);
            requestParams.set(CommonParams.WT, server.parser.getWriterType());
            requestParams.set(CommonParams.VERSION, server.parser.getVersion());

            method = new HttpPost(server.getBaseURL() + "/update"
                + ClientUtils.toQueryString(requestParams, false));
            method.setEntity(template);
            method.addHeader("User-Agent", HttpSolrServer.AGENT);
            method.addHeader("Content-Type", contentType);
            
            
            response = server.getHttpClient().execute(method);
            int statusCode = response.getStatusLine().getStatusCode();
            log.info("Status for: "
                + updateRequest.getDocuments().get(0).getFieldValue("id")
                + " is " + statusCode);
            if (statusCode != HttpStatus.SC_OK) {
              StringBuilder msg = new StringBuilder();
              msg.append(response.getStatusLine().getReasonPhrase());
              msg.append("\n\n");
              msg.append("\n\n");
              msg.append("request: ").append(method.getURI());
              handleError(new Exception(msg.toString()));
            }
          } finally {
            try {
              if (response != null) {
                response.getEntity().getContent().close();
              }
            } catch (Exception ex) {
            }
          }
        }
      } catch (Throwable e) {
        handleError(e);
      } finally {

        // remove it from the list of running things unless we are the last
        // runner and the queue is full...
        // in which case, the next queue.put() would block and there would be no
        // runners to handle it.
        // This case has been further handled by using offer instead of put, and
        // using a retry loop
        // to avoid blocking forever (see request()).
        synchronized (runners) {
          if (runners.size() == 1 && queue.remainingCapacity() == 0) {
            // keep this runner alive
            scheduler.execute(this);
          } else {
            runners.remove(this);
          }
        }

        log.info("finished: {}", this);
        runnerLock.unlock();
      }
    }
  }

  public NamedList<Object> request(final SolrRequest request)
      throws SolrServerException, IOException {
    if (!(request instanceof UpdateRequest)) {
      return server.request(request);
    }
    UpdateRequest req = (UpdateRequest) request;

    // this happens for commit...
    if (req.getDocuments() == null || req.getDocuments().isEmpty()) {
      blockUntilFinished();
      return server.request(request);
    }

    SolrParams params = req.getParams();
    if (params != null) {
      // check if it is waiting for the searcher
      if (params.getBool(UpdateParams.WAIT_SEARCHER, false)) {
        log.info("blocking for commit/optimize");
        blockUntilFinished(); // empty the queue
        return server.request(request);
      }
    }

    try {
      CountDownLatch tmpLock = lock;
      if (tmpLock != null) {
        tmpLock.await();
      }

      boolean success = queue.offer(req);

      for (;;) {
        synchronized (runners) {
          if (runners.isEmpty() || (queue.remainingCapacity() < queue.size() // queue
                                                                             // is
                                                                             // half
                                                                             // full
                                                                             // and
                                                                             // we
                                                                             // can
                                                                             // add
                                                                             // more
                                                                             // runners
              && runners.size() < threadCount)) {
            // We need more runners, so start a new one.
            Runner r = new Runner();
            runners.add(r);
            scheduler.execute(r);
          } else {
            // break out of the retry loop if we added the element to the queue
            // successfully, *and*
            // while we are still holding the runners lock to prevent race
            // conditions.
            // race conditions.
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
          success = queue.offer(req, 100, TimeUnit.MILLISECONDS);
        }

      }

    } catch (InterruptedException e) {
      log.error("interrupted", e);
      throw new IOException(e.getLocalizedMessage());
    }

    // RETURN A DUMMY result
    NamedList<Object> dummy = new NamedList<Object>();
    dummy.add("NOTE", "the request is processed in a background stream");
    return dummy;
  }

  public synchronized void blockUntilFinished() {
    lock = new CountDownLatch(1);
    try {
      // Wait until no runners are running
      for (;;) {
        Runner runner;
        synchronized (runners) {
          runner = runners.peek();
        }
        if (runner == null)
          break;
        runner.runnerLock.lock();
        runner.runnerLock.unlock();
      }
    } finally {
      lock.countDown();
      lock = null;
    }
  }

  public void handleError(Throwable ex) {
    log.error("error", ex);
  }

  @Override
  public void shutdown() {
    server.shutdown();
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(60, TimeUnit.SECONDS))
          log.error("ExecutorService did not terminate");
      }
    } catch (InterruptedException ie) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public void shutdownNow() {
    server.shutdown();
    scheduler.shutdownNow(); // Cancel currently executing tasks
    try {
      if (!scheduler.awaitTermination(30, TimeUnit.SECONDS))
        log.error("ExecutorService did not terminate");
    } catch (InterruptedException ie) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public void setParser(ResponseParser responseParser) {
    server.setParser(responseParser);
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    server.setRequestWriter(requestWriter);
  }
}
