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
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
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
  final ExecutorService scheduler;
  final Queue<Runner> runners;
  volatile CountDownLatch lock = null; // used to block everything
  final int threadCount;
  boolean shutdownExecutor = false;
  int pollQueueTime = 250;
  private final boolean streamDeletes;

  /**
   * Uses an internally managed HttpClient instance.
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
    shutdownExecutor = true;
  }
  
  public ConcurrentUpdateSolrServer(String solrServerUrl,
      HttpClient client, int queueSize, int threadCount) {
    this(solrServerUrl, client, queueSize, threadCount, Executors.newCachedThreadPool(
        new SolrjNamedThreadFactory("concurrentUpdateScheduler")));
    shutdownExecutor = true;
  }

  /**
   * Uses the supplied HttpClient to send documents to the Solr server.
   */
  public ConcurrentUpdateSolrServer(String solrServerUrl,
      HttpClient client, int queueSize, int threadCount, ExecutorService es) {
    this(solrServerUrl, client, queueSize, threadCount, es, false);
  }
  
  /**
   * Uses the supplied HttpClient to send documents to the Solr server.
   */
  public ConcurrentUpdateSolrServer(String solrServerUrl,
      HttpClient client, int queueSize, int threadCount, ExecutorService es, boolean streamDeletes) {
    this.server = new HttpSolrServer(solrServerUrl, client);
    this.server.setFollowRedirects(false);
    queue = new LinkedBlockingQueue<>(queueSize);
    this.threadCount = threadCount;
    runners = new LinkedList<>();
    scheduler = es;
    this.streamDeletes = streamDeletes;
  }

  public Set<String> getQueryParams() {
    return this.server.getQueryParams();
  }

  /**
   * Expert Method.
   * @param queryParams set of param keys to only send via the query string
   */
  public void setQueryParams(Set<String> queryParams) {
    this.server.setQueryParams(queryParams);
  }
  
  /**
   * Opens a connection and sends everything...
   */
  class Runner implements Runnable {
    final Lock runnerLock = new ReentrantLock();

    @Override
    public void run() {
      runnerLock.lock();

      log.debug("starting runner: {}", this);
      HttpPost method = null;
      HttpResponse response = null;            
      try {
        while (!queue.isEmpty()) {
          try {
            final UpdateRequest updateRequest = 
                queue.poll(pollQueueTime, TimeUnit.MILLISECONDS);
            if (updateRequest == null)
              break;
                       
            String contentType = server.requestWriter.getUpdateContentType();
            final boolean isXml = ClientUtils.TEXT_XML.equals(contentType);

            final ModifiableSolrParams origParams = new ModifiableSolrParams(updateRequest.getParams());

            EntityTemplate template = new EntityTemplate(new ContentProducer() {

              @Override
              public void writeTo(OutputStream out) throws IOException {
                try {
                  if (isXml) {
                    out.write("<stream>".getBytes(StandardCharsets.UTF_8)); // can be anything
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
                          fmt = "<optimize waitSearcher=\"%s\" />";
                        } else if (params.getBool(UpdateParams.COMMIT, false)) {
                          fmt = "<commit waitSearcher=\"%s\" />";
                        }
                        if (fmt != null) {
                          byte[] content = String.format(Locale.ROOT,
                              fmt,
                              params.getBool(UpdateParams.WAIT_SEARCHER, false)
                                  + "").getBytes(StandardCharsets.UTF_8);
                          out.write(content);
                        }
                      }
                    }
                    out.flush();
                    req = queue.poll(pollQueueTime, TimeUnit.MILLISECONDS);
                  }
                  
                  if (isXml) {
                    out.write("</stream>".getBytes(StandardCharsets.UTF_8));
                  }

                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  log.warn("", e);
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
            if (statusCode != HttpStatus.SC_OK) {
              StringBuilder msg = new StringBuilder();
              msg.append(response.getStatusLine().getReasonPhrase());
              msg.append("\n\n\n\n");
              msg.append("request: ").append(method.getURI());

              SolrException solrExc = new SolrException(ErrorCode.getErrorCode(statusCode), msg.toString());
              // parse out the metadata from the SolrException
              try {
                NamedList<Object> resp =
                    server.parser.processResponse(response.getEntity().getContent(),
                        response.getEntity().getContentType().getValue());
                NamedList<Object> error = (NamedList<Object>) resp.get("error");
                if (error != null)
                  solrExc.setMetadata((NamedList<String>) error.get("metadata"));
              } catch (Exception exc) {
                // don't want to fail to report error if parsing the response fails
                log.warn("Failed to parse error response from "+server.getBaseURL()+" due to: "+exc);
              }

              handleError(solrExc);
            } else {
              onSuccess(response);
            }
          } finally {
            try {
              if (response != null) {
                response.getEntity().getContent().close();
              }
            } catch (Exception ex) {
              log.warn("", ex);
            }
          }
        }
      } catch (Throwable e) {
        if (e instanceof OutOfMemoryError) {
          throw (OutOfMemoryError) e;
        }
        handleError(e);
      } finally {
        synchronized (runners) {
          if (runners.size() == 1 && !queue.isEmpty()) {
            // keep this runner alive
            scheduler.execute(this);
          } else {
            runners.remove(this);
            if (runners.isEmpty())
              runners.notifyAll();
          }
        }

        log.debug("finished: {}", this);
        runnerLock.unlock();
      }
    }
  }

  @Override
  public NamedList<Object> request(final SolrRequest request)
      throws SolrServerException, IOException {
    if (!(request instanceof UpdateRequest)) {
      return server.request(request);
    }
    UpdateRequest req = (UpdateRequest) request;

    // this happens for commit...
    if (streamDeletes) {
      if ((req.getDocuments() == null || req.getDocuments().isEmpty())
          && (req.getDeleteById() == null || req.getDeleteById().isEmpty())
          && (req.getDeleteByIdMap() == null || req.getDeleteByIdMap().isEmpty())) {
        if (req.getDeleteQuery() == null) {
          blockUntilFinished();
          return server.request(request);
        }
      }
    } else {
      if ((req.getDocuments() == null || req.getDocuments().isEmpty())) {
        blockUntilFinished();
        return server.request(request);
      }
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
          // see if queue is half full and we can add more runners
          // special case: if only using a threadCount of 1 and the queue
          // is filling up, allow 1 add'l runner to help process the queue
          if (runners.isEmpty() || (queue.remainingCapacity() < queue.size() && runners.size() < threadCount))
          {
            // We need more runners, so start a new one.
            Runner r = new Runner();
            runners.add(r);
            scheduler.execute(r);
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
          success = queue.offer(req, 100, TimeUnit.MILLISECONDS);
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
      synchronized (runners) {
        while (!runners.isEmpty()) {
          try {
            runners.wait();
          } catch (InterruptedException e) {
            Thread.interrupted();
          }
          
          if (scheduler.isTerminated())
            break;
                      
          // if we reach here, then we probably got the notifyAll, but need to check if
          // the queue is empty before really considering this is finished (SOLR-4260)
          int queueSize = queue.size();
          if (queueSize > 0) {
            log.warn("No more runners, but queue still has "+
              queueSize+" adding more runners to process remaining requests on queue");
            Runner r = new Runner();
            runners.add(r);
            scheduler.execute(r);
          }
        }
      }
    } finally {
      lock.countDown();
      lock = null;
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
  public void shutdown() {
    server.shutdown();
    if (shutdownExecutor) {
      scheduler.shutdown();
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
    }
  }
  
  public void setConnectionTimeout(int timeout) {
    HttpClientUtil.setConnectionTimeout(server.getHttpClient(), timeout);
  }

  /**
   * set soTimeout (read timeout) on the underlying HttpConnectionManager. This is desirable for queries, but probably
   * not for indexing.
   */
  public void setSoTimeout(int timeout) {
    HttpClientUtil.setSoTimeout(server.getHttpClient(), timeout);
  }

  public void shutdownNow() {
    server.shutdown();
    if (shutdownExecutor) {
      scheduler.shutdownNow(); // Cancel currently executing tasks
      try {
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) 
          log.error("ExecutorService did not terminate");
      } catch (InterruptedException ie) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }    
  }
  
  public void setParser(ResponseParser responseParser) {
    server.setParser(responseParser);
  }
  
  
  /**
   * @param pollQueueTime time for an open connection to wait for updates when
   * the queue is empty. 
   */
  public void setPollQueueTime(int pollQueueTime) {
    this.pollQueueTime = pollQueueTime;
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    server.setRequestWriter(requestWriter);
  }
}
