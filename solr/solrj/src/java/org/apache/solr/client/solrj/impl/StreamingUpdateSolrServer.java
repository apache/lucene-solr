/**
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
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamingUpdateSolrServer} buffers all added documents and writes them
 * into open HTTP connections. This class is thread safe.
 * 
 * Although any SolrServer request can be made with this implementation, 
 * it is only recommended to use the {@link StreamingUpdateSolrServer} with
 * /update requests.  The query interface is better suited for 
 * 
 *
 * @since solr 1.4
 */
public class StreamingUpdateSolrServer extends CommonsHttpSolrServer
{
  static final Logger log = LoggerFactory.getLogger( StreamingUpdateSolrServer.class );
  
  final BlockingQueue<UpdateRequest> queue;
  final ExecutorService scheduler = Executors.newCachedThreadPool();
  final String updateUrl = "/update";
  final Queue<Runner> runners;
  volatile CountDownLatch lock = null;  // used to block everything
  final int threadCount;

  /**
   * Uses an internal MultiThreadedHttpConnectionManager to manage http connections
   *
   * @param solrServerUrl The Solr server URL
   * @param queueSize     The buffer size before the documents are sent to the server
   * @param threadCount   The number of background threads used to empty the queue
   * @throws MalformedURLException
   */
  public StreamingUpdateSolrServer(String solrServerUrl, int queueSize, int threadCount) throws MalformedURLException {
    this(solrServerUrl, null, queueSize, threadCount);
  }

  /**
   * Uses the supplied HttpClient to send documents to the Solr server, the HttpClient should be instantiated using a
   * MultiThreadedHttpConnectionManager.
   */
  public StreamingUpdateSolrServer(String solrServerUrl, HttpClient client, int queueSize, int threadCount) throws MalformedURLException {
    super(solrServerUrl, client);
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
      log.info( "starting runner: {}" , this );
      PostMethod method = null;
      try {
        while (!queue.isEmpty())  {
          try {
            final UpdateRequest updateRequest = queue.poll(250, TimeUnit.MILLISECONDS);
            if (updateRequest == null) break;
            RequestEntity request = new RequestEntity() {
              // we don't know the length
              public long getContentLength() { return -1; }
              public String getContentType() { return requestWriter.getUpdateContentType(); }
              public boolean isRepeatable()  { return false; }

              public void writeRequest(OutputStream out) throws IOException {
                try {
                  if (ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType())) {
                    out.write("<stream>".getBytes("UTF-8")); // can be anything
                  }
                  UpdateRequest req = updateRequest;
                  while (req != null) {
                    requestWriter.write(req, out);
                    if (ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType())) {
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
                          byte[] content = String.format(fmt,
                              params.getBool(UpdateParams.WAIT_SEARCHER, false) + "").getBytes("UTF-8");
                          out.write(content);
                        }
                      }
                    }
                    out.flush();
                    req = queue.poll(250, TimeUnit.MILLISECONDS);
                  }
                  if (ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType())) {
                    out.write("</stream>".getBytes("UTF-8"));
                  }
                  out.flush();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            };

            String path = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType()) ? "/update" : "/update/javabin";

            method = new PostMethod(_baseURL+path );
            method.setRequestEntity( request );
            method.setFollowRedirects( false );
            method.addRequestHeader( "User-Agent", AGENT );
            
            int statusCode = getHttpClient().executeMethod(method);
            log.info("Status for: " + updateRequest.getDocuments().get(0).getFieldValue("id") + " is " + statusCode);
            if (statusCode != HttpStatus.SC_OK) {
              StringBuilder msg = new StringBuilder();
              msg.append( method.getStatusLine().getReasonPhrase() );
              msg.append( "\n\n" );
              msg.append( method.getStatusText() );
              msg.append( "\n\n" );
              msg.append("request: ").append(method.getURI());
              handleError( new Exception( msg.toString() ) );
            }
          } finally {
            try {
              // make sure to release the connection
              if(method != null)
                method.releaseConnection();
            }
            catch( Exception ex ){}
          }
        }
      }
      catch (Throwable e) {
        handleError( e );
      }
      finally {

        // remove it from the list of running things unless we are the last runner and the queue is full...
        // in which case, the next queue.put() would block and there would be no runners to handle it.
        // This case has been further handled by using offer instead of put, and using a retry loop
        // to avoid blocking forever (see request()).
        synchronized (runners) {
          if (runners.size() == 1 && queue.remainingCapacity() == 0) {
           // keep this runner alive
           scheduler.execute(this);
          } else {
            runners.remove( this );
          }
        }

        log.info( "finished: {}" , this );
        runnerLock.unlock();
      }
    }
  }
  
  @Override
  public NamedList<Object> request( final SolrRequest request ) throws SolrServerException, IOException
  {
    if( !(request instanceof UpdateRequest) ) {
      return super.request( request );
    }
    UpdateRequest req = (UpdateRequest)request;
    
    // this happens for commit...
    if( req.getDocuments()==null || req.getDocuments().isEmpty() ) {
      blockUntilFinished();
      return super.request( request );
    }

    SolrParams params = req.getParams();
    if( params != null ) {
      // check if it is waiting for the searcher
      if( params.getBool( UpdateParams.WAIT_SEARCHER, false ) ) {
        log.info( "blocking for commit/optimize" );
        blockUntilFinished();  // empty the queue
        return super.request( request );
      }
    }

    try {
      CountDownLatch tmpLock = lock;
      if( tmpLock != null ) {
        tmpLock.await();
      }

      boolean success = queue.offer(req);

      for(;;) {
        synchronized( runners ) {
          if( runners.isEmpty()
                  || (queue.remainingCapacity() < queue.size()    // queue is half full and we can add more runners
                  && runners.size() < threadCount) )
          {
            // We need more runners, so start a new one.
            Runner r = new Runner();
            runners.add( r );
            scheduler.execute( r );
          } else {
            // break out of the retry loop if we added the element to the queue successfully, *and*
            // while we are still holding the runners lock to prevent race conditions.
            // race conditions.
            if (success) break;
          }
        }

        // Retry to add to the queue w/o the runners lock held (else we risk temporary deadlock)
        // This retry could also fail because
        // 1) existing runners were not able to take off any new elements in the queue
        // 2) the queue was filled back up since our last try
        // If we succeed, the queue may have been completely emptied, and all runners stopped.
        // In all cases, we should loop back to the top to see if we need to start more runners.
        //
        if (!success) {
          success = queue.offer(req, 100, TimeUnit.MILLISECONDS);
        }

      }


    }
    catch (InterruptedException e) {
      log.error( "interrupted", e );
      throw new IOException( e.getLocalizedMessage() );
    }
    
    // RETURN A DUMMY result
    NamedList<Object> dummy = new NamedList<Object>();
    dummy.add( "NOTE", "the request is processed in a background stream" );
    return dummy;
  }

  public synchronized void blockUntilFinished()
  {
    lock = new CountDownLatch(1);
    try {
      // Wait until no runners are running
      for(;;) {
        Runner runner;
        synchronized(runners) {
          runner = runners.peek();
        }
        if (runner == null) break;
        runner.runnerLock.lock();
        runner.runnerLock.unlock();
      }
    } finally {
      lock.countDown();
      lock=null;
    }
  }
  
  public void handleError( Throwable ex )
  {
    log.error( "error", ex );
  }
}
