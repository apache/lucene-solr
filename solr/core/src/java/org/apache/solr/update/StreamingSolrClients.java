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
package org.apache.solr.update;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.eclipse.jetty.client.api.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSolrClients {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Http2SolrClient httpClient;

  private Map<String, SingleStreamClient> clients = new HashMap<>();
  private List<Error> errors = Collections.synchronizedList(new ArrayList<>());

  private ExecutorService updateExecutor;

  public StreamingSolrClients(UpdateShardHandler updateShardHandler) {
    this.updateExecutor = updateShardHandler.getUpdateExecutor();
    this.httpClient = updateShardHandler.getUpdateOnlyHttpClient();
  }

  public List<Error> getErrors() {
    return errors;
  }

  public void clearErrors() {
    errors.clear();
  }

  public synchronized SingleStreamClient getClient(final SolrCmdDistributor.Req req) {
    String url = getFullUrl(req.node.getUrl());
    SingleStreamClient client = clients.get(url);
    if (client == null) {
      client = new SingleStreamClient(httpClient, url);
      clients.put(url, client);
    }

    return client;
  }

  class SingleStreamClient {

    private final Http2SolrClient client;
    private final String basePath;
    private Http2SolrClient.OutStream outStream;
    private SolrCmdDistributor.Req lastReq;
    private int numRequests = 0;

    public SingleStreamClient(Http2SolrClient client, String basePath) {
      this.client = client;
      this.basePath = basePath;
    }

    public void request(SolrCmdDistributor.Req req) {
      this.lastReq = req;
      if (req.synchronous) {
        closeStream();
        sendSyncReq(req);
        return;
      }

      UpdateRequest request = req.uReq;
      numRequests++;
      //TODO datcm serialize the request up-front and reuse it
      try {
        if (outStream == null) {
          outStream = client.initOutStream(basePath, request, request.getCollection());
        }
        client.send(outStream, request, request.getCollection());
      } catch (IOException e) {
        if (outStream != null) {
          closeStream();
        } else {
          handleError(e, req);
        }
      }
    }

    protected void handleError(Exception ex, SolrCmdDistributor.Req req) {
      log.error("Error when calling {} to {}", req, req.node.getUrl(), ex);
      Error error = new Error();
      error.e = ex;
      if (ex instanceof SolrException) {
        error.statusCode = ((SolrException) ex).code();
      }
      error.req = req;
      errors.add(error);
    }

    public void preFinish() {
      if (outStream != null) {
        try {
          client.closeStream(outStream);
        } catch (IOException e) {
          if (numRequests > 1) {
            handleError(new RuntimeException(), lastReq);
          } else {
            handleError(e, lastReq);
          }
        }
      }
    }

    public void finish() {
      closeStream();
    }

    private void sendSyncReq(SolrCmdDistributor.Req req) {
      try {
        NamedList<Object> rs = client.request(req.uReq);
        trackResult(rs);
      } catch (Exception e) {
        handleError(e, req);
      }
    }

    private void trackResult(NamedList<Object> rs) {
      lastReq.trackRequestResult(rs, true);
    }

    /**
     * Close previous stream and call error handling code
     */
    private void closeStream() {
      if (outStream != null) {
        try {
          NamedList<Object> rs = client.closeAndGetResponse(outStream);
          trackResult(rs);
        } catch (Exception e) {
          if (numRequests > 1) {
            handleError(new RuntimeException(), lastReq);
          } else {
            handleError(e, lastReq);
          }
        }
      }
      outStream = null;
    }
  }

  public synchronized void finishStreams() {
    //TODO nococmmit SOLR-13975
    for (SingleStreamClient client : clients.values()) {
      client.preFinish();
    }
    for (SingleStreamClient client : clients.values()) {
      client.closeStream();
    }
  }

  private String getFullUrl(String url) {
    String fullUrl;
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
      fullUrl = "http://" + url;
    } else {
      fullUrl = url;
    }
    return fullUrl;
  }

  public Http2SolrClient getHttpClient() {
    return httpClient;
  }

}
