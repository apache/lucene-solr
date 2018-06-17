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
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.util.SolrInternalHttpClient;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements {@link SolrCloudManager} using a SolrClient
 */
public class SolrClientCloudManager implements SolrCloudManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CloudSolrClient solrClient;
  private final ZkDistribStateManager stateManager;
  private final DistributedQueueFactory queueFactory;
  private final ZkStateReader zkStateReader;
  private final SolrZkClient zkClient;
  private final ObjectCache objectCache;
  private volatile boolean isClosed;

  public SolrClientCloudManager(DistributedQueueFactory queueFactory, CloudSolrClient solrClient) {
    this.queueFactory = queueFactory;
    this.solrClient = solrClient;
    this.zkStateReader = solrClient.getZkStateReader();
    this.zkClient = zkStateReader.getZkClient();
    this.stateManager = new ZkDistribStateManager(zkClient);
    this.isClosed = false;
    this.objectCache = new ObjectCache();
  }

  @Override
  public void close() {
    isClosed = true;
    IOUtils.closeQuietly(objectCache);
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return TimeSource.NANO_TIME;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return solrClient.getClusterStateProvider();
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return new SolrClientNodeStateProvider(solrClient);
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return stateManager;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    try {
      return req.process(solrClient);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  private static final byte[] EMPTY = new byte[0];

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    // nocommit
    SolrInternalHttpClient httpClient = solrClient.getHttpClient();
    try (Http2SolrClient client = new Http2SolrClient.Builder(url).withHttpClient(httpClient).build()) {
      final HttpRequestBase req;
      HttpEntity entity = null;
      if (payload != null) {
        entity = new StringEntity(payload, "UTF-8");
      }
      switch (method) {
        case GET:
          return client.httpGet(url).asString.getBytes("UTF-8");
        case POST:
          return client.httpPost(url, payload.getBytes("UTF-8"), "text/html; charset=UTF-8").asString.getBytes("UTF-8");
          //req = new HttpPost(url);
//          if (entity != null) {
//            ((HttpPost) req).setEntity(entity);
//          }
//          break;
        case PUT:
          return client.httpPut(url, payload.getBytes("UTF-8"), "text/html; charset=UTF-8").getBytes("UTF-8");
//          req = new HttpPut(url);
//          if (entity != null) {
//            ((HttpPut) req).setEntity(entity);
//          }
//          break;
        case DELETE:
          return client.httpDelete(url).getBytes();
          //req = new HttpDelete(url);
          //break;
        default:
          throw new IOException("Unsupported method " + method);
      }
//      if (headers != null) {
//        headers.forEach((k, v) -> req.addHeader(k, v));
//      }
//      RequestConfig.Builder requestConfigBuilder = HttpClientUtil.createDefaultRequestConfigBuilder();
//      if (timeout > 0) {
//        requestConfigBuilder.setSocketTimeout(timeout);
//        requestConfigBuilder.setConnectTimeout(timeout);
//      }
//      requestConfigBuilder.setRedirectsEnabled(followRedirects);
//      req.setConfig(requestConfigBuilder.build());
//      HttpClientContext httpClientRequestContext = HttpClientUtil.createNewHttpClientRequestContext();
//     // HttpResponse rsp = client.execute(req, httpClientRequestContext);
//      int statusCode = rsp.getStatusLine().getStatusCode();
//      if (statusCode != 200) {
//        throw new IOException("Error sending request to " + url + ", HTTP response: " + rsp.toString());
//      }
//      HttpEntity responseEntity = rsp.getEntity();
//      if (responseEntity != null && responseEntity.getContent() != null) {
//        return EntityUtils.toByteArray(responseEntity);
//      } else {
//        return EMPTY;
//      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    // nocommit exceptions
    catch (SolrServerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return queueFactory;
  }

}
