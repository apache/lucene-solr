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
import java.net.ConnectException;
import java.net.SocketException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.util.NamedList;

public class MockingHttp2SolrClient extends Http2SolrClient {

  public enum Exp {CONNECT_EXCEPTION, SOCKET_EXCEPTION};

  private volatile Exp exp = null;

  public MockingHttp2SolrClient(String baseSolrUrl, Builder builder) {
    super(baseSolrUrl, builder);
  }

  public static class Builder extends Http2SolrClient.Builder {

    public Builder(String baseSolrUrl, UpdateShardHandlerConfig config) {
      super(baseSolrUrl);
      this.connectionTimeout(config.getDistributedConnectionTimeout());
      this.idleTimeout(config.getDistributedSocketTimeout());
    }

    public MockingHttp2SolrClient build() {
      return new MockingHttp2SolrClient(baseSolrUrl, this);
    }
  }


  public void setExp(Exp exp) {
    this.exp = exp;
  }

  private IOException exception() {
    switch (exp) {
      case CONNECT_EXCEPTION:
        return new ConnectException();
      case SOCKET_EXCEPTION:
        return new SocketException();
      default:
        break;
    }
    return null;
  }

  @Override
  public NamedList<Object> request(SolrRequest request, String collection)
      throws SolrServerException, IOException {
    if (exp != null) {
      if (LuceneTestCase.random().nextBoolean()) {
        throw exception();
      } else {
        throw new SolrServerException(exception());
      }
    }

    return super.request(request);
  }

  public Http2ClientResponse request(SolrRequest solrRequest, String collection, OnComplete onComplete)
      throws SolrServerException, IOException {
    if (exp != null) {
      if (LuceneTestCase.random().nextBoolean()) {
        throw exception();
      } else {
        throw new SolrServerException(exception());
      }
    }
    return super.request(solrRequest, collection, onComplete);
  }
}
