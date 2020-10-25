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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

public class MockingHttp2SolrClient extends Http2SolrClient {

  public enum Exp {CONNECT_EXCEPTION, SOCKET_EXCEPTION, BAD_REQUEST};

  private volatile Exp exp = null;
  private boolean oneExpPerReq;
  @SuppressWarnings({"rawtypes"})
  private Set<SolrRequest> reqGotException;

  public MockingHttp2SolrClient(String baseSolrUrl, Builder builder) {
    super(baseSolrUrl, builder);
    this.oneExpPerReq = builder.oneExpPerReq;
    this.reqGotException = new HashSet<>();
  }

  public static class Builder extends Http2SolrClient.Builder {
    private boolean oneExpPerReq = false;

    public Builder(UpdateShardHandlerConfig config) {
      super();
      this.connectionTimeout(config.getDistributedConnectionTimeout());
      this.idleTimeout(config.getDistributedSocketTimeout());
    }

    public MockingHttp2SolrClient build() {
      return new MockingHttp2SolrClient(null, this);
    }

    // DBQ won't cause exception
    Builder oneExpPerReq() {
      oneExpPerReq = true;
      return this;
    }
  }


  public void setExp(Exp exp) {
    this.exp = exp;
  }

  private Exception exception() {
    switch (exp) {
      case CONNECT_EXCEPTION:
        return new ConnectException();
      case SOCKET_EXCEPTION:
        return new SocketException();
      case BAD_REQUEST:
        return new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bad Request");
      default:
        break;
    }
    return null;
  }

  @Override
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"})SolrRequest request,
                                   String collection)
      throws SolrServerException, IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest ur = (UpdateRequest) request;
      if (!ur.getDeleteQuery().isEmpty())
        return super.request(request, collection);
    }

    if (exp != null) {
      if (oneExpPerReq) {
        if (reqGotException.contains(request))
          return super.request(request, collection);
        else
          reqGotException.add(request);
      }

      Exception e = exception();
      if (e instanceof IOException) {
        if (LuceneTestCase.random().nextBoolean()) {
          throw (IOException) e;
        } else {
          throw new SolrServerException(e);
        }
      } else if (e instanceof SolrServerException) {
        throw (SolrServerException) e;
      } else {
        throw new SolrServerException(e);
      }
    }

    return super.request(request, collection);
  }

  public NamedList<Object> request(@SuppressWarnings({"rawtypes"})SolrRequest request,
                                   String collection, OnComplete onComplete)
      throws SolrServerException, IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest ur = (UpdateRequest) request;
      // won't throw exception if request is DBQ
      if (ur.getDeleteQuery() != null && !ur.getDeleteQuery().isEmpty()) {
        return super.request(request, collection, onComplete);
      }
    }

    if (exp != null) {
      if (oneExpPerReq) {
        if (reqGotException.contains(request)) {
          return super.request(request, collection, onComplete);
        }
        else
          reqGotException.add(request);
      }

      Exception e = exception();
      if (e instanceof IOException) {
        if (LuceneTestCase.random().nextBoolean()) {
          throw (IOException) e;
        } else {
          throw new SolrServerException(e);
        }
      } else if (e instanceof SolrServerException) {
        throw (SolrServerException) e;
      } else {
        throw new SolrServerException(e);
      }
    }

    return super.request(request, collection, onComplete);
  }
}
