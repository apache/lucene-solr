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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

public class MockStreamingSolrClients extends StreamingSolrClients {
  
  public enum Exp {CONNECT_EXCEPTION, SOCKET_EXCEPTION, BAD_REQUEST};
  
  private volatile Exp exp = null;
  
  public MockStreamingSolrClients(UpdateShardHandler updateShardHandler) {
    super(updateShardHandler);
  }
  
  @Override
  public synchronized SolrClient getSolrClient(final SolrCmdDistributor.Req req) {
    SolrClient client = super.getSolrClient(req);
    return new MockSolrClient(client);
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

  class MockSolrClient extends SolrClient {

    private SolrClient solrClient;

    public MockSolrClient(SolrClient solrClient) {
      this.solrClient = solrClient;
    }
    
    @Override
    public NamedList<Object> request(@SuppressWarnings({"rawtypes"})SolrRequest request, String collection)
        throws SolrServerException, IOException {
      if (exp != null) {
        Exception e = exception();
        if (e instanceof IOException) {
          if (LuceneTestCase.random().nextBoolean()) {
            throw (IOException)e;
          } else {
            throw new SolrServerException(e);
          }
        } else if (e instanceof SolrServerException) {
          throw (SolrServerException)e;
        } else {
          throw new SolrServerException(e);
        }
      }
      
      return solrClient.request(request);
    }

    @Override
    public void close() {}
    
  }
}
