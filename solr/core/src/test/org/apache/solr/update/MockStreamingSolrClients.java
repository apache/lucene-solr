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
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

public class MockStreamingSolrClients extends StreamingSolrClients {
  
  public enum Exp {CONNECT_EXCEPTION, SOCKET_EXCEPTION, BAD_REQUEST};
  
  private volatile Exp exp = null;
  
  public MockStreamingSolrClients(UpdateShardHandler updateShardHandler) {
    super(updateShardHandler);
  }

  @Override
  public synchronized SingleStreamClient getClient(SolrCmdDistributor.Req req) {
    SingleStreamClient client = super.getClient(req);
    return new MockClient(client);
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

  class MockClient extends SingleStreamClient {
    SingleStreamClient client;

    public MockClient(SingleStreamClient client) {
      super(null, null);
      this.client = client;
    }

    @Override
    public void request(SolrCmdDistributor.Req req) {
      if (exp != null) {
        Exception e = exception();
        if (e instanceof IOException) {
          if (LuceneTestCase.random().nextBoolean()) {
            this.client.handleError(e, req);
            return;
          } else {
            this.client.handleError(e, req);
            return;
          }
        } else if (e instanceof SolrServerException) {
          this.client.handleError(e, req);
          return;
        } else {
          this.client.handleError(e, req);
          return;
        }
      }
      this.client.request(req);
    }

    @Override
    public void finish() {
      this.client.finish();
    }
  }

}
