package org.apache.solr.update;

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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockStreamingSolrServers extends StreamingSolrServers {
  public static Logger log = LoggerFactory
      .getLogger(MockStreamingSolrServers.class);
  
  public enum Exp {CONNECT_EXCEPTION, SOCKET_EXCEPTION};
  
  private volatile Exp exp = null;
  
  public MockStreamingSolrServers(UpdateShardHandler updateShardHandler) {
    super(updateShardHandler);
  }
  
  @Override
  public synchronized SolrServer getSolrServer(final SolrCmdDistributor.Req req) {
    SolrServer server = super.getSolrServer(req);
    return new MockSolrServer(server);
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

  class MockSolrServer extends SolrServer {

    private SolrServer solrServer;

    public MockSolrServer(SolrServer solrServer) {
      this.solrServer = solrServer;
    }
    
    @Override
    public NamedList<Object> request(SolrRequest request)
        throws SolrServerException, IOException {
      if (exp != null) {
        throw exception();
      }
      
      return solrServer.request(request);
    }


    @Override
    public void shutdown() {}
    
  }
}
