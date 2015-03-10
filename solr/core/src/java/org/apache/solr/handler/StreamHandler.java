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

package org.apache.solr.handler;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.TupleStream;
import org.apache.solr.client.solrj.io.StreamContext;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.common.util.Base64;


public class StreamHandler extends RequestHandlerBase implements SolrCoreAware {

  private SolrClientCache clientCache = new SolrClientCache();

  public void inform(SolrCore core) {

    core.addCloseHook( new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void postClose(SolrCore core) {
        clientCache.close();
      }
    });
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    String encodedStream = params.get("stream");
    encodedStream = URLDecoder.decode(encodedStream, "UTF-8");
    byte[] bytes = Base64.base64ToByteArray(encodedStream);
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    ObjectInputStream objectInputStream = new ObjectInputStream(byteStream);
    TupleStream tupleStream = (TupleStream)objectInputStream.readObject();

    int worker = params.getInt("workerID");
    int numWorkers = params.getInt("numWorkers");
    StreamContext context = new StreamContext();
    context.workerID = worker;
    context.numWorkers = numWorkers;
    context.clientCache = clientCache;
    tupleStream.setStreamContext(context);
    rsp.add("tuples", tupleStream);
  }

  public String getDescription() {
    return "StreamHandler";
  }

  public String getSource() {
    return null;
  }
}