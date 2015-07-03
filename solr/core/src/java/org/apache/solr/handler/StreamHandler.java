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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.MergeStream;
import org.apache.solr.client.solrj.io.stream.ParallelStream;
import org.apache.solr.client.solrj.io.stream.RankStream;
import org.apache.solr.client.solrj.io.stream.ReducerStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UniqueStream;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamHandler extends RequestHandlerBase implements SolrCoreAware {

  static SolrClientCache clientCache = new SolrClientCache();
  private StreamFactory streamFactory = new StreamFactory();
  private Logger logger = LoggerFactory.getLogger(StreamHandler.class);

  public void inform(SolrCore core) {
    
    /* The stream factory will always contain the zkUrl for the given collection
     * Adds default streams with their corresponding function names. These 
     * defaults can be overridden or added to in the solrConfig in the stream 
     * RequestHandler def. Example config override
     *  <lst name="streamFunctions">
     *    <str name="group">org.apache.solr.client.solrj.io.stream.ReducerStream</str>
     *    <str name="count">org.apache.solr.client.solrj.io.stream.CountStream</str>
     *  </lst>
     * */

    String defaultCollection = null;
    String defaultZkhost     = null;
    CoreContainer coreContainer = core.getCoreDescriptor().getCoreContainer();

    if(coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreDescriptor().getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
    }

     streamFactory
      .withStreamFunction("search", CloudSolrStream.class)
      .withStreamFunction("merge", MergeStream.class)
      .withStreamFunction("unique", UniqueStream.class)
      .withStreamFunction("top", RankStream.class)
      .withStreamFunction("group", ReducerStream.class)
      .withStreamFunction("parallel", ParallelStream.class);

    
    // This pulls all the overrides and additions from the config
    Object functionMappingsObj = initArgs.get("streamFunctions");
    if(null != functionMappingsObj){
      NamedList<?> functionMappings = (NamedList<?>)functionMappingsObj;
      for(Entry<String,?> functionMapping : functionMappings){
        Class<?> clazz = core.getResourceLoader().findClass((String)functionMapping.getValue(), Expressible.class);
        streamFactory.withStreamFunction(functionMapping.getKey(), clazz);
      }
    }
        
    core.addCloseHook(new CloseHook() {
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

    boolean objectSerialize = params.getBool("objectSerialize", false);
    TupleStream tupleStream = null;

    try {
      if (objectSerialize) {
        String encodedStream = params.get("stream");
        encodedStream = URLDecoder.decode(encodedStream, "UTF-8");
        byte[] bytes = Base64.base64ToByteArray(encodedStream);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteStream);
        tupleStream = (TupleStream) objectInputStream.readObject();
      } else {
        tupleStream = this.streamFactory.constructStream(params.get("stream"));
      }
    } catch (Exception e) {
      //Catch exceptions that occur while the stream is being created. This will include streaming expression parse rules.
      logger.error("Exception creating TupleStream", e);
      rsp.add("tuples", new DummyErrorStream(e));

      return;
    }

    int worker = params.getInt("workerID", 0);
    int numWorkers = params.getInt("numWorkers", 1);
    StreamContext context = new StreamContext();
    context.workerID = worker;
    context.numWorkers = numWorkers;
    context.setSolrClientCache(clientCache);
    tupleStream.setStreamContext(context);
    rsp.add("tuples", new ExceptionStream(tupleStream));
  }

  public String getDescription() {
    return "StreamHandler";
  }

  public String getSource() {
    return null;
  }


  public static class DummyErrorStream extends TupleStream {
    private Exception e;

    public DummyErrorStream(Exception e) {
      this.e = e;
    }
    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() {
    }

    public void open() {
    }

    public void setStreamContext(StreamContext context) {
    }

    public List<TupleStream> children() {
      return null;
    }

    public Tuple read() {
      String msg = e.getMessage();
      Map m = new HashMap();
      m.put("EOF", true);
      m.put("_EXCEPTION_", msg);
      return new Tuple(m);
    }
  }
}
