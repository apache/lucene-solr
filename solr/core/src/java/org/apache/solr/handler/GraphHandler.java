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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.graph.Traversal;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.DefaultStreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StreamParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Solr Request Handler for graph traversal with streaming functions that responds with GraphML markup.
 * </p>
 * <p>
 * It loads the default set of streaming expression functions via {@link org.apache.solr.client.solrj.io.stream.expr.DefaultStreamFactory}.
 * </p>
 * <p>
 * To add additional functions, just define them as plugins in solrconfig.xml via
 * {@code
 * &lt;expressible name="count" class="org.apache.solr.client.solrj.io.stream.RecordCountStream" /&gt;
 * }
 * </p>
 * <p>
 * The @deprecated configuration method as of Solr 8.5 is
  * {@code
 *  &lt;lst name="streamFunctions"&gt;
 *    &lt;str name="group"&gt;org.apache.solr.client.solrj.io.stream.ReducerStream&lt;/str&gt;
 *    &lt;str name="count"&gt;org.apache.solr.client.solrj.io.stream.RecordCountStream&lt;/str&gt;
 *  &lt;/lst&gt;
  * }
 *</p>
 *
 * @since 6.1.0
 */
public class GraphHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {

  private StreamFactory streamFactory = new DefaultStreamFactory();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String coreName;
  private SolrClientCache solrClientCache;

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @SuppressWarnings({"unchecked"})
  public void inform(SolrCore core) {
    String defaultCollection;
    String defaultZkhost;
    CoreContainer coreContainer = core.getCoreContainer();
    this.coreName = core.getName();
    this.solrClientCache = coreContainer.getSolrClientCache();

    if(coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
      streamFactory.withDefaultZkHost(defaultZkhost);
    }

    // This pulls all the overrides and additions from the config
    StreamHandler.addExpressiblePlugins(streamFactory, core);

    // Check deprecated approach.
    Object functionMappingsObj = initArgs.get("streamFunctions");
    if(null != functionMappingsObj){
      log.warn("solrconfig.xml: <streamFunctions> is deprecated for adding additional streaming functions to GraphHandler.");
      NamedList<?> functionMappings = (NamedList<?>)functionMappingsObj;
      for(Entry<String,?> functionMapping : functionMappings) {
        String key = functionMapping.getKey();
        PluginInfo pluginInfo = new PluginInfo(key, Collections.singletonMap("class", functionMapping.getValue()));

        if (pluginInfo.pkgName == null) {
          Class<? extends Expressible> clazz = core.getResourceLoader().findClass((String) functionMapping.getValue(),
              Expressible.class);
          streamFactory.withFunctionName(key, clazz);
        } else {
          @SuppressWarnings("resource")
          StreamHandler.ExpressibleHolder holder = new StreamHandler.ExpressibleHolder(pluginInfo, core, SolrConfig.classVsSolrPluginInfo.get(Expressible.class.getName()));
          streamFactory.withFunctionName(key, () -> holder.getClazz());
        }

      }

    }
  }

  @SuppressWarnings({"unchecked"})
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    params = adjustParams(params);
    req.setParams(params);


    TupleStream tupleStream = null;

    try {
      tupleStream = this.streamFactory.constructStream(params.get("expr"));
    } catch (Exception e) {
      //Catch exceptions that occur while the stream is being created. This will include streaming expression parse rules.
      SolrException.log(log, e);
      @SuppressWarnings({"rawtypes"})
      Map requestContext = req.getContext();
      requestContext.put("stream", new DummyErrorStream(e));
      return;
    }

    StreamContext context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    context.put("core", this.coreName);
    Traversal traversal = new Traversal();
    context.put("traversal", traversal);
    tupleStream.setStreamContext(context);
    @SuppressWarnings({"rawtypes"})
    Map requestContext = req.getContext();
    requestContext.put("stream", new TimerStream(new ExceptionStream(tupleStream)));
    requestContext.put("traversal", traversal);
  }

  public String getDescription() {
    return "GraphHandler";
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

    public Exception getException() {
      return this.e;
    }

    public void setStreamContext(StreamContext context) {
    }

    public List<TupleStream> children() {
      return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
      return null;
    }

    public Tuple read() {
      return Tuple.EXCEPTION(e.getMessage(), true);
    }
  }


  private SolrParams adjustParams(SolrParams params) {
    ModifiableSolrParams adjustedParams = new ModifiableSolrParams();
    adjustedParams.add(params);
    adjustedParams.add(CommonParams.OMIT_HEADER, "true");
    return adjustedParams;
  }

  public static class TimerStream extends TupleStream {

    private long begin;
    private TupleStream tupleStream;

    public TimerStream(TupleStream tupleStream) {
      this.tupleStream = tupleStream;
    }

    public StreamComparator getStreamSort() {
      return this.tupleStream.getStreamSort();
    }

    public void close() throws IOException {
      this.tupleStream.close();
    }

    public void open() throws IOException {
      this.begin = System.nanoTime();
      this.tupleStream.open();
    }

    public void setStreamContext(StreamContext context) {
      this.tupleStream.setStreamContext(context);
    }

    public List<TupleStream> children() {
      return this.tupleStream.children();
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
      return null;
    }

    @SuppressWarnings({"unchecked"})
    public Tuple read() throws IOException {
      Tuple tuple = this.tupleStream.read();
      if(tuple.EOF) {
        long totalTime = (System.nanoTime() - begin) / 1000000;
        tuple.put(StreamParams.RESPONSE_TIME, totalTime);
      }
      return tuple;
    }
  }

}
