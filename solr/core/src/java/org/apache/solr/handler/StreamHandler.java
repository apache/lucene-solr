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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.routing.RequestReplicaListTransformerGenerator;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StreamParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.pkg.PackagePluginHolder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * <p>
 * Solr Request Handler for streaming data.
 * </p>
 * <p>
 * It loads a Solr specific set of streaming expression functions via {@link org.apache.solr.handler.SolrDefaultStreamFactory}.
 * </p>
 * <p>
 * To add additional functions, just define them as plugins in solrconfig.xml via
 * {@code
 * &lt;expressible name="count" class="org.apache.solr.client.solrj.io.stream.RecordCountStream" /&gt;
 * }
 * </p>
 *
 * @since 5.1.0
 */
public class StreamHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {

  private ModelCache modelCache;
  @SuppressWarnings({"rawtypes"})
  private ConcurrentMap objectCache;
  private SolrDefaultStreamFactory streamFactory = new SolrDefaultStreamFactory();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String coreName;
  private SolrClientCache solrClientCache;
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map<String, DaemonStream> daemons = Collections.synchronizedMap(new HashMap());

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @SuppressWarnings({"rawtypes"})
  public void inform(SolrCore core) {
    String defaultCollection;
    String defaultZkhost;
    CoreContainer coreContainer = core.getCoreContainer();
    this.solrClientCache = coreContainer.getSolrClientCache();
    this.coreName = core.getName();
    String cacheKey = this.getClass().getName() + "_" + coreName + "_";
    this.objectCache = coreContainer.getObjectCache().computeIfAbsent(cacheKey + "objectCache",
        ConcurrentHashMap.class, k-> new ConcurrentHashMap());
    if (coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
      streamFactory.withDefaultZkHost(defaultZkhost);
      modelCache = coreContainer.getObjectCache().computeIfAbsent(cacheKey + "modelCache",
          ModelCache.class,
          k -> new ModelCache(250, defaultZkhost, solrClientCache));
    }
    streamFactory.withSolrResourceLoader(core.getResourceLoader());

    // This pulls all the overrides and additions from the config
    addExpressiblePlugins(streamFactory, core);
  }

  @SuppressWarnings({"unchecked"})
  public static void addExpressiblePlugins(StreamFactory streamFactory, SolrCore core) {
    List<PluginInfo> pluginInfos = core.getSolrConfig().getPluginInfos(Expressible.class.getName());
    for (PluginInfo pluginInfo : pluginInfos) {
      if (pluginInfo.pkgName != null) {
        @SuppressWarnings("resource")
        ExpressibleHolder holder = new ExpressibleHolder(pluginInfo, core, SolrConfig.classVsSolrPluginInfo.get(Expressible.class.getName()));
        streamFactory.withFunctionName(pluginInfo.name,
            () -> holder.getClazz());
      } else {
        Class<? extends Expressible> clazz = core.getMemClassLoader().findClass(pluginInfo.className, Expressible.class);
        streamFactory.withFunctionName(pluginInfo.name, clazz);
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  public static class ExpressibleHolder extends PackagePluginHolder {
    private Class clazz;

    public ExpressibleHolder(PluginInfo info, SolrCore core, SolrConfig.SolrPluginInfo pluginMeta) {
      super(info, core, pluginMeta);
    }

    @SuppressWarnings({"rawtypes"})
    public Class getClazz() {
      return clazz;
    }

    @Override
    protected Object initNewInstance(PackageLoader.Package.Version newest) {
      return clazz = newest.getLoader().findClass(pluginInfo.className, Expressible.class);
    }

  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    params = adjustParams(params);
    req.setParams(params);

    if (params.get("action") != null) {
      handleAdmin(req, rsp, params);
      return;
    }

    TupleStream tupleStream;

    try {
      StreamExpression streamExpression = StreamExpressionParser.parse(params.get(StreamParams.EXPR));
      if (this.streamFactory.isEvaluator(streamExpression)) {
        StreamExpression tupleExpression = new StreamExpression(StreamParams.TUPLE);
        tupleExpression.addParameter(new StreamExpressionNamedParameter(StreamParams.RETURN_VALUE, streamExpression));
        tupleStream = this.streamFactory.constructStream(tupleExpression);
      } else {
        tupleStream = this.streamFactory.constructStream(streamExpression);
      }
    } catch (Exception e) {
      // Catch exceptions that occur while the stream is being created. This will include streaming expression parse
      // rules.
      SolrException.log(log, e);
      rsp.add(StreamParams.RESULT_SET, new DummyErrorStream(e));

      return;
    }


    final SolrCore core = req.getCore(); // explicit check for null core (temporary?, for tests)
    @SuppressWarnings("resource")
    ZkController zkController = core == null ? null : core.getCoreContainer().getZkController();
    RequestReplicaListTransformerGenerator requestReplicaListTransformerGenerator;
    if (zkController != null) {
      requestReplicaListTransformerGenerator = new RequestReplicaListTransformerGenerator(
          zkController.getZkStateReader().getClusterProperties()
              .getOrDefault(ZkStateReader.DEFAULT_SHARD_PREFERENCES, "")
              .toString(),
          zkController.getNodeName(),
          zkController.getBaseUrl(),
          zkController.getSysPropsCacher()
      );
    } else {
      requestReplicaListTransformerGenerator = new RequestReplicaListTransformerGenerator();
    }

    int worker = params.getInt("workerID", 0);
    int numWorkers = params.getInt("numWorkers", 1);
    StreamContext context = new StreamContext();
    context.setRequestParams(params);
    context.setRequestReplicaListTransformerGenerator(requestReplicaListTransformerGenerator);
    context.put("shards", getCollectionShards(params));
    context.workerID = worker;
    context.numWorkers = numWorkers;
    context.setSolrClientCache(solrClientCache);
    context.setModelCache(modelCache);
    context.setObjectCache(objectCache);
    context.put("core", this.coreName);
    context.put("solr-core", req.getCore());
    tupleStream.setStreamContext(context);

    // if asking for explanation then go get it
    if (params.getBool("explain", false)) {
      rsp.add("explanation", tupleStream.toExplanation(this.streamFactory));
    }

    if (tupleStream instanceof DaemonStream) {
      DaemonStream daemonStream = (DaemonStream) tupleStream;
      if (daemons.containsKey(daemonStream.getId())) {
        daemons.remove(daemonStream.getId()).close();
      }
      daemonStream.setDaemons(daemons);
      daemonStream.open(); // This will start the daemonStream
      daemons.put(daemonStream.getId(), daemonStream);
      rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon:" + daemonStream.getId() + " started on " + coreName));
    } else {
      rsp.add(StreamParams.RESULT_SET, new TimerStream(new ExceptionStream(tupleStream)));
    }
  }

  private void handleAdmin(SolrQueryRequest req, SolrQueryResponse rsp, SolrParams params) {
    String action = params.get("action").toLowerCase(Locale.ROOT).trim();
    if ("plugins".equals(action)) {
      rsp.add("plugins", (MapWriter) ew -> streamFactory.getFunctionNames().forEach((s, classSupplier) -> ew.putNoEx(s, classSupplier.get().getName())));
      return;
    }

    if ("list".equals(action)) {
      Collection<DaemonStream> vals = daemons.values();
      rsp.add(StreamParams.RESULT_SET, new DaemonCollectionStream(vals));
      return;
    }

    String id = params.get(ID);
    DaemonStream d = daemons.get(id);
    if (d == null) {
      rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon:" + id + " not found on " + coreName));
      return;
    }

    switch (action) {
      case "stop":
        d.close();
        rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon:" + id + " stopped on " + coreName));
        break;

      case "start":
        try {
          d.open();
        } catch (IOException e) {
          rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon: " + id + " error: " + e.getMessage()));
        }
        rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon:" + id + " started on " + coreName));
        break;

      case "kill":
        daemons.remove(id);
        d.close(); // we already found it in the daemons list, so we don't need to verify we removed it.
        rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon:" + id + " killed on " + coreName));
        break;

      default:
        rsp.add(StreamParams.RESULT_SET, new DaemonResponseStream("Daemon:" + id + " action '"
            + action + "' not recognized on " + coreName));
        break;
    }
  }

  private SolrParams adjustParams(SolrParams params) {
    ModifiableSolrParams adjustedParams = new ModifiableSolrParams();
    adjustedParams.add(params);
    adjustedParams.add(CommonParams.OMIT_HEADER, "true");
    return adjustedParams;
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

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("error")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() {
      String msg = e.getMessage();

      Throwable t = e.getCause();
      while (t != null) {
        msg = t.getMessage();
        t = t.getCause();
      }
      return Tuple.EXCEPTION(msg, true);
    }
  }

  public static class DaemonCollectionStream extends TupleStream {
    private Iterator<DaemonStream> it;

    public DaemonCollectionStream(Collection<DaemonStream> col) {
      this.it = col.iterator();
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

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("daemon-collection")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() {
      if (it.hasNext()) {
        return it.next().getInfo();
      } else {
        return Tuple.EOF();
      }
    }
  }

  public static class DaemonResponseStream extends TupleStream {
    private String message;
    private boolean sendEOF = false;

    public DaemonResponseStream(String message) {
      this.message = message;
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

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("daemon-response")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() {
      if (sendEOF) {
        return Tuple.EOF();
      } else {
        sendEOF = true;
        return new Tuple("DaemonOp", message);
      }
    }
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

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("timer")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    @SuppressWarnings({"unchecked"})
    public Tuple read() throws IOException {
      Tuple tuple = this.tupleStream.read();
      if (tuple.EOF) {
        long totalTime = (System.nanoTime() - begin) / 1000000;
        tuple.put(StreamParams.RESPONSE_TIME, totalTime);
      }
      return tuple;
    }
  }

  private Map<String, List<String>> getCollectionShards(SolrParams params) {

    Map<String, List<String>> collectionShards = new HashMap<>();
    Iterator<String> paramsIt = params.getParameterNamesIterator();
    while (paramsIt.hasNext()) {
      String param = paramsIt.next();
      if (param.indexOf(".shards") > -1) {
        String collection = param.split("\\.")[0];
        String shardString = params.get(param);
        String[] shards = shardString.split(",");
        @SuppressWarnings({"rawtypes"})
        List<String> shardList = new ArrayList<>();
        for (String shard : shards) {
          shardList.add(shard);
        }
        collectionShards.put(collection, shardList);
      }
    }

    if (collectionShards.size() > 0) {
      return collectionShards;
    } else {
      return null;
    }
  }
}
