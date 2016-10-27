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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.graph.GatherNodesStream;
import org.apache.solr.client.solrj.io.graph.ShortestPathStream;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {

  static SolrClientCache clientCache = new SolrClientCache();
  static ModelCache modelCache = null;
  private StreamFactory streamFactory = new StreamFactory();
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String coreName;
  private Map<String, DaemonStream> daemons = Collections.synchronizedMap(new HashMap());

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  public void inform(SolrCore core) {
    
    /* The stream factory will always contain the zkUrl for the given collection
     * Adds default streams with their corresponding function names. These 
     * defaults can be overridden or added to in the solrConfig in the stream 
     * RequestHandler def. Example config override
     *  <lst name="streamFunctions">
     *    <str name="group">org.apache.solr.client.solrj.io.stream.ReducerStream</str>
     *    <str name="count">org.apache.solr.client.solrj.io.stream.RecordCountStream</str>
     *  </lst>
     * */

    String defaultCollection;
    String defaultZkhost;
    CoreContainer coreContainer = core.getCoreDescriptor().getCoreContainer();
    this.coreName = core.getName();

    if(coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreDescriptor().getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
      streamFactory.withDefaultZkHost(defaultZkhost);
      modelCache = new ModelCache(250,
                                  defaultZkhost,
                                  clientCache);
    }

     streamFactory
       // source streams
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("facet", FacetStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("jdbc", JDBCStream.class)
      .withFunctionName("topic", TopicStream.class)
      .withFunctionName("commit", CommitStream.class)
      
      // decorator streams
      .withFunctionName("merge", MergeStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("top", RankStream.class)
      .withFunctionName("group", GroupOperation.class)
      .withFunctionName("reduce", ReducerStream.class)
      .withFunctionName("parallel", ParallelStream.class)
      .withFunctionName("rollup", RollupStream.class)
      .withFunctionName("stats", StatsStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class)
      .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class) 
      .withFunctionName("hashJoin", HashJoinStream.class)
      .withFunctionName("outerHashJoin", OuterHashJoinStream.class)
      .withFunctionName("intersect", IntersectStream.class)
      .withFunctionName("complement", ComplementStream.class)
      .withFunctionName("sort", SortStream.class)
      .withFunctionName("train", TextLogitStream.class)
      .withFunctionName("features", FeaturesSelectionStream.class)
      .withFunctionName("daemon", DaemonStream.class)
      .withFunctionName("shortestPath", ShortestPathStream.class)
      .withFunctionName("gatherNodes", GatherNodesStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("scoreNodes", ScoreNodesStream.class)
      .withFunctionName("model", ModelStream.class)
      .withFunctionName("classify", ClassifyStream.class)
      .withFunctionName("fetch", FetchStream.class)
      .withFunctionName("executor", ExecutorStream.class)

      // metrics
      .withFunctionName("min", MinMetric.class)
      .withFunctionName("max", MaxMetric.class)
      .withFunctionName("avg", MeanMetric.class)
      .withFunctionName("sum", SumMetric.class)
      .withFunctionName("count", CountMetric.class)
      
      // tuple manipulation operations
      .withFunctionName("replace", ReplaceOperation.class)
      .withFunctionName("concat", ConcatOperation.class)
      
      // stream reduction operations
      .withFunctionName("group", GroupOperation.class)
      .withFunctionName("distinct", DistinctOperation.class);

     // This pulls all the overrides and additions from the config
     List<PluginInfo> pluginInfos = core.getSolrConfig().getPluginInfos(Expressible.class.getName());
     for (PluginInfo pluginInfo : pluginInfos) {
       Class<? extends Expressible> clazz = core.getResourceLoader().findClass(pluginInfo.className, Expressible.class);
       streamFactory.withFunctionName(pluginInfo.name, clazz);
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
    params = adjustParams(params);
    req.setParams(params);

    if(params.get("action") != null) {
      handleAdmin(req, rsp, params);
      return;
    }

    TupleStream tupleStream;

    try {
      tupleStream = this.streamFactory.constructStream(params.get("expr"));
    } catch (Exception e) {
      //Catch exceptions that occur while the stream is being created. This will include streaming expression parse rules.
      SolrException.log(logger, e);
      rsp.add("result-set", new DummyErrorStream(e));

      return;
    }

    int worker = params.getInt("workerID", 0);
    int numWorkers = params.getInt("numWorkers", 1);
    StreamContext context = new StreamContext();
    context.workerID = worker;
    context.numWorkers = numWorkers;
    context.setSolrClientCache(clientCache);
    context.setModelCache(modelCache);
    context.put("core", this.coreName);
    context.put("solr-core", req.getCore());
    tupleStream.setStreamContext(context);
    
    // if asking for explanation then go get it
    if(params.getBool("explain", false)){
      rsp.add("explanation", tupleStream.toExplanation(this.streamFactory));
    }
    
    if(tupleStream instanceof DaemonStream) {
      DaemonStream daemonStream = (DaemonStream)tupleStream;
      if(daemons.containsKey(daemonStream.getId())) {
        daemons.remove(daemonStream.getId()).close();
      }
      daemonStream.setDaemons(daemons);
      daemonStream.open();  //This will start the deamonStream
      daemons.put(daemonStream.getId(), daemonStream);
      rsp.add("result-set", new DaemonResponseStream("Deamon:"+daemonStream.getId()+" started on "+coreName));
    } else {
      rsp.add("result-set", new TimerStream(new ExceptionStream(tupleStream)));
    }
  }

  private void handleAdmin(SolrQueryRequest req, SolrQueryResponse rsp, SolrParams params) {
    String action = params.get("action");
    if("stop".equalsIgnoreCase(action)) {
      String id = params.get("id");
      DaemonStream d = daemons.get(id);
      if(d != null) {
        d.close();
        rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " stopped on " + coreName));
      } else {
        rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " not found on " + coreName));
      }
    } else if("start".equalsIgnoreCase(action)) {
      String id = params.get("id");
      DaemonStream d = daemons.get(id);
      d.open();
      rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " started on " + coreName));
    } else if("list".equalsIgnoreCase(action)) {
      Collection<DaemonStream> vals = daemons.values();
      rsp.add("result-set", new DaemonCollectionStream(vals));
    } else if("kill".equalsIgnoreCase(action)) {
      String id = params.get("id");
      DaemonStream d = daemons.remove(id);
      if (d != null) {
        d.close();
      }
      rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " killed on " + coreName));
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
      while(t != null) {
        msg = t.getMessage();
        t = t.getCause();
      }


      Map m = new HashMap();
      m.put("EOF", true);
      m.put("EXCEPTION", msg);
      return new Tuple(m);
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
      if(it.hasNext()) {
        return it.next().getInfo();
      } else {
        Map m = new HashMap();
        m.put("EOF", true);
        return new Tuple(m);
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
        Map m = new HashMap();
        m.put("EOF", true);
        return new Tuple(m);
      } else {
        sendEOF = true;
        Map m = new HashMap();
        m.put("DaemonOp",message);
        return new Tuple(m);
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

    public Tuple read() throws IOException {
      Tuple tuple = this.tupleStream.read();
      if(tuple.EOF) {
        long totalTime = (System.nanoTime() - begin) / 1000000;
        tuple.fields.put("RESPONSE_TIME", totalTime);
      }
      return tuple;
    }
  }
}
