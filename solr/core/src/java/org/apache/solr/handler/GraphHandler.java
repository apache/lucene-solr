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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.graph.GatherNodesStream;
import org.apache.solr.client.solrj.io.graph.ShortestPathStream;
import org.apache.solr.client.solrj.io.graph.Traversal;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {

  private StreamFactory streamFactory = new StreamFactory();
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String coreName;

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
    }

    streamFactory
        // streams
        .withFunctionName("search", CloudSolrStream.class)
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
        .withFunctionName("facet", FacetStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("jdbc", JDBCStream.class)
        .withFunctionName("intersect", IntersectStream.class)
        .withFunctionName("select", SelectStream.class)
        .withFunctionName("complement", ComplementStream.class)
        .withFunctionName("daemon", DaemonStream.class)
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("shortestPath", ShortestPathStream.class)
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("sort", SortStream.class)
            .withFunctionName("scoreNodes", ScoreNodesStream.class)

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
    Object functionMappingsObj = initArgs.get("streamFunctions");
    if(null != functionMappingsObj){
      NamedList<?> functionMappings = (NamedList<?>)functionMappingsObj;
      for(Entry<String,?> functionMapping : functionMappings){
        Class<? extends Expressible> clazz = core.getResourceLoader().findClass((String)functionMapping.getValue(),
            Expressible.class);
        streamFactory.withFunctionName(functionMapping.getKey(), clazz);
      }
    }
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    params = adjustParams(params);
    req.setParams(params);


    TupleStream tupleStream = null;

    try {
      tupleStream = this.streamFactory.constructStream(params.get("expr"));
    } catch (Exception e) {
      //Catch exceptions that occur while the stream is being created. This will include streaming expression parse rules.
      SolrException.log(logger, e);
      Map requestContext = req.getContext();
      requestContext.put("stream", new DummyErrorStream(e));
      return;
    }

    StreamContext context = new StreamContext();
    context.setSolrClientCache(StreamHandler.clientCache);
    context.put("core", this.coreName);
    Traversal traversal = new Traversal();
    context.put("traversal", traversal);
    tupleStream.setStreamContext(context);
    Map requestContext = req.getContext();
    requestContext.put("stream", new TimerStream(new ExceptionStream(tupleStream)));
    requestContext.put("traversal", traversal);
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
      String msg = e.getMessage();
      Map m = new HashMap();
      m.put("EOF", true);
      m.put("EXCEPTION", msg);
      return new Tuple(m);
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
