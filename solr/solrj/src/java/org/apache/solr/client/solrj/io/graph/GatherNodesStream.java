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

package org.apache.solr.client.solrj.io.graph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

import static org.apache.solr.common.params.CommonParams.SORT;

public class GatherNodesStream extends TupleStream implements Expressible {

  private String zkHost;
  private String collection;
  private StreamContext streamContext;
  private Map<String,String> queryParams;
  private String traverseFrom;
  private String traverseTo;
  private String gather;
  private boolean trackTraversal;
  private boolean useDefaultTraversal;

  private TupleStream tupleStream;
  private Set<Traversal.Scatter> scatter;
  private Iterator<Tuple> out;
  private Traversal traversal;
  private List<Metric> metrics;
  private int maxDocFreq;

  public GatherNodesStream(String zkHost,
                           String collection,
                           TupleStream tupleStream,
                           String traverseFrom,
                           String traverseTo,
                           String gather,
                           Map queryParams,
                           List<Metric> metrics,
                           boolean trackTraversal,
                           Set<Traversal.Scatter> scatter,
                           int maxDocFreq) {

    init(zkHost,
        collection,
        tupleStream,
        traverseFrom,
        traverseTo,
        gather,
        queryParams,
        metrics,
        trackTraversal,
        scatter,
        maxDocFreq);
  }

  public GatherNodesStream(StreamExpression expression, StreamFactory factory) throws IOException {


    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    // Collection Name
    if(null == collectionName) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }


    Set<Traversal.Scatter> scatter = new HashSet();

    StreamExpressionNamedParameter scatterExpression = factory.getNamedOperand(expression, "scatter");

    if(scatterExpression == null) {
      scatter.add(Traversal.Scatter.LEAVES);
    } else {
      String s =  ((StreamExpressionValue)scatterExpression.getParameter()).getValue();
      String[] sArray = s.split(",");
      for(String sv : sArray) {
        sv = sv.trim();
        if(Traversal.Scatter.BRANCHES.toString().equalsIgnoreCase(sv)) {
          scatter.add(Traversal.Scatter.BRANCHES);
        } else if (Traversal.Scatter.LEAVES.toString().equalsIgnoreCase(sv)) {
          scatter.add(Traversal.Scatter.LEAVES);
        }
      }
    }

    String gather = null;
    StreamExpressionNamedParameter gatherExpression = factory.getNamedOperand(expression, "gather");

    if(gatherExpression == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - from param is required",expression));
    } else {
      gather = ((StreamExpressionValue)gatherExpression.getParameter()).getValue();
    }

    String traverseFrom = null;
    String traverseTo = null;
    StreamExpressionNamedParameter edgeExpression = factory.getNamedOperand(expression, "walk");

    TupleStream stream = null;

    if(edgeExpression == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - walk param is required", expression));
    } else {
      if(streamExpressions.size() > 0) {
        stream = factory.constructStream(streamExpressions.get(0));
        String edge = ((StreamExpressionValue) edgeExpression.getParameter()).getValue();
        String[] fields = edge.split("->");
        if (fields.length != 2) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - walk param separated by an -> and must contain two fields", expression));
        }
        traverseFrom = fields[0].trim();
        traverseTo = fields[1].trim();
      } else {
        String edge = ((StreamExpressionValue) edgeExpression.getParameter()).getValue();
        String[] fields = edge.split("->");
        if (fields.length != 2) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - walk param separated by an -> and must contain two fields", expression));
        }

        String[] rootNodes = fields[0].split(",");
        List<String> l = new ArrayList();
        for(String n : rootNodes) {
          l.add(n.trim());
        }

        stream = new NodeStream(l);
        traverseFrom = "node";
        traverseTo = fields[1].trim();
      }
    }

    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);
    List<Metric> metrics = new ArrayList();
    for(int idx = 0; idx < metricExpressions.size(); ++idx){
      metrics.add(factory.constructMetric(metricExpressions.get(idx)));
    }

    boolean trackTraversal = false;

    StreamExpressionNamedParameter trackExpression = factory.getNamedOperand(expression, "trackTraversal");

    if(trackExpression != null) {
      trackTraversal = Boolean.parseBoolean(((StreamExpressionValue) trackExpression.getParameter()).getValue());
    } else {
      useDefaultTraversal = true;
    }

    StreamExpressionNamedParameter docFreqExpression = factory.getNamedOperand(expression, "maxDocFreq");
    int docFreq = -1;

    if(docFreqExpression != null) {
      docFreq = Integer.parseInt(((StreamExpressionValue) docFreqExpression.getParameter()).getValue());
    }

    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") &&
          !namedParam.getName().equals("gather") &&
          !namedParam.getName().equals("walk") &&
          !namedParam.getName().equals("scatter") &&
          !namedParam.getName().equals("maxDocFreq") &&
          !namedParam.getName().equals("trackTraversal"))
      {
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }

    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }

    // We've got all the required items
    init(zkHost,
         collectionName,
         stream,
         traverseFrom,
         traverseTo ,
         gather,
         params,
         metrics,
         trackTraversal,
         scatter,
         docFreq);
  }

  private void init(String zkHost,
                    String collection,
                    TupleStream tupleStream,
                    String traverseFrom,
                    String traverseTo,
                    String gather,
                    Map queryParams,
                    List<Metric> metrics,
                    boolean trackTraversal,
                    Set<Traversal.Scatter> scatter,
                    int maxDocFreq) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.tupleStream = tupleStream;
    this.traverseFrom = traverseFrom;
    this.traverseTo = traverseTo;
    this.gather = gather;
    this.queryParams = queryParams;
    this.metrics = metrics;
    this.trackTraversal = trackTraversal;
    this.scatter = scatter;
    this.maxDocFreq = maxDocFreq;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {

    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);

    if(includeStreams && !(tupleStream instanceof NodeStream)){
      if(tupleStream instanceof Expressible){
        expression.addParameter(((Expressible)tupleStream).toExpression(factory));
      }
      else{
        throw new IOException("This GatherNodesStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }

    Set<Map.Entry<String,String>> entries =  queryParams.entrySet();
    // parameters
    for(Map.Entry param : entries){
      String value = param.getValue().toString();

      // SOLR-8409: This is a special case where the params contain a " character
      // Do note that in any other BASE streams with parameters where a " might come into play
      // that this same replacement needs to take place.
      value = value.replace("\"", "\\\"");

      expression.addParameter(new StreamExpressionNamedParameter(param.getKey().toString(), value));
    }

    if(metrics != null) {
      for (Metric metric : metrics) {
        expression.addParameter(metric.toExpression(factory));
      }
    }

    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    expression.addParameter(new StreamExpressionNamedParameter("gather", zkHost));
    if(maxDocFreq > -1) {
      expression.addParameter(new StreamExpressionNamedParameter("maxDocFreq", Integer.toString(maxDocFreq)));
    }
    if(tupleStream instanceof NodeStream) {
      NodeStream nodeStream = (NodeStream)tupleStream;
      expression.addParameter(new StreamExpressionNamedParameter("walk", nodeStream.toString() + "->" + traverseTo));

    } else {
      expression.addParameter(new StreamExpressionNamedParameter("walk", traverseFrom + "->" + traverseTo));
    }

    expression.addParameter(new StreamExpressionNamedParameter("trackTraversal", Boolean.toString(trackTraversal)));

    StringBuilder buf = new StringBuilder();
    for(Traversal.Scatter sc : scatter) {
      if(buf.length() > 0 ) {
        buf.append(",");
      }
      buf.append(sc.toString());
    }

    expression.addParameter(new StreamExpressionNamedParameter("scatter", buf.toString()));

    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.GRAPH_SOURCE);
    explanation.setExpression(toExpression(factory).toString());
    
    // one child is a stream
    explanation.addChild(tupleStream.toExplanation(factory));
    
    // one child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName("solr (graph)");
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);    
    child.setExpression(queryParams.entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));    
    explanation.addChild(child);
    
    if(null != metrics){
      for(Metric metric : metrics){
          explanation.addHelper(metric.toExplanation(factory));
      }
    }
    
    return explanation;
  }


  public void setStreamContext(StreamContext context) {
    this.traversal = (Traversal) context.get("traversal");
    if (traversal == null) {
      //No traversal in the context. So create a new context and a new traversal.
      //This ensures that two separate traversals in the same expression don't pollute each others traversal.
      StreamContext localContext = new StreamContext();

      localContext.numWorkers = context.numWorkers;
      localContext.workerID = context.workerID;
      localContext.setSolrClientCache(context.getSolrClientCache());
      localContext.setStreamFactory(context.getStreamFactory());

      for(Object key :context.getEntries().keySet()) {
        localContext.put(key, context.get(key));
      }

      traversal = new Traversal();

      localContext.put("traversal", traversal);

      this.tupleStream.setStreamContext(localContext);
      this.streamContext = localContext;
    } else {
      this.tupleStream.setStreamContext(context);
      this.streamContext = context;
    }
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  private class JoinRunner implements Callable<List<Tuple>> {

    private List<String> nodes;
    private List<Tuple> edges = new ArrayList();

    public JoinRunner(List<String> nodes) {
      this.nodes = nodes;
    }

    public List<Tuple> call() {


      Set<String> flSet = new HashSet();
      flSet.add(gather);
      flSet.add(traverseTo);

      //Add the metric columns

      if(metrics != null) {
        for(Metric metric : metrics) {
          for(String column : metric.getColumns()) {
            flSet.add(column);
          }
        }
      }

      if(queryParams.containsKey("fl")) {
        String flString = queryParams.get("fl");
        String[] flArray = flString.split(",");
        for(String f : flArray) {
          flSet.add(f.trim());
        }
      }

      Iterator<String> it = flSet.iterator();
      StringBuilder buf = new StringBuilder();
      while(it.hasNext()) {
        buf.append(it.next());
        if(it.hasNext()) {
          buf.append(",");
        }
      }
      
      ModifiableSolrParams joinSParams = new ModifiableSolrParams(SolrParams.toMultiMap(new NamedList(queryParams)));
      joinSParams.set("fl", buf.toString());
      joinSParams.set("qt", "/export");
      joinSParams.set(SORT, gather + " asc,"+traverseTo +" asc");

      StringBuffer nodeQuery = new StringBuffer();

      boolean comma = false;
      for(String node : nodes) {
        if(comma) {
          nodeQuery.append(",");
        }
        nodeQuery.append(node);
        comma = true;
      }

      if(maxDocFreq > -1) {
        String docFreqParam = " maxDocFreq="+maxDocFreq;
        joinSParams.set("q", "{!graphTerms f=" + traverseTo + docFreqParam + "}" + nodeQuery.toString());
      } else {
        joinSParams.set("q", "{!terms f=" + traverseTo+"}" + nodeQuery.toString());
      }

      TupleStream stream = null;
      try {
        stream = new UniqueStream(new CloudSolrStream(zkHost, collection, joinSParams), new MultipleFieldEqualitor(new FieldEqualitor(gather), new FieldEqualitor(traverseTo)));
        stream.setStreamContext(streamContext);
        stream.open();
        BATCH:
        while (true) {
          Tuple tuple = stream.read();
          if (tuple.EOF) {
            break BATCH;
          }

          edges.add(tuple);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        try {
          stream.close();
        } catch(Exception ce) {
          throw new RuntimeException(ce);
        }
      }
      return edges;
    }
  }


  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {

    if (out == null) {
      List<String> joinBatch = new ArrayList();
      List<Future<List<Tuple>>> futures = new ArrayList();
      Map<String, Node> level = new HashMap();

      ExecutorService threadPool = null;
      try {
        threadPool = ExecutorUtil.newMDCAwareFixedThreadPool(4, new SolrjNamedThreadFactory("GatherNodesStream"));

        Map<String, Node> roots = new HashMap();

        while (true) {
          Tuple tuple = tupleStream.read();
          if (tuple.EOF) {
            if (joinBatch.size() > 0) {
              JoinRunner joinRunner = new JoinRunner(joinBatch);
              Future future = threadPool.submit(joinRunner);
              futures.add(future);
            }
            break;
          }

          String value = tuple.getString(traverseFrom);

          if(traversal.getDepth() == 0) {
            //This gathers the root nodes
            //We check to see if there are dupes in the root nodes because root streams may not have been uniqued.
            String key = collection+"."+value;
            if(!roots.containsKey(key)) {
              Node node = new Node(value, trackTraversal);
              if (metrics != null) {
                List<Metric> _metrics = new ArrayList();
                for (Metric metric : metrics) {
                  _metrics.add(metric.newInstance());
                }
                node.setMetrics(_metrics);
              }

              roots.put(key, node);
            } else {
              continue;
            }
          }

          joinBatch.add(value);
          if (joinBatch.size() == 400) {
            JoinRunner joinRunner = new JoinRunner(joinBatch);
            Future future = threadPool.submit(joinRunner);
            futures.add(future);
            joinBatch = new ArrayList();
          }
        }

        if(traversal.getDepth() == 0) {
          traversal.addLevel(roots, collection, traverseFrom);
        }

        this.traversal.setScatter(scatter);

        if(useDefaultTraversal) {
          this.trackTraversal = traversal.getTrackTraversal();
        } else {
          this.traversal.setTrackTraversal(trackTraversal);
        }

        for (Future<List<Tuple>> future : futures) {
          List<Tuple> tuples = future.get();
          for (Tuple tuple : tuples) {
            String _traverseTo = tuple.getString(traverseTo);
            String _gather = tuple.getString(gather);
            String key = collection + "." + _gather;
            if (!traversal.visited(key, _traverseTo, tuple)) {
              Node node = level.get(key);
              if (node != null) {
                node.add((traversal.getDepth()-1)+"^"+_traverseTo, tuple);
              } else {
                node = new Node(_gather, trackTraversal);
                if (metrics != null) {
                  List<Metric> _metrics = new ArrayList();
                  for (Metric metric : metrics) {
                    _metrics.add(metric.newInstance());
                  }
                  node.setMetrics(_metrics);
                }
                node.add((traversal.getDepth()-1)+"^"+_traverseTo, tuple);
                level.put(key, node);
              }
            }
          }
        }

        traversal.addLevel(level, collection, gather);
        out = traversal.iterator();
      } catch(Exception e) {
        throw new RuntimeException(e);
      } finally {
        threadPool.shutdown();
      }
    }

    if (out.hasNext()) {
      return out.next();
    } else {
      Map map = new HashMap();
      map.put("EOF", true);
      Tuple tuple = new Tuple(map);
      return tuple;
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  static class NodeStream extends TupleStream {

    private List<String> ids;
    private Iterator<String> it;

    public NodeStream(List<String> ids) {
      this.ids = ids;
    }

    public void open() {this.it = ids.iterator();}
    public void close() {}
    public StreamComparator getStreamSort() {return null;}
    public List<TupleStream> children() {return new ArrayList();}
    public void setStreamContext(StreamContext context) {}

    public Tuple read() {
      HashMap map = new HashMap();
      if(it.hasNext()) {
        map.put("node",it.next());
        return new Tuple(map);
      } else {

        map.put("EOF", true);
        return new Tuple(map);
      }
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      boolean comma = false;
      for(String s : ids) {
        if(comma) {
          builder.append(",");
        }
        builder.append(s);
        comma = true;
      }
      return builder.toString();
    }
    
    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("non-expressible")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_SOURCE)
        .withExpression("non-expressible");
    }
  }
}