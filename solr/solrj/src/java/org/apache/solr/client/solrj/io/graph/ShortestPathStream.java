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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * @since 6.1.0
 */
public class ShortestPathStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private String fromNode;
  private String toNode;
  private String fromField;
  private String toField;
  private int joinBatchSize;
  private int maxDepth;
  private String zkHost;
  private String collection;
  private LinkedList<Tuple> shortestPaths = new LinkedList();
  private boolean found;
  private StreamContext streamContext;
  private int threads;
  private SolrParams queryParams;

  public ShortestPathStream(String zkHost,
                            String collection,
                            String fromNode,
                            String toNode,
                            String fromField,
                            String toField,
                            SolrParams queryParams,
                            int joinBatchSize,
                            int threads,
                            int maxDepth) {

    init(zkHost,
        collection,
        fromNode,
        toNode,
        fromField,
        toField,
        queryParams,
        joinBatchSize,
        threads,
        maxDepth);
  }

  public ShortestPathStream(StreamExpression expression, StreamFactory factory) throws IOException {

    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    String fromNode = null;
    StreamExpressionNamedParameter fromExpression = factory.getNamedOperand(expression, "from");

    if(fromExpression == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - from param is required",expression));
    } else {
      fromNode = ((StreamExpressionValue)fromExpression.getParameter()).getValue();
    }

    String toNode = null;
    StreamExpressionNamedParameter toExpression = factory.getNamedOperand(expression, "to");

    if(toExpression == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - to param is required", expression));
    } else {
      toNode = ((StreamExpressionValue)toExpression.getParameter()).getValue();
    }

    String fromField = null;
    String toField = null;

    StreamExpressionNamedParameter edgeExpression = factory.getNamedOperand(expression, "edge");

    if(edgeExpression == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - edge param is required", expression));
    } else {
      String edge = ((StreamExpressionValue)edgeExpression.getParameter()).getValue();
      String[] fields = edge.split("=");
      if(fields.length != 2) {
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - edge param separated by and = and must contain two fields", expression));
      }
      fromField = fields[0].trim();
      toField = fields[1].trim();
    }

    int threads = 6;

    StreamExpressionNamedParameter threadsExpression = factory.getNamedOperand(expression, "threads");

    if(threadsExpression != null) {
      threads = Integer.parseInt(((StreamExpressionValue)threadsExpression.getParameter()).getValue());
    }

    int partitionSize = 250;

    StreamExpressionNamedParameter partitionExpression = factory.getNamedOperand(expression, "partitionSize");

    if(partitionExpression != null) {
      partitionSize = Integer.parseInt(((StreamExpressionValue)partitionExpression.getParameter()).getValue());
    }

    int maxDepth = 0;

    StreamExpressionNamedParameter depthExpression = factory.getNamedOperand(expression, "maxDepth");

    if(depthExpression == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - maxDepth param is required", expression));
    } else {
      maxDepth = Integer.parseInt(((StreamExpressionValue) depthExpression.getParameter()).getValue());
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") &&
          !namedParam.getName().equals("to") &&
          !namedParam.getName().equals("from") &&
          !namedParam.getName().equals("edge") &&
          !namedParam.getName().equals("maxDepth") &&
          !namedParam.getName().equals("threads") &&
          !namedParam.getName().equals("partitionSize"))
      {
        params.set(namedParam.getName(), namedParam.getParameter().toString().trim());
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
    init(zkHost, collectionName, fromNode, toNode, fromField, toField, params, partitionSize, threads, maxDepth);
  }

  private void init(String zkHost,
                    String collection,
                    String fromNode,
                    String toNode,
                    String fromField,
                    String toField,
                    SolrParams queryParams,
                    int joinBatchSize,
                    int threads,
                    int maxDepth) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.fromNode = fromNode;
    this.toNode = toNode;
    this.fromField = fromField;
    this.toField = toField;
    this.queryParams = queryParams;
    this.joinBatchSize = joinBatchSize;
    this.threads = threads;
    this.maxDepth = maxDepth;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {

    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);

    // parameters
    ModifiableSolrParams mParams = new ModifiableSolrParams(queryParams);
    for(Map.Entry<String, String[]> param : mParams.getMap().entrySet()){
      String value = String.join(",", param.getValue());

      // SOLR-8409: This is a special case where the params contain a " character
      // Do note that in any other BASE streams with parameters where a " might come into play
      // that this same replacement needs to take place.
      value = value.replace("\"", "\\\"");

      expression.addParameter(new StreamExpressionNamedParameter(param.getKey().toString(), value));
    }

    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    expression.addParameter(new StreamExpressionNamedParameter("maxDepth", Integer.toString(maxDepth)));
    expression.addParameter(new StreamExpressionNamedParameter("threads", Integer.toString(threads)));
    expression.addParameter(new StreamExpressionNamedParameter("partitionSize", Integer.toString(joinBatchSize)));
    expression.addParameter(new StreamExpressionNamedParameter("from", fromNode));
    expression.addParameter(new StreamExpressionNamedParameter("to", toNode));
    expression.addParameter(new StreamExpressionNamedParameter("edge", fromField+"="+toField));
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.GRAPH_SOURCE);
    explanation.setExpression(toExpression(factory).toString());
    
    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName("solr (graph)");
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);
    ModifiableSolrParams mParams = new ModifiableSolrParams(queryParams);
    child.setExpression(mParams.getMap().entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    explanation.addChild(child);
    
    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    return l;
  }

  public void open() throws IOException {

    List<Map<String,List<String>>> allVisited = new ArrayList();
    Map visited = new HashMap();
    visited.put(this.fromNode, null);

    allVisited.add(visited);
    int depth = 0;
    Map<String, List<String>> nextVisited = null;
    List<Edge> targets = new ArrayList();
    ExecutorService threadPool = null;

    try {

      threadPool = ExecutorUtil.newMDCAwareFixedThreadPool(threads, new SolrjNamedThreadFactory("ShortestPathStream"));

      //Breadth first search
      TRAVERSE:
      while (targets.size() == 0 && depth < maxDepth) {
        Set<String> nodes = visited.keySet();
        Iterator<String> it = nodes.iterator();
        nextVisited = new HashMap();
        int batchCount = 0;
        List<String> queryNodes = new ArrayList();
        List<Future> futures = new ArrayList();
        JOIN:
        //Queue up all the batches
        while (it.hasNext()) {
          String node = it.next();
          queryNodes.add(node);
          ++batchCount;
          if (batchCount == joinBatchSize || !it.hasNext()) {
            try {
              JoinRunner joinRunner = new JoinRunner(queryNodes);
              Future<List<Edge>> future = threadPool.submit(joinRunner);
              futures.add(future);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            batchCount = 0;
            queryNodes = new ArrayList();
          }
        }

        try {
          //Process the batches as they become available
          OUTER:
          for (Future<List<Edge>> future : futures) {
            List<Edge> edges = future.get();
            INNER:
            for (Edge edge : edges) {
              if (toNode.equals(edge.to)) {
                targets.add(edge);
                if(nextVisited.containsKey(edge.to)) {
                  List<String> parents = nextVisited.get(edge.to);
                  parents.add(edge.from);
                } else {
                  List<String> parents = new ArrayList();
                  parents.add(edge.from);
                  nextVisited.put(edge.to, parents);
                }
              } else {
                if (!cycle(edge.to, allVisited)) {
                  if(nextVisited.containsKey(edge.to)) {
                    List<String> parents = nextVisited.get(edge.to);
                    parents.add(edge.from);
                  } else {
                    List<String> parents = new ArrayList();
                    parents.add(edge.from);
                    nextVisited.put(edge.to, parents);
                  }
                }
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        allVisited.add(nextVisited);
        visited = nextVisited;
        ++depth;
      }
    } finally {
      threadPool.shutdown();
    }

    Set<String> finalPaths = new HashSet();
    if(targets.size() > 0) {
      for(Edge edge : targets) {
        List<LinkedList> paths = new ArrayList();
        LinkedList<String> path = new LinkedList();
        path.addFirst(edge.to);
        paths.add(path);
        //Walk back up the tree a collect the parent nodes.
        INNER:
        for (int i = allVisited.size() - 1; i >= 0; --i) {
          Map<String, List<String>> v = allVisited.get(i);
          Iterator<LinkedList> it = paths.iterator();
          List newPaths = new ArrayList();
          while(it.hasNext()) {
            LinkedList p = it.next();
            List<String> parents = v.get(p.peekFirst());
            if (parents != null) {
              for(String parent : parents) {
                LinkedList newPath = new LinkedList(p);
                newPath.addFirst(parent);
                newPaths.add(newPath);
              }
              paths = newPaths;
            }
          }
        }

        for(LinkedList p : paths) {
          String s = p.toString();
          if (!finalPaths.contains(s)){
            Tuple shortestPath = new Tuple(new HashMap());
            shortestPath.put("path", p);
            shortestPaths.add(shortestPath);
            finalPaths.add(s);
          }
        }
      }
    }
  }

  private class JoinRunner implements Callable<List<Edge>> {

    private List<String> nodes;
    private List<Edge> edges = new ArrayList();

    public JoinRunner(List<String> nodes) {
      this.nodes = nodes;
    }

    public List<Edge> call() {

      ModifiableSolrParams joinParams = new ModifiableSolrParams(queryParams);
      String fl = fromField + "," + toField;

      joinParams.set("fl", fl);
      joinParams.set("qt", "/export");
      joinParams.set(SORT, toField + " asc,"+fromField +" asc");

      StringBuffer nodeQuery = new StringBuffer();

      for(String node : nodes) {
        nodeQuery.append(node).append(" ");
      }

      String q = fromField + ":(" + nodeQuery.toString().trim() + ")";

      joinParams.set("q", q);
      TupleStream stream = null;
      try {
        stream = new UniqueStream(new CloudSolrStream(zkHost, collection, joinParams), new MultipleFieldEqualitor(new FieldEqualitor(toField), new FieldEqualitor(fromField)));
        stream.setStreamContext(streamContext);
        stream.open();
        BATCH:
        while (true) {
          Tuple tuple = stream.read();
          if (tuple.EOF) {
            break BATCH;
          }
          String _toNode = tuple.getString(toField);
          String _fromNode = tuple.getString(fromField);
          Edge edge = new Edge(_fromNode, _toNode);
          edges.add(edge);
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

  private static class Edge {

    private String from;
    private String to;

    public Edge(String from, String to) {
      this.from = from;
      this.to = to;
    }
  }

  private boolean cycle(String node, List<Map<String,List<String>>> allVisited) {
    //Check all visited trees for each level to see if we've encountered this node before.
    for(Map<String, List<String>> visited : allVisited) {
      if(visited.containsKey(node)) {
        return true;
      }
    }

    return false;
  }

  public void close() throws IOException {
    this.found = false;
  }

  public Tuple read() throws IOException {
    if(shortestPaths.size() > 0) {
      found = true;
      Tuple t = shortestPaths.removeFirst();
      return t;
    } else {
      Map m = new HashMap();
      m.put("EOF", true);
      if(!found) {
        m.put("sorry", "No path found");
      }
      return new Tuple(m);
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }
}
