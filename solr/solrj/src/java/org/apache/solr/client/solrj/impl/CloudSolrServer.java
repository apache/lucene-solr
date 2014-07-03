package org.apache.solr.client.solrj.impl;

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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.KeeperException;

/**
 * SolrJ client class to communicate with SolrCloud.
 * Instances of this class communicate with Zookeeper to discover
 * Solr endpoints for SolrCloud collections, and then use the 
 * {@link LBHttpSolrServer} to issue requests.
 * 
 * This class assumes the id field for your documents is called
 * 'id' - if this is not the case, you must set the right name
 * with {@link #setIdField(String)}.
 */
public class CloudSolrServer extends SolrServer {
  private volatile ZkStateReader zkStateReader;
  private String zkHost; // the zk server address
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  private volatile String defaultCollection;
  private final LBHttpSolrServer lbServer;
  private final boolean shutdownLBHttpSolrServer;
  private HttpClient myClient;
  Random rand = new Random();
  
  private final boolean updatesToLeaders;
  private boolean parallelUpdates = true;
  private ExecutorService threadPool = Executors
      .newCachedThreadPool(new SolrjNamedThreadFactory(
          "CloudSolrServer ThreadPool"));
  private String idField = "id";
  private final Set<String> NON_ROUTABLE_PARAMS;
  {
    NON_ROUTABLE_PARAMS = new HashSet<>();
    NON_ROUTABLE_PARAMS.add(UpdateParams.EXPUNGE_DELETES);
    NON_ROUTABLE_PARAMS.add(UpdateParams.MAX_OPTIMIZE_SEGMENTS);
    NON_ROUTABLE_PARAMS.add(UpdateParams.COMMIT);
    NON_ROUTABLE_PARAMS.add(UpdateParams.WAIT_SEARCHER);
    NON_ROUTABLE_PARAMS.add(UpdateParams.OPEN_SEARCHER);
    
    NON_ROUTABLE_PARAMS.add(UpdateParams.SOFT_COMMIT);
    NON_ROUTABLE_PARAMS.add(UpdateParams.PREPARE_COMMIT);
    NON_ROUTABLE_PARAMS.add(UpdateParams.OPTIMIZE);
    
    // Not supported via SolrCloud
    // NON_ROUTABLE_PARAMS.add(UpdateParams.ROLLBACK);

  }



  /**
   * @param zkHost The client endpoint of the zookeeper quorum containing the cloud state,
   * in the form HOST:PORT.
   */
  public CloudSolrServer(String zkHost) {
      this.zkHost = zkHost;
      this.myClient = HttpClientUtil.createClient(null);
      this.lbServer = new LBHttpSolrServer(myClient);
      this.lbServer.setRequestWriter(new BinaryRequestWriter());
      this.lbServer.setParser(new BinaryResponseParser());
      this.updatesToLeaders = true;
      shutdownLBHttpSolrServer = true;
  }
  
  public CloudSolrServer(String zkHost, boolean updatesToLeaders)
      throws MalformedURLException {
    this.zkHost = zkHost;
    this.myClient = HttpClientUtil.createClient(null);
    this.lbServer = new LBHttpSolrServer(myClient);
    this.lbServer.setRequestWriter(new BinaryRequestWriter());
    this.lbServer.setParser(new BinaryResponseParser());
    this.updatesToLeaders = updatesToLeaders;
    shutdownLBHttpSolrServer = true;
  }

  /**
   * @param zkHost The client endpoint of the zookeeper quorum containing the cloud state,
   * in the form HOST:PORT.
   * @param lbServer LBHttpSolrServer instance for requests. 
   */
  public CloudSolrServer(String zkHost, LBHttpSolrServer lbServer) {
    this.zkHost = zkHost;
    this.lbServer = lbServer;
    this.updatesToLeaders = true;
    shutdownLBHttpSolrServer = false;
  }
  
  /**
   * @param zkHost The client endpoint of the zookeeper quorum containing the cloud state,
   * in the form HOST:PORT.
   * @param lbServer LBHttpSolrServer instance for requests. 
   * @param updatesToLeaders sends updates only to leaders - defaults to true
   */
  public CloudSolrServer(String zkHost, LBHttpSolrServer lbServer, boolean updatesToLeaders) {
    this.zkHost = zkHost;
    this.lbServer = lbServer;
    this.updatesToLeaders = updatesToLeaders;
    shutdownLBHttpSolrServer = false;
  }
  
  public ResponseParser getParser() {
    return lbServer.getParser();
  }
  
  /**
   * Note: This setter method is <b>not thread-safe</b>.
   * 
   * @param processor
   *          Default Response Parser chosen to parse the response if the parser
   *          were not specified as part of the request.
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   */
  public void setParser(ResponseParser processor) {
    lbServer.setParser(processor);
  }
  
  public RequestWriter getRequestWriter() {
    return lbServer.getRequestWriter();
  }
  
  public void setRequestWriter(RequestWriter requestWriter) {
    lbServer.setRequestWriter(requestWriter);
  }

  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  /**
   * @param idField the field to route documents on.
   */
  public void setIdField(String idField) {
    this.idField = idField;
  }

  /**
   * @return the field that updates are routed on.
   */
  public String getIdField() {
    return idField;
  }
  
  /** Sets the default collection for request */
  public void setDefaultCollection(String collection) {
    this.defaultCollection = collection;
  }

  /** Gets the default collection for request */
  public String getDefaultCollection() {
    return defaultCollection;
  }

  /** Set the connect timeout to the zookeeper ensemble in ms */
  public void setZkConnectTimeout(int zkConnectTimeout) {
    this.zkConnectTimeout = zkConnectTimeout;
  }

  /** Set the timeout to the zookeeper ensemble in ms */
  public void setZkClientTimeout(int zkClientTimeout) {
    this.zkClientTimeout = zkClientTimeout;
  }

  /**
   * Connect to the zookeeper ensemble.
   * This is an optional method that may be used to force a connect before any other requests are sent.
   *
   */
  public void connect() {
    if (zkStateReader == null) {
      synchronized (this) {
        if (zkStateReader == null) {
          ZkStateReader zk = null;
          try {
            zk = new ZkStateReader(zkHost, zkClientTimeout,
                zkConnectTimeout);
            zk.createClusterStateWatchersAndUpdate();
            zkStateReader = zk;
          } catch (InterruptedException e) {
            if (zk != null) zk.close();
            Thread.currentThread().interrupt();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (KeeperException e) {
            if (zk != null) zk.close();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (IOException e) {
            if (zk != null) zk.close();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (TimeoutException e) {
            if (zk != null) zk.close();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (Exception e) {
            if (zk != null) zk.close();
            // do not wrap because clients may be relying on the underlying exception being thrown
            throw e;
          }
        }
      }
    }
  }

  public void setParallelUpdates(boolean parallelUpdates) {
    this.parallelUpdates = parallelUpdates;
  }

  private NamedList directUpdate(AbstractUpdateRequest request, ClusterState clusterState) throws SolrServerException {
    UpdateRequest updateRequest = (UpdateRequest) request;
    ModifiableSolrParams params = (ModifiableSolrParams) request.getParams();
    ModifiableSolrParams routableParams = new ModifiableSolrParams();
    ModifiableSolrParams nonRoutableParams = new ModifiableSolrParams();

    if(params != null) {
      nonRoutableParams.add(params);
      routableParams.add(params);
      for(String param : NON_ROUTABLE_PARAMS) {
        routableParams.remove(param);
      }
    }

    String collection = nonRoutableParams.get(UpdateParams.COLLECTION, defaultCollection);
    if (collection == null) {
      throw new SolrServerException("No collection param specified on request and no default collection has been set.");
    }


    //Check to see if the collection is an alias.
    Aliases aliases = zkStateReader.getAliases();
    if(aliases != null) {
      Map<String, String> collectionAliases = aliases.getCollectionAliasMap();
      if(collectionAliases != null && collectionAliases.containsKey(collection)) {
        collection = collectionAliases.get(collection);
      }
    }

    DocCollection col = clusterState.getCollection(collection);

    DocRouter router = col.getRouter();
    
    if (router instanceof ImplicitDocRouter) {
      // short circuit as optimization
      return null;
    }

    //Create the URL map, which is keyed on slice name.
    //The value is a list of URLs for each replica in the slice.
    //The first value in the list is the leader for the slice.
    Map<String,List<String>> urlMap = buildUrlMap(col);
    if (urlMap == null) {
      // we could not find a leader yet - use unoptimized general path
      return null;
    }

    NamedList<Throwable> exceptions = new NamedList<Throwable>();
    NamedList<NamedList> shardResponses = new NamedList<NamedList>();

    Map<String, LBHttpSolrServer.Req> routes = updateRequest.getRoutes(router, col, urlMap, routableParams, this.idField);
    if (routes == null) {
      return null;
    }

    long start = System.nanoTime();

    if (parallelUpdates) {
      final Map<String, Future<NamedList<?>>> responseFutures = new HashMap<>(routes.size());
      for (final Map.Entry<String, LBHttpSolrServer.Req> entry : routes.entrySet()) {
        final String url = entry.getKey();
        final LBHttpSolrServer.Req lbRequest = entry.getValue();
        responseFutures.put(url, threadPool.submit(new Callable<NamedList<?>>() {
          @Override
          public NamedList<?> call() throws Exception {
            return lbServer.request(lbRequest).getResponse();
          }
        }));
      }

      for (final Map.Entry<String, Future<NamedList<?>>> entry: responseFutures.entrySet()) {
        final String url = entry.getKey();
        final Future<NamedList<?>> responseFuture = entry.getValue();
        try {
          shardResponses.add(url, responseFuture.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          exceptions.add(url, e.getCause());
        }
      }

      if (exceptions.size() > 0) {
        throw new RouteException(ErrorCode.SERVER_ERROR, exceptions, routes);
      }
    } else {
      for (Map.Entry<String, LBHttpSolrServer.Req> entry : routes.entrySet()) {
        String url = entry.getKey();
        LBHttpSolrServer.Req lbRequest = entry.getValue();
        try {
          NamedList rsp = lbServer.request(lbRequest).getResponse();
          shardResponses.add(url, rsp);
        } catch (Exception e) {
          throw new SolrServerException(e);
        }
      }
    }

    UpdateRequest nonRoutableRequest = null;
    List<String> deleteQuery = updateRequest.getDeleteQuery();
    if (deleteQuery != null && deleteQuery.size() > 0) {
      UpdateRequest deleteQueryRequest = new UpdateRequest();
      deleteQueryRequest.setDeleteQuery(deleteQuery);
      nonRoutableRequest = deleteQueryRequest;
    }
    
    Set<String> paramNames = nonRoutableParams.getParameterNames();
    
    Set<String> intersection = new HashSet<>(paramNames);
    intersection.retainAll(NON_ROUTABLE_PARAMS);
    
    if (nonRoutableRequest != null || intersection.size() > 0) {
      if (nonRoutableRequest == null) {
        nonRoutableRequest = new UpdateRequest();
      }
      nonRoutableRequest.setParams(nonRoutableParams);
      List<String> urlList = new ArrayList<>();
      urlList.addAll(routes.keySet());
      Collections.shuffle(urlList, rand);
      LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(nonRoutableRequest, urlList);
      try {
        LBHttpSolrServer.Rsp rsp = lbServer.request(req);
        shardResponses.add(urlList.get(0), rsp.getResponse());
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, urlList.get(0), e);
      }
    }

    long end = System.nanoTime();

    RouteResponse rr =  condenseResponse(shardResponses, (long)((end - start)/1000000));
    rr.setRouteResponses(shardResponses);
    rr.setRoutes(routes);
    return rr;
  }

  private Map<String,List<String>> buildUrlMap(DocCollection col) {
    Map<String, List<String>> urlMap = new HashMap<>();
    Collection<Slice> slices = col.getActiveSlices();
    Iterator<Slice> sliceIterator = slices.iterator();
    while (sliceIterator.hasNext()) {
      Slice slice = sliceIterator.next();
      String name = slice.getName();
      List<String> urls = new ArrayList<>();
      Replica leader = slice.getLeader();
      if (leader == null) {
        // take unoptimized general path - we cannot find a leader yet
        return null;
      }
      ZkCoreNodeProps zkProps = new ZkCoreNodeProps(leader);
      String url = zkProps.getCoreUrl();
      urls.add(url);
      Collection<Replica> replicas = slice.getReplicas();
      Iterator<Replica> replicaIterator = replicas.iterator();
      while (replicaIterator.hasNext()) {
        Replica replica = replicaIterator.next();
        if (!replica.getNodeName().equals(leader.getNodeName()) &&
            !replica.getName().equals(leader.getName())) {
          ZkCoreNodeProps zkProps1 = new ZkCoreNodeProps(replica);
          String url1 = zkProps1.getCoreUrl();
          urls.add(url1);
        }
      }
      urlMap.put(name, urls);
    }
    return urlMap;
  }

  public RouteResponse condenseResponse(NamedList response, long timeMillis) {
    RouteResponse condensed = new RouteResponse();
    int status = 0;
    Integer rf = null;
    Integer minRf = null;
    for(int i=0; i<response.size(); i++) {
      NamedList shardResponse = (NamedList)response.getVal(i);
      NamedList header = (NamedList)shardResponse.get("responseHeader");      
      Integer shardStatus = (Integer)header.get("status");
      int s = shardStatus.intValue();
      if(s > 0) {
          status = s;
      }
      Object rfObj = header.get(UpdateRequest.REPFACT);
      if (rfObj != null && rfObj instanceof Integer) {
        Integer routeRf = (Integer)rfObj;
        if (rf == null || routeRf < rf)
          rf = routeRf;
      }
      minRf = (Integer)header.get(UpdateRequest.MIN_REPFACT);
    }

    NamedList cheader = new NamedList();
    cheader.add("status", status);
    cheader.add("QTime", timeMillis);
    if (rf != null)
      cheader.add(UpdateRequest.REPFACT, rf);
    if (minRf != null)
      cheader.add(UpdateRequest.MIN_REPFACT, minRf);
    
    condensed.add("responseHeader", cheader);
    return condensed;
  }

  public static class RouteResponse extends NamedList {
    private NamedList routeResponses;
    private Map<String, LBHttpSolrServer.Req> routes;

    public void setRouteResponses(NamedList routeResponses) {
      this.routeResponses = routeResponses;
    }

    public NamedList getRouteResponses() {
      return routeResponses;
    }

    public void setRoutes(Map<String, LBHttpSolrServer.Req> routes) {
      this.routes = routes;
    }

    public Map<String, LBHttpSolrServer.Req> getRoutes() {
      return routes;
    }

  }

  public static class RouteException extends SolrException {

    private NamedList<Throwable> throwables;
    private Map<String, LBHttpSolrServer.Req> routes;

    public RouteException(ErrorCode errorCode, NamedList<Throwable> throwables, Map<String, LBHttpSolrServer.Req> routes){
      super(errorCode, throwables.getVal(0).getMessage(), throwables.getVal(0));
      this.throwables = throwables;
      this.routes = routes;
    }

    public NamedList<Throwable> getThrowables() {
      return throwables;
    }

    public Map<String, LBHttpSolrServer.Req> getRoutes() {
      return this.routes;
    }
  }

  @Override
  public NamedList<Object> request(SolrRequest request)
      throws SolrServerException, IOException {
    connect();
    
    ClusterState clusterState = zkStateReader.getClusterState();
    
    boolean sendToLeaders = false;
    List<String> replicas = null;
    
    if (request instanceof IsUpdateRequest) {
      if (request instanceof UpdateRequest) {
        NamedList response = directUpdate((AbstractUpdateRequest) request,
            clusterState);
        if (response != null) {
          return response;
        }
      }
      sendToLeaders = true;
      replicas = new ArrayList<>();
    }
    
    SolrParams reqParams = request.getParams();
    if (reqParams == null) {
      reqParams = new ModifiableSolrParams();
    }
    List<String> theUrlList = new ArrayList<>();
    if (request.getPath().equals("/admin/collections")
        || request.getPath().equals("/admin/cores")) {
      Set<String> liveNodes = clusterState.getLiveNodes();
      for (String liveNode : liveNodes) {
        theUrlList.add(zkStateReader.getBaseUrlForNodeName(liveNode));
      }
    } else {
      String collection = reqParams.get(UpdateParams.COLLECTION, defaultCollection);
      
      if (collection == null) {
        throw new SolrServerException(
            "No collection param specified on request and no default collection has been set.");
      }
      
      Set<String> collectionsList = getCollectionList(clusterState, collection);
      if (collectionsList.size() == 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Could not find collection: " + collection);
      }

      String shardKeys =  reqParams.get(ShardParams._ROUTE_);
      if(shardKeys == null) {
        shardKeys = reqParams.get(ShardParams.SHARD_KEYS); // deprecated
      }

      // TODO: not a big deal because of the caching, but we could avoid looking
      // at every shard
      // when getting leaders if we tweaked some things
      
      // Retrieve slices from the cloud state and, for each collection
      // specified,
      // add it to the Map of slices.
      Map<String,Slice> slices = new HashMap<>();
      for (String collectionName : collectionsList) {
        DocCollection col = clusterState.getCollection(collectionName);
        Collection<Slice> routeSlices = col.getRouter().getSearchSlices(shardKeys, reqParams , col);
        ClientUtils.addSlices(slices, collectionName, routeSlices, true);
      }
      Set<String> liveNodes = clusterState.getLiveNodes();

      List<String> leaderUrlList = null;
      List<String> urlList = null;
      List<String> replicasList = null;
      
      // build a map of unique nodes
      // TODO: allow filtering by group, role, etc
      Map<String,ZkNodeProps> nodes = new HashMap<>();
      List<String> urlList2 = new ArrayList<>();
      for (Slice slice : slices.values()) {
        for (ZkNodeProps nodeProps : slice.getReplicasMap().values()) {
          ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(nodeProps);
          String node = coreNodeProps.getNodeName();
          if (!liveNodes.contains(coreNodeProps.getNodeName())
              || !coreNodeProps.getState().equals(ZkStateReader.ACTIVE)) continue;
          if (nodes.put(node, nodeProps) == null) {
            if (!sendToLeaders || (sendToLeaders && coreNodeProps.isLeader())) {
              String url;
              if (reqParams.get(UpdateParams.COLLECTION) == null) {
                url = ZkCoreNodeProps.getCoreUrl(
                    nodeProps.getStr(ZkStateReader.BASE_URL_PROP),
                    defaultCollection);
              } else {
                url = coreNodeProps.getCoreUrl();
              }
              urlList2.add(url);
            } else if (sendToLeaders) {
              String url;
              if (reqParams.get(UpdateParams.COLLECTION) == null) {
                url = ZkCoreNodeProps.getCoreUrl(
                    nodeProps.getStr(ZkStateReader.BASE_URL_PROP),
                    defaultCollection);
              } else {
                url = coreNodeProps.getCoreUrl();
              }
              replicas.add(url);
            }
          }
        }
      }
      
      if (sendToLeaders) {
        leaderUrlList = urlList2;
        replicasList = replicas;
      } else {
        urlList = urlList2;
      }
      
      if (sendToLeaders) {
        theUrlList = new ArrayList<>(leaderUrlList.size());
        theUrlList.addAll(leaderUrlList);
      } else {
        theUrlList = new ArrayList<>(urlList.size());
        theUrlList.addAll(urlList);
      }
      Collections.shuffle(theUrlList, rand);
      if (sendToLeaders) {
        ArrayList<String> theReplicas = new ArrayList<>(
            replicasList.size());
        theReplicas.addAll(replicasList);
        Collections.shuffle(theReplicas, rand);
        // System.out.println("leaders:" + theUrlList);
        // System.out.println("replicas:" + theReplicas);
        theUrlList.addAll(theReplicas);
      }
      
    }
    
    // System.out.println("########################## MAKING REQUEST TO " +
    // theUrlList);
    
    LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(request, theUrlList);
    LBHttpSolrServer.Rsp rsp = lbServer.request(req);
    return rsp.getResponse();
  }

  private Set<String> getCollectionList(ClusterState clusterState,
      String collection) {
    // Extract each comma separated collection name and store in a List.
    List<String> rawCollectionsList = StrUtils.splitSmart(collection, ",", true);
    Set<String> collectionsList = new HashSet<>();
    // validate collections
    for (String collectionName : rawCollectionsList) {
      if (!clusterState.getCollections().contains(collectionName)) {
        Aliases aliases = zkStateReader.getAliases();
        String alias = aliases.getCollectionAlias(collectionName);
        if (alias != null) {
          List<String> aliasList = StrUtils.splitSmart(alias, ",", true); 
          collectionsList.addAll(aliasList);
          continue;
        }
        
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection not found: " + collectionName);
      }
      
      collectionsList.add(collectionName);
    }
    return collectionsList;
  }

  @Override
  public void shutdown() {
    if (zkStateReader != null) {
      synchronized(this) {
        if (zkStateReader!= null)
          zkStateReader.close();
        zkStateReader = null;
      }
    }
    
    if (shutdownLBHttpSolrServer) {
      lbServer.shutdown();
    }
    
    if (myClient!=null) {
      myClient.getConnectionManager().shutdown();
    }

    if(this.threadPool != null && !this.threadPool.isShutdown()) {
      this.threadPool.shutdown();
    }
  }

  public LBHttpSolrServer getLbServer() {
    return lbServer;
  }
  
  public boolean isUpdatesToLeaders() {
    return updatesToLeaders;
  }

  /**
   * Useful for determining the minimum achieved replication factor across
   * all shards involved in processing an update request, typically useful
   * for gauging the replication factor of a batch. 
   */
  @SuppressWarnings("rawtypes")
  public int getMinAchievedReplicationFactor(String collection, NamedList resp) {
    // it's probably already on the top-level header set by condense
    NamedList header = (NamedList)resp.get("responseHeader");
    Integer achRf = (Integer)header.get(UpdateRequest.REPFACT);
    if (achRf != null)
      return achRf.intValue();

    // not on the top-level header, walk the shard route tree
    Map<String,Integer> shardRf = getShardReplicationFactor(collection, resp);
    for (Integer rf : shardRf.values()) {
      if (achRf == null || rf < achRf) {
        achRf = rf;
      }
    }    
    return (achRf != null) ? achRf.intValue() : -1;
  }
  
  /**
   * Walks the NamedList response after performing an update request looking for
   * the replication factor that was achieved in each shard involved in the request.
   * For single doc updates, there will be only one shard in the return value. 
   */
  @SuppressWarnings("rawtypes")
  public Map<String,Integer> getShardReplicationFactor(String collection, NamedList resp) {
    connect();
    
    Map<String,Integer> results = new HashMap<String,Integer>();
    if (resp instanceof CloudSolrServer.RouteResponse) {
      NamedList routes = ((CloudSolrServer.RouteResponse)resp).getRouteResponses();      
      ClusterState clusterState = zkStateReader.getClusterState();     
      Map<String,String> leaders = new HashMap<String,String>();
      for (Slice slice : clusterState.getActiveSlices(collection)) {
        Replica leader = slice.getLeader();
        if (leader != null) {
          ZkCoreNodeProps zkProps = new ZkCoreNodeProps(leader);
          String leaderUrl = zkProps.getBaseUrl() + "/" + zkProps.getCoreName();
          leaders.put(leaderUrl, slice.getName());
          String altLeaderUrl = zkProps.getBaseUrl() + "/" + collection;
          leaders.put(altLeaderUrl, slice.getName());
        }
      }
      
      Iterator<Map.Entry<String,Object>> routeIter = routes.iterator();
      while (routeIter.hasNext()) {
        Map.Entry<String,Object> next = routeIter.next();
        String host = next.getKey();
        NamedList hostResp = (NamedList)next.getValue();
        Integer rf = (Integer)((NamedList)hostResp.get("responseHeader")).get(UpdateRequest.REPFACT);
        if (rf != null) {
          String shard = leaders.get(host);
          if (shard == null) {
            if (host.endsWith("/"))
              shard = leaders.get(host.substring(0,host.length()-1));
            if (shard == null) {
              shard = host;
            }
          }
          results.put(shard, rf);
        }
      }
    }    
    return results;
  }
}
