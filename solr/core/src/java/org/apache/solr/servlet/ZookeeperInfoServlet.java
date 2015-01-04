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

package org.apache.solr.servlet;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.FastWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Zookeeper Info
 *
 * @since solr 4.0
 */
public final class ZookeeperInfoServlet extends BaseSolrServlet {
  static final Logger log = LoggerFactory.getLogger(ZookeeperInfoServlet.class);
  
  // used for custom sorting collection names looking like prefix##
  // only go out to 7 digits (which safely fits in an int)
  private static final Pattern endsWithDigits = Pattern.compile("^(\\D*)(\\d{1,7}?)$");
  
  /**
   * Enumeration of ways to filter collections on the graph panel.
   */
  static enum FilterType {
    none, name, status  
  }
  
  /**
   * Holds state of a single page of collections requested from the cloud panel.
   */
  static final class PageOfCollections {
    List<String> selected;
    int numFound = 0; // total number of matches (across all pages)
    int start = 0;
    int rows = -1;
    FilterType filterType;
    String filter;
    
    PageOfCollections(int start, int rows, FilterType filterType, String filter) {
      this.start = start;
      this.rows = rows;
      this.filterType = filterType;
      this.filter = filter;
    }
    
    void selectPage(List<String> collections) {
      numFound = collections.size();
      // start with full set and then find the sublist for the desired selected
      selected = collections;
                  
      if (rows > 0) { // paging desired
        if (start > numFound)
          start = 0; // this might happen if they applied a new filter
        
        int lastIndex = Math.min(start+rows, numFound);        
        if (start > 0 || lastIndex < numFound)
          selected = collections.subList(start, lastIndex);
      }     
    }
                
    /**
     * Filters a list of collections by name if applicable. 
     */
    List<String> applyNameFilter(List<String> collections) {
      
      if (filterType != FilterType.name || filter == null)
        return collections; // name filter doesn't apply
            
      // typically, a user will type a prefix and then *, e.g. tj*
      // when they really mean tj.*
      String regexFilter = (!filter.endsWith(".*") && filter.endsWith("*")) 
          ? filter.substring(0,filter.length()-1)+".*" : filter; 
      
      // case-insensitive
      if (!regexFilter.startsWith("(?i)"))
        regexFilter = "(?i)"+regexFilter;
      
      Pattern filterRegex = Pattern.compile(regexFilter);        
      List<String> filtered = new ArrayList<String>();
      for (String next : collections) {
        if (matches(filterRegex, next))
          filtered.add(next);
      }
      
      return filtered;
    }
    
    /**
     * Walk the collection state JSON object to see if it has any replicas that match
     * the state the user is filtering by. 
     */
    @SuppressWarnings("unchecked")
    final boolean matchesStatusFilter(Map<String,Object> collectionState, Set<String> liveNodes) {
      
      if (filterType != FilterType.status || filter == null || filter.length() == 0)
        return true; // no status filter, so all match
      
      boolean isHealthy = true; // means all replicas for all shards active
      boolean hasDownedShard = false; // means one or more shards is down
      boolean replicaInRecovery = false;
      
      Map<String,Object> shards = (Map<String,Object>)collectionState.get("shards");
      for (String shardId : shards.keySet()) {
        boolean hasActive = false;
        Map<String,Object> shard = (Map<String,Object>)shards.get(shardId);
        Map<String,Object> replicas = (Map<String,Object>)shard.get("replicas");
        for (String replicaId : replicas.keySet()) {
          Map<String,Object> replicaState = (Map<String,Object>)replicas.get(replicaId);
          String coreState = (String)replicaState.get("state");
          String nodeName = (String)replicaState.get("node_name");
          
          // state can lie to you if the node is offline, so need to reconcile with live_nodes too
          if (!liveNodes.contains(nodeName))
            coreState = "down"; // not on a live node, so must be down
          
          if ("active".equals(coreState)) {
            hasActive = true; // assumed no replicas active and found one that is for this shard
          } else {
            if ("recovering".equals(coreState)) {
              replicaInRecovery = true;
            }
            isHealthy = false; // assumed healthy and found one replica that is not
          }          
        }
        
        if (!hasActive)
          hasDownedShard = true; // this is bad
      }
      
      if ("healthy".equals(filter)) {
        return isHealthy;
      } else if ("degraded".equals(filter)) {
        return !hasDownedShard && !isHealthy; // means no shards offline but not 100% healthy either
      } else if ("downed_shard".equals(filter)) {
        return hasDownedShard;
      } else if ("recovering".equals(filter)) {
        return !isHealthy && replicaInRecovery;
      }
      
      return true;
    }
    
    final boolean matches(final Pattern filter, final String collName) {
      return filter.matcher(collName).matches();
    }
    
    String getPagingHeader() {
      return start+"|"+rows+"|"+numFound+"|"+(filterType != null ? filterType.toString() : "")+"|"+(filter != null ? filter : "");
    }
    
    public String toString() {
      return getPagingHeader();
    }

  }
  
  /**
   * Supports paged navigation of collections on the cloud panel. To avoid serving
   * stale collection data, this object watches the /collections znode, which will
   * change if a collection is added or removed.
   */
  static final class PagedCollectionSupport implements Watcher, Comparator<String>, OnReconnect {

    // this is the full merged list of collections from ZooKeeper
    private List<String> cachedCollections;

    /**
     * If the list of collections changes, mark the cache as stale.
     */
    @Override
    public void process(WatchedEvent event) {
      synchronized (this) {
        cachedCollections = null;
      }
    }
    
    /**
     * Create a merged view of all collections (internal from /clusterstate.json and external from /collections/?/state.json 
     */
    private synchronized List<String> getCollections(SolrZkClient zkClient) throws KeeperException, InterruptedException {
      if (cachedCollections == null) {
        // cache is stale, rebuild the full list ...
        cachedCollections = new ArrayList<String>();
        
        List<String> fromZk = zkClient.getChildren("/collections", this, true);
        if (fromZk != null)
          cachedCollections.addAll(fromZk);
                
        // sort the final merged set of collections
        Collections.sort(cachedCollections, this);
      }
      
      return cachedCollections;
    }
                
    /**
     * Gets the requested page of collections after applying filters and offsets. 
     */
    public PageOfCollections fetchPage(PageOfCollections page, SolrZkClient zkClient) 
        throws KeeperException, InterruptedException {


      List<String> children = getCollections(zkClient);
      page.selected = children; // start with the page being the full list
      
      // activate paging (if disabled) for large collection sets
      if (page.start == 0 && page.rows == -1 && page.filter == null && children.size() > 10) {
        page.rows = 20;
        page.start = 0;
      }
      
      // apply the name filter if supplied (we don't need to pull state
      // data from ZK to do name filtering
      if (page.filterType == FilterType.name && page.filter != null)
        children = page.applyNameFilter(children);

      // a little hacky ... we can't select the page when filtering by
      // status until reading all status objects from ZK
      if (page.filterType != FilterType.status)
        page.selectPage(children);        
      
      return page;
    }
        
    @Override
    public int compare(String left, String right) {
      if (left == null)
        return -1;
      
      if (left.equals(right))
        return 0;
      
      // sort lexically unless the two collection names start with the same base prefix
      // and end in a number (which is a common enough naming scheme to have direct 
      // support for it)
      Matcher leftMatcher = endsWithDigits.matcher(left);
      if (leftMatcher.matches()) {
        Matcher rightMatcher = endsWithDigits.matcher(right);
        if (rightMatcher.matches()) {
          String leftGroup1 = leftMatcher.group(1);
          String rightGroup1 = rightMatcher.group(1);
          if (leftGroup1.equals(rightGroup1)) {
            // both start with the same prefix ... compare indexes
            // using longs here as we don't know how long the 2nd group is
            int leftGroup2 = Integer.parseInt(leftMatcher.group(2));
            int rightGroup2 = Integer.parseInt(rightMatcher.group(2));            
            return (leftGroup2 > rightGroup2) ? 1 : ((leftGroup2 == rightGroup2) ? 0 : -1);
          }
        }
      }
      return left.compareTo(right);
    }

    /**
     * Called after a ZooKeeper session expiration occurs
     */
    @Override
    public void command() {
      // we need to re-establish the watcher on the collections list after session expires
      synchronized (this) {
        cachedCollections = null;
      }
    }
  }
  
  private PagedCollectionSupport pagingSupport;

  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws ServletException,IOException {
    // This attribute is set by the SolrDispatchFilter
    CoreContainer cores = (CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");
    if (cores == null) {
      throw new ServletException("Missing request attribute org.apache.solr.CoreContainer.");
    }

    synchronized (this) {
      if (pagingSupport == null) {
        pagingSupport = new PagedCollectionSupport();
        ZkController zkController = cores.getZkController();
        if (zkController != null) {
          // get notified when the ZK session expires (so we can clear the cached collections and rebuild)
          zkController.addOnReconnectListener(pagingSupport);
        }
      }
    }
    
    final SolrParams params;
    try {
      params = SolrRequestParsers.DEFAULT.parse(null, request.getServletPath(), request).getParams();
    } catch (Exception e) {
      int code=500;
      if (e instanceof SolrException) {
        code = Math.min(599, Math.max(100, ((SolrException)e).code()));
      }
      response.sendError(code, e.toString());
      return;
    }

    String path = params.get("path");
    String addr = params.get("addr");

    if (addr != null && addr.length() == 0) {
      addr = null;
    }

    String detailS = params.get("detail");
    boolean detail = detailS != null && detailS.equals("true");

    String dumpS = params.get("dump");
    boolean dump = dumpS != null && dumpS.equals("true");
    
    int start = paramAsInt("start", params, 0);
    int rows = paramAsInt("rows", params, -1);
    
    String filterType = params.get("filterType");
    if (filterType != null) {
      filterType = filterType.trim().toLowerCase(Locale.ROOT);
      if (filterType.length() == 0)
        filterType = null;
    }
    FilterType type = (filterType != null) ? FilterType.valueOf(filterType) : FilterType.none;
    
    String filter = (type != FilterType.none) ? params.get("filter") : null;
    if (filter != null) {
      filter = filter.trim();
      if (filter.length() == 0)
        filter = null;
    }
    
    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");

    Writer out = new FastWriter(new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8));

    ZKPrinter printer = new ZKPrinter(response, out, cores.getZkController(), addr);
    printer.detail = detail;
    printer.dump = dump;
    boolean isGraphView = "graph".equals(params.get("view"));
    printer.page = (isGraphView && "/clusterstate.json".equals(path))
        ? new PageOfCollections(start, rows, type, filter) : null;
    printer.pagingSupport = pagingSupport;

    try {
      printer.print(path);
    } finally {
      printer.close();
    }
    
    out.flush();
  }

  @Override
  public void doPost(HttpServletRequest request,
                     HttpServletResponse response)
      throws ServletException,IOException {
    doGet(request, response);
  }

  protected int paramAsInt(final String paramName, final SolrParams params, final int defaultVal) {
    int val = defaultVal;
    String paramS = params.get(paramName);
    if (paramS != null) {
      String trimmed = paramS.trim();
      if (trimmed.length() > 0) {
        try {
          val = Integer.parseInt(trimmed);
        } catch (NumberFormatException nfe) {
          log.warn("Invalid value "+paramS+" passed for parameter "+paramName+"; expected integer!");
        }        
      }
    }
    return val;
  }

  //--------------------------------------------------------------------------------------
  //
  //--------------------------------------------------------------------------------------

  static class ZKPrinter {
    static boolean FULLPATH_DEFAULT = false;

    boolean indent = true;
    boolean fullpath = FULLPATH_DEFAULT;
    boolean detail = false;
    boolean dump = false;
    
    String addr; // the address passed to us
    String keeperAddr; // the address we're connected to

    boolean doClose;  // close the client after done if we opened it

    final HttpServletResponse response;
    final Writer out;
    SolrZkClient zkClient;

    int level;
    int maxData = 95;
    
    PageOfCollections page;
    PagedCollectionSupport pagingSupport;
    ZkController zkController;

    public ZKPrinter(HttpServletResponse response, Writer out, ZkController controller, String addr) throws IOException {
      this.zkController = controller;
      this.response = response;
      this.out = out;
      this.addr = addr;

      if (addr == null) {
        if (controller != null) {
          // this core is zk enabled
          keeperAddr = controller.getZkServerAddress();
          zkClient = controller.getZkClient();
          if (zkClient != null && zkClient.isConnected()) {
            return;
          } else {
            // try a different client with this address
            addr = keeperAddr;
          }
        }
      }

      keeperAddr = addr;
      if (addr == null) {
        writeError(404, "Zookeeper is not configured for this Solr Core. Please try connecting to an alternate zookeeper address.");
        return;
      }

      try {
        zkClient = new SolrZkClient(addr, 10000);
        doClose = true;
      } catch (Exception e) {
        writeError(503, "Could not connect to zookeeper at '" + addr + "'\"");
        zkClient = null;
        return;
      }

    }

    public void close() {
      if (doClose) {
        zkClient.close();
      }
    }

    // main entry point
    void print(String path) throws IOException {
      if (zkClient == null) {
        return;
      }

      // normalize path
      if (path == null) {
        path = "/";
      } else {
        path = path.trim();
        if (path.length() == 0) {
          path = "/";
        }
      }

      if (path.endsWith("/") && path.length() > 1) {
        path = path.substring(0, path.length() - 1);
      }

      int idx = path.lastIndexOf('/');
      String parent = idx >= 0 ? path.substring(0, idx) : path;
      if (parent.length() == 0) {
        parent = "/";
      }

      CharArr chars = new CharArr();
      JSONWriter json = new JSONWriter(chars, 2);
      json.startObject();

      if (detail) {
        if (!printZnode(json, path)) {
          return;
        }
        json.writeValueSeparator();
      }

      json.writeString("tree");
      json.writeNameSeparator();
      json.startArray();
      if (!printTree(json, path)) {
        return; // there was an error
      }
      json.endArray();
      json.endObject();
      out.write(chars.toString());
    }

    void writeError(int code, String msg) throws IOException {
      response.setStatus(code);

      CharArr chars = new CharArr();
      JSONWriter w = new JSONWriter(chars, 2);
      w.startObject();
      w.indent();
      w.writeString("status");
      w.writeNameSeparator();
      w.write(code);
      w.writeValueSeparator();
      w.indent();
      w.writeString("error");
      w.writeNameSeparator();
      w.writeString(msg);
      w.endObject();

      out.write(chars.toString());
    }


    boolean printTree(JSONWriter json, String path) throws IOException {
      String label = path;
      if (!fullpath) {
        int idx = path.lastIndexOf('/');
        label = idx > 0 ? path.substring(idx + 1) : path;
      }
      json.startObject();
      //writeKeyValue(json, "data", label, true );
      json.writeString("data");
      json.writeNameSeparator();

      json.startObject();
      writeKeyValue(json, "title", label, true);
      json.writeValueSeparator();
      json.writeString("attr");
      json.writeNameSeparator();
      json.startObject();
      writeKeyValue(json, "href", "zookeeper?detail=true&path=" + URLEncoder.encode(path, "UTF-8"), true);
      json.endObject();
      json.endObject();

      Stat stat = new Stat();
      try {
        // Trickily, the call to zkClient.getData fills in the stat variable
        byte[] data = zkClient.getData(path, null, stat, true);

        if (stat.getEphemeralOwner() != 0) {
          writeKeyValue(json, "ephemeral", true, false);
          writeKeyValue(json, "version", stat.getVersion(), false);
        }

        if (dump) {
          json.writeValueSeparator();
          printZnode(json, path);
        }

      } catch (IllegalArgumentException e) {
        // path doesn't exist (must have been removed)
        writeKeyValue(json, "warning", "(path gone)", false);
      } catch (KeeperException e) {
        writeKeyValue(json, "warning", e.toString(), false);
        log.warn("Keeper Exception", e);
      } catch (InterruptedException e) {
        writeKeyValue(json, "warning", e.toString(), false);
        log.warn("InterruptedException", e);
      }

      if (stat.getNumChildren() > 0) {
        json.writeValueSeparator();
        if (indent) {
          json.indent();
        }
        json.writeString("children");
        json.writeNameSeparator();
        json.startArray();

        try {
          List<String> children = zkClient.getChildren(path, null, true);
          java.util.Collections.sort(children);

          boolean first = true;
          for (String child : children) {
            if (!first) {
              json.writeValueSeparator();
            }

            String childPath = path + (path.endsWith("/") ? "" : "/") + child;
            if (!printTree(json, childPath)) {
              return false;
            }
            first = false;
          }
        } catch (KeeperException e) {
          writeError(500, e.toString());
          return false;
        } catch (InterruptedException e) {
          writeError(500, e.toString());
          return false;
        } catch (IllegalArgumentException e) {
          // path doesn't exist (must have been removed)
          json.writeString("(children gone)");
        }

        json.endArray();
      }

      json.endObject();
      return true;
    }

    String time(long ms) {
      return (new Date(ms)).toString() + " (" + ms + ")";
    }

    public void writeKeyValue(JSONWriter json, String k, Object v, boolean isFirst) {
      if (!isFirst) {
        json.writeValueSeparator();
      }
      if (indent) {
        json.indent();
      }
      json.writeString(k);
      json.writeNameSeparator();
      json.write(v);
    }

    @SuppressWarnings("unchecked")
    boolean printZnode(JSONWriter json, String path) throws IOException {
      try {     
        String dataStr = null;
        String dataStrErr = null;
        Stat stat = new Stat();
        // Trickily, the call to zkClient.getData fills in the stat variable
        byte[] data = zkClient.getData(path, null, stat, true);          
        if (null != data) {
          try {
            dataStr = (new BytesRef(data)).utf8ToString();
          } catch (Exception e) {
            dataStrErr = "data is not parsable as a utf8 String: " + e.toString();
          }
        }
        // support paging of the collections graph view (in case there are many collections)
        if (page != null) {
          // we've already pulled the data for /clusterstate.json from ZooKeeper above,
          // but it needs to be parsed into a map so we can lookup collection states before
          // trying to find them in the /collections/?/state.json znode
          Map<String,Object> clusterstateJsonMap = null;
          if (dataStr != null) {
            try {
              clusterstateJsonMap = (Map<String, Object>) ObjectBuilder.fromJSON(dataStr);
            } catch (Exception e) {
              throw new SolrException(ErrorCode.SERVER_ERROR,
                  "Failed to parse /clusterstate.json from ZooKeeper due to: " + e, e);
            }
          } else {
            clusterstateJsonMap = ZkNodeProps.makeMap();
          }
          
          // fetch the requested page of collections and then retrieve the state for each 
          page = pagingSupport.fetchPage(page, zkClient);
          // keep track of how many collections match the filter
          boolean applyStatusFilter = 
              (page.filterType == FilterType.status && page.filter != null);
          List<String> matchesStatusFilter = applyStatusFilter ? new ArrayList<String>() : null;           
          Set<String> liveNodes = applyStatusFilter ? 
              zkController.getZkStateReader().getClusterState().getLiveNodes() : null;
          
          SortedMap<String,Object> collectionStates = new TreeMap<String,Object>(pagingSupport);          
          for (String collection : page.selected) {
            Object collectionState = clusterstateJsonMap.get(collection);
            if (collectionState != null) {              
              // collection state was in /clusterstate.json
              if (applyStatusFilter) {
                // verify this collection matches the status filter
                if (page.matchesStatusFilter((Map<String,Object>)collectionState,liveNodes)) {
                  matchesStatusFilter.add(collection);
                  collectionStates.put(collection, collectionState);
                }
              } else {
                collectionStates.put(collection, collectionState);                
              }              
            } else {
              // looks like an external collection ...
              String collStatePath = String.format(Locale.ROOT, "/collections/%s/state.json", collection);
              String childDataStr = null;
              try {              
                byte[] childData = zkClient.getData(collStatePath, null, null, true);
                if (childData != null)
                  childDataStr = (new BytesRef(childData)).utf8ToString();
              } catch (KeeperException.NoNodeException nne) {
                log.warn("State for collection "+collection+
                    " not found in /clusterstate.json or /collections/"+collection+"/state.json!");
              } catch (Exception childErr) {
                log.error("Failed to get "+collStatePath+" due to: "+childErr);
              }
              
              if (childDataStr != null) {
                Map<String,Object> extColl = (Map<String,Object>)ObjectBuilder.fromJSON(childDataStr);
                collectionState = extColl.get(collection);
                
                if (applyStatusFilter) {
                  // verify this collection matches the filtered state
                  if (page.matchesStatusFilter((Map<String,Object>)collectionState,liveNodes)) {
                    matchesStatusFilter.add(collection);
                    collectionStates.put(collection, collectionState);
                  }
                } else {
                  collectionStates.put(collection, collectionState);                
                }              
              }              
            }            
          }
          
          if (applyStatusFilter) {
            // update the paged navigation info after applying the status filter
            page.selectPage(matchesStatusFilter);
            
            // rebuild the Map of state data
            SortedMap<String,Object> map = new TreeMap<String,Object>(pagingSupport);                      
            for (String next : page.selected)
              map.put(next, collectionStates.get(next));
            collectionStates = map;
          }          
          
          if (collectionStates != null) {
            CharArr out = new CharArr();
            new JSONWriter(out, 2).write(collectionStates);
            dataStr = out.toString();
          }
        }

        json.writeString("znode");
        json.writeNameSeparator();
        json.startObject();

        writeKeyValue(json, "path", path, true);

        json.writeValueSeparator();
        json.writeString("prop");
        json.writeNameSeparator();
        json.startObject();
        writeKeyValue(json, "version", stat.getVersion(), true);
        writeKeyValue(json, "aversion", stat.getAversion(), false);
        writeKeyValue(json, "children_count", stat.getNumChildren(), false);
        writeKeyValue(json, "ctime", time(stat.getCtime()), false);
        writeKeyValue(json, "cversion", stat.getCversion(), false);
        writeKeyValue(json, "czxid", stat.getCzxid(), false);
        writeKeyValue(json, "ephemeralOwner", stat.getEphemeralOwner(), false);
        writeKeyValue(json, "mtime", time(stat.getMtime()), false);
        writeKeyValue(json, "mzxid", stat.getMzxid(), false);
        writeKeyValue(json, "pzxid", stat.getPzxid(), false);
        writeKeyValue(json, "dataLength", stat.getDataLength(), false);
        if (null != dataStrErr) {
          writeKeyValue(json, "dataNote", dataStrErr, false);
        }
        json.endObject();

        if (null != dataStr) {
          writeKeyValue(json, "data", dataStr, false);
        }

        if (page != null) {
          writeKeyValue(json, "paging", page.getPagingHeader(), false);
        }

        json.endObject();
      } catch (KeeperException e) {
        writeError(500, e.toString());
        return false;
      } catch (InterruptedException e) {
        writeError(500, e.toString());
        return false;
      }
      return true;
    }
  }
}
