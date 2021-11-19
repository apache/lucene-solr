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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.SimplePostTool.BAOS;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.OMIT_HEADER;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CommonParams.WT;


/**
 * Zookeeper Info
 *
 * @since solr 4.0
 */
public final class ZookeeperInfoHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final String PARAM_DETAIL = "detail";
  private final CoreContainer cores;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // used for custom sorting collection names looking like prefix##
  // only go out to 7 digits (which safely fits in an int)
  private static final Pattern endsWithDigits = Pattern.compile("^(\\D*)(\\d{1,7}?)$");

  public ZookeeperInfoHandler(CoreContainer cc) {
    this.cores = cc;
  }


  @Override
  public String getDescription() {
    return "Fetch Zookeeper contents";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  /**
   * Enumeration of ways to filter collections on the graph panel.
   */
  static enum FilterType {
    none, name, status
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    SolrParams params = request.getParams();
    String path = params.get(PATH, "");
    String detail = params.get(PARAM_DETAIL, "false");
    if ("/security.json".equalsIgnoreCase(path) && "true".equalsIgnoreCase(detail)) {
      return PermissionNameProvider.Name.SECURITY_READ_PERM;
    } else {
      return PermissionNameProvider.Name.ZK_READ_PERM;
    }
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

        int lastIndex = Math.min(start + rows, numFound);
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
          ? filter.substring(0, filter.length() - 1) + ".*" : filter;

      // case-insensitive
      if (!regexFilter.startsWith("(?i)"))
        regexFilter = "(?i)" + regexFilter;

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
    final boolean matchesStatusFilter(Map<String, Object> collectionState, Set<String> liveNodes) {

      if (filterType != FilterType.status || filter == null || filter.length() == 0)
        return true; // no status filter, so all match

      boolean isHealthy = true; // means all replicas for all shards active
      boolean hasDownedShard = false; // means one or more shards is down
      boolean replicaInRecovery = false;

      Map<String, Object> shards = (Map<String, Object>) collectionState.get(DocCollection.SHARDS);
      for (Object o : shards.values()) {
        boolean hasActive = false;
        Map<String, Object> shard = (Map<String, Object>) o;
        Map<String, Object> replicas = (Map<String, Object>) shard.get(Slice.REPLICAS);
        for (Object value : replicas.values()) {
          Map<String, Object> replicaState = (Map<String, Object>) value;
          Replica.State coreState = Replica.State.getState((String) replicaState.get(ZkStateReader.STATE_PROP));
          String nodeName = (String) replicaState.get(ZkStateReader.NODE_NAME_PROP);

          // state can lie to you if the node is offline, so need to reconcile with live_nodes too
          if (!liveNodes.contains(nodeName))
            coreState = Replica.State.DOWN; // not on a live node, so must be down

          if (coreState == Replica.State.ACTIVE) {
            hasActive = true; // assumed no replicas active and found one that is for this shard
          } else {
            if (coreState == Replica.State.RECOVERING) {
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
      } else if (Replica.State.getState(filter) == Replica.State.RECOVERING) {
        return !isHealthy && replicaInRecovery;
      }

      return true;
    }

    final boolean matches(final Pattern filter, final String collName) {
      return filter.matcher(collName).matches();
    }

    String getPagingHeader() {
      return start + "|" + rows + "|" + numFound + "|" + (filterType != null ? filterType.toString() : "") + "|" + (filter != null ? filter : "");
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
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
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
  @SuppressWarnings({"unchecked"})
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final SolrParams params = req.getParams();
    Map<String, String> map = new HashMap<>(1);
    map.put(WT, "raw");
    map.put(OMIT_HEADER, "true");
    req.setParams(SolrParams.wrapDefaults(new MapSolrParams(map), params));
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

    String path = params.get(PATH);

    if (params.get("addr") != null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Illegal parameter \"addr\"");
    }

    String detailS = params.get(PARAM_DETAIL);
    boolean detail = detailS != null && detailS.equals("true");

    String dumpS = params.get("dump");
    boolean dump = dumpS != null && dumpS.equals("true");

    int start = params.getInt("start", 0);
    int rows = params.getInt("rows", -1);

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

    ZKPrinter printer = new ZKPrinter(cores.getZkController());
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
    rsp.getValues().add(RawResponseWriter.CONTENT,printer);
  }

  //--------------------------------------------------------------------------------------
  //
  //--------------------------------------------------------------------------------------

  static class ZKPrinter implements ContentStream {
    static boolean FULLPATH_DEFAULT = false;

    boolean indent = true;
    boolean fullpath = FULLPATH_DEFAULT;
    boolean detail = false;
    boolean dump = false;

    String keeperAddr; // the address we're connected to

    final BAOS baos = new BAOS();
    final Writer out = new OutputStreamWriter(baos,  StandardCharsets.UTF_8);
    SolrZkClient zkClient;

    PageOfCollections page;
    PagedCollectionSupport pagingSupport;
    ZkController zkController;

    public ZKPrinter(ZkController controller) throws IOException {
      this.zkController = controller;
      keeperAddr = controller.getZkServerAddress();
      zkClient = controller.getZkClient();
    }

    public void close() {
      try {
        out.flush();
      } catch (Exception e) {
        throw new RuntimeException(e);
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
      throw new SolrException(ErrorCode.getErrorCode(code), msg);
      /*response.setStatus(code);

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

      out.write(chars.toString());*/
    }


    boolean printTree(JSONWriter json, String path) throws IOException {
      String label = path;
      if (!fullpath) {
        int idx = path.lastIndexOf('/');
        label = idx > 0 ? path.substring(idx + 1) : path;
      }
      json.startObject();
      writeKeyValue(json, "text", label, true);
      json.writeValueSeparator();
      json.writeString("a_attr");
      json.writeNameSeparator();
      json.startObject();
      String href = "admin/zookeeper?detail=true&path=" + URLEncoder.encode(path, "UTF-8");
      writeKeyValue(json, "href", href, true);
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
          Map<String, Object> clusterstateJsonMap = null;
          if (dataStr != null) {
            try {
              clusterstateJsonMap = (Map<String, Object>) Utils.fromJSONString(dataStr);
            } catch (Exception e) {
              throw new SolrException(ErrorCode.SERVER_ERROR,
                  "Failed to parse /clusterstate.json from ZooKeeper due to: " + e, e);
            }
          } else {
            clusterstateJsonMap = Utils.makeMap();
          }

          // fetch the requested page of collections and then retrieve the state for each 
          page = pagingSupport.fetchPage(page, zkClient);
          // keep track of how many collections match the filter
          boolean applyStatusFilter =
              (page.filterType == FilterType.status && page.filter != null);
          List<String> matchesStatusFilter = applyStatusFilter ? new ArrayList<>() : null;
          ClusterState cs = zkController.getZkStateReader().getClusterState();
          Set<String> liveNodes = applyStatusFilter ? cs.getLiveNodes() : null;

          SortedMap<String, Object> collectionStates = new TreeMap<String, Object>(pagingSupport);
          for (String collection : page.selected) {
            Object collectionState = clusterstateJsonMap.get(collection);
            if (collectionState != null) {
              // collection state was in /clusterstate.json
              if (applyStatusFilter) {
                // verify this collection matches the status filter
                if (page.matchesStatusFilter((Map<String, Object>) collectionState, liveNodes)) {
                  matchesStatusFilter.add(collection);
                  collectionStates.put(collection, ClusterStatus.postProcessCollectionJSON((Map<String, Object>) collectionState));
                }
              } else {
                collectionStates.put(collection, ClusterStatus.postProcessCollectionJSON((Map<String, Object>) collectionState));
              }
            } else {
              // looks like an external collection ... just use DocCollection instead of reading ZK data directly to work with per replica states
              DocCollection dc = cs.getCollectionOrNull(collection);
              if (dc != null) {
                // TODO: for collections with perReplicaState, a ser/deser to JSON was needed to get the state to render correctly for the UI?
                collectionState = dc.isPerReplicaState() ? Utils.fromJSONString(Utils.toJSONString(dc)) : dc.getProperties();
                if (applyStatusFilter) {
                  // verify this collection matches the filtered state
                  if (page.matchesStatusFilter((Map<String, Object>) collectionState, liveNodes)) {
                    matchesStatusFilter.add(collection);
                    collectionStates.put(collection, ClusterStatus.postProcessCollectionJSON((Map<String, Object>) collectionState));
                  }
                } else {
                  collectionStates.put(collection, ClusterStatus.postProcessCollectionJSON((Map<String, Object>) collectionState));
                }
              }
            }
          }

          if (applyStatusFilter) {
            // update the paged navigation info after applying the status filter
            page.selectPage(matchesStatusFilter);

            // rebuild the Map of state data
            SortedMap<String, Object> map = new TreeMap<String, Object>(pagingSupport);
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

        writeKeyValue(json, PATH, path, true);

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

   /* @Override
    public void write(OutputStream os) throws IOException {
      ByteBuffer bytes = baos.getByteBuffer();
      os.write(bytes.array(),0,bytes.limit());
    }
*/
    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getSourceInfo() {
      return null;
    }

    @Override
    public String getContentType() {
      return JSONResponseWriter.CONTENT_TYPE_JSON_UTF8;
    }

    @Override
    public Long getSize() {
      return null;
    }

    @Override
    public InputStream getStream() throws IOException {
      return new ByteBufferInputStream(baos.getByteBuffer());
    }

    @Override
    public Reader getReader() throws IOException {
      return null;
    }
  }
}
