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
package org.apache.solr.metrics.rrd;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.rrd4j.core.RrdBackend;
import org.rrd4j.core.RrdBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RRD backend factory using Solr documents as underlying storage.
 * <p>RRD databases are identified by paths in the format <code>solr:dbName</code>.
 * Typically the path will correspond to the name of metric or a group of metrics, eg:
 * <code>solr:QUERY./select.requests</code></p>
 * <p>NOTE: Solr doesn't register instances of this factory in the static
 * registry {@link RrdBackendFactory#registerFactory(RrdBackendFactory)} because
 * it's then impossible to manage its life-cycle.</p>
 *
 * @deprecated this functionality will be removed in Solr 9.0
 */
public class SolrRrdBackendFactory extends RrdBackendFactory implements SolrCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int DEFAULT_SYNC_PERIOD = 60;
  public static final int DEFAULT_MAX_DBS = 500;

  public static final String NAME = "SOLR";
  public static final String URI_PREFIX = "solr:";
  public static final String ID_SEP = "|";
  public static final String ID_PREFIX = "rrd";
  public static final String DOC_TYPE = "metrics_rrd";

  public static final String DATA_FIELD = "data_bin";

  private final SolrClient solrClient;
  private final TimeSource timeSource;
  private final String collection;
  private final int syncPeriod;
  private final int idPrefixLength;
  private ScheduledThreadPoolExecutor syncService;
  private volatile boolean closed = false;
  private volatile boolean persistent = true;

  private final Map<String, SolrRrdBackend> backends = new ConcurrentHashMap<>();

  /**
   * Create a factory.
   * @param solrClient SolrClient to use
   * @param collection collection name where documents are stored (typically this is
   *                   {@link CollectionAdminParams#SYSTEM_COLL})
   * @param syncPeriod synchronization period in seconds - how often modified
   *                   databases are stored as updated Solr documents
   * @param timeSource time source
   */
  public SolrRrdBackendFactory(SolrClient solrClient, String collection, int syncPeriod, TimeSource timeSource) {
    this.solrClient = solrClient;
    this.timeSource = timeSource;
    this.collection = collection;
    this.syncPeriod = syncPeriod;
    if (log.isDebugEnabled()) {
      log.debug("Created {}", hashCode());
    }
    this.idPrefixLength = ID_PREFIX.length() + ID_SEP.length();
    syncService = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2,
        new SolrNamedThreadFactory("SolrRrdBackendFactory"));
    syncService.setRemoveOnCancelPolicy(true);
    syncService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    syncService.scheduleWithFixedDelay(() -> maybeSyncBackends(),
        timeSource.convertDelay(TimeUnit.SECONDS, syncPeriod, TimeUnit.MILLISECONDS),
        timeSource.convertDelay(TimeUnit.SECONDS, syncPeriod, TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  public TimeSource getTimeSource() {
    return timeSource;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Factory already closed");
    }
  }

  @Override
  public boolean canStore(URI uri) {
    if (uri == null) {
      return false;
    }
    if (uri.getScheme().toUpperCase(Locale.ROOT).equals(getName())) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String getPath(URI uri) {
    return uri.getSchemeSpecificPart();
  }

  @Override
  public URI getUri(String path) {
    if (!path.startsWith(URI_PREFIX)) {
      path = URI_PREFIX + path;
    }
    try {
      return new URI(path);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid path: " + path);
    }
  }

  @Override
  public URI getCanonicalUri(URI uri) {
    return uri;
  }

//  @Override
//  protected URI getRootUri() {
//    try {
//      return new URI("solr", null, null, null);
//    } catch (URISyntaxException e) {
//      throw new RuntimeException("Impossible error", e);
//    }
//  }
//
  /**
   * Open (or get) a backend.
   * @param path backend path (without URI scheme)
   * @param readOnly if true then the backend will never be synchronized to Solr,
   *                 and updates will be silently ignored. Read-only backends can
   *                 be safely closed and discarded after use.
   * @return an instance of Solr backend.
   * @throws IOException on Solr error when retrieving existing data
   */
  @Override
  protected synchronized RrdBackend open(String path, boolean readOnly) throws IOException {
    ensureOpen();
    SolrRrdBackend backend = backends.computeIfAbsent(path, p -> new SolrRrdBackend(p, readOnly, this));
    if (backend.isReadOnly()) {
      if (readOnly) {
        return backend;
      } else {
        // replace it with a writable one
        backend = new SolrRrdBackend(path, readOnly, this);
        backends.put(path, backend);
        return backend;
      }
    } else {
      if (readOnly) {
        // return a throwaway unregistered read-only copy
        return new SolrRrdBackend(backend);
      } else {
        return backend;
      }
    }
  }

  SolrRrdBackend.SyncData getData(String path) throws IOException {
    if (!persistent) {
      return null;
    }
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CommonParams.Q, "{!term f=id}" + ID_PREFIX + ID_SEP + path);
      params.add(CommonParams.FQ, CommonParams.TYPE + ":" + DOC_TYPE);
      QueryResponse rsp = solrClient.query(collection, params);
      SolrDocumentList docs = rsp.getResults();
      if (docs == null || docs.isEmpty()) {
        return null;
      }
      if (docs.size() > 1) {
        throw new SolrServerException("Expected at most 1 doc with id '" + path + "' but got " + docs);
      }
      SolrDocument doc = docs.get(0);
      Object o = doc.getFieldValue(DATA_FIELD);
      if (o == null) {
        return null;
      }
      if (o instanceof byte[]) {
        Object timeObj = doc.getFieldValue("timestamp_l");
        Long time = timeObj instanceof Number ? ((Number)timeObj).longValue() : Long.parseLong(String.valueOf(timeObj));
        return new SolrRrdBackend.SyncData((byte[])o, time);
      } else {
        throw new SolrServerException("Unexpected value of '" + DATA_FIELD + "' field: " + o.getClass().getName() + ": " + o);
      }
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  void unregisterBackend(String path) {
    backends.remove(path);
  }

  private static final class DbComparator implements Comparator<Pair<String, Long>> {
    static final DbComparator INSTANCE = new DbComparator();

    @Override
    public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
      return o1.first().compareTo(o2.first());
    }
  }

  /**
   * List all available databases created by this node name
   * @param maxLength maximum number of results to return
   * @return list of database names and their last update times, or empty
   * @throws IOException on server errors
   */
  public List<Pair<String, Long>> list(int maxLength) throws IOException {
    Map<String, Pair<String, Long>> byName = new HashMap<>();
    if (persistent) {
      try {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add(CommonParams.Q, "*:*");
        params.add(CommonParams.FQ, CommonParams.TYPE + ":" + DOC_TYPE);
        params.add(CommonParams.FL, "id,timestamp_l");
        params.add(CommonParams.ROWS, String.valueOf(maxLength));
        QueryResponse rsp = solrClient.query(collection, params);
        SolrDocumentList docs = rsp.getResults();
        if (docs != null) {
          docs.forEach(d -> {
            Object o = d.getFieldValue("timestamp_l");
            if (o == null) {
              return;
            }
            Long time = o instanceof Number ? ((Number)o).longValue() : Long.parseLong(String.valueOf(o));
            Pair<String, Long> p = new Pair<>(((String)d.getFieldValue("id")).substring(idPrefixLength), time);
            byName.put(p.first(), p);
          });
        }
      } catch (SolrServerException e) {
        log.warn("Error retrieving RRD list", e);
      }
    }
    // add in-memory backends not yet stored, or replace with more recent versions
    backends.forEach((name, db) -> {
      long lastModifiedTime = db.getLastModifiedTime();
      Pair<String, Long> stored = byName.get(name);
      Pair<String, Long> inMemory = new Pair<>(name, lastModifiedTime);
      if (stored != null) {
        if (stored.second() < lastModifiedTime) {
          byName.put(name, inMemory);
        }
      } else {
        byName.put(name, inMemory);
      }
    });
    ArrayList<Pair<String, Long>> list = new ArrayList<>(byName.values());
    Collections.sort(list, DbComparator.INSTANCE);
    return list;
  }

  /**
   * Remove all databases created by this node name.
   * @throws IOException on server error
   */
  public void removeAll() throws IOException {
    for (Iterator<SolrRrdBackend> it = backends.values().iterator(); it.hasNext(); ) {
      SolrRrdBackend backend = it.next();
      it.remove();
      IOUtils.closeQuietly(backend);
    }
    if (!persistent) {
      return;
    }
    // remove all Solr docs
    try {
      solrClient.deleteByQuery(collection,
          "{!term f=" + CommonParams.TYPE + "}:" + DOC_TYPE, syncPeriod * 1000);
    } catch (SolrServerException e) {
      log.warn("Error deleting RRDs", e);
    }
  }

  /**
   * Remove a database.
   * @param path database path.
   * @throws IOException on Solr exception
   */
  public void remove(String path) throws IOException {
    SolrRrdBackend backend = backends.remove(path);
    if (backend != null) {
      IOUtils.closeQuietly(backend);
    }
    if (!persistent) {
      return;
    }
    // remove Solr doc
    try {
      solrClient.deleteByQuery(collection, "{!term f=id}" + ID_PREFIX + ID_SEP + path);
    } catch (SolrServerException | SolrException e) {
      log.warn("Error deleting RRD for path {}", path, e);
    }
  }

  synchronized void maybeSyncBackends() {
    if (closed) {
      return;
    }
    if (!persistent) {
      return;
    }
    if (Thread.interrupted()) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("-- maybe sync backends: {}", backends.keySet());
    }
    Map<String, SolrRrdBackend.SyncData> syncDatas = new HashMap<>();
    backends.forEach((path, backend) -> {
      SolrRrdBackend.SyncData syncData = backend.getSyncDataAndMarkClean();
      if (syncData != null) {
        syncDatas.put(backend.getPath(), syncData);
      }
    });
    if (syncDatas.isEmpty()) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("-- syncing {}", syncDatas.keySet());
    }
    // write updates
    try {
      syncDatas.forEach((path, syncData) -> {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", ID_PREFIX + ID_SEP + path);
        doc.addField(CommonParams.TYPE, DOC_TYPE);
        doc.addField(DATA_FIELD, syncData.data);
        doc.setField("timestamp_l", syncData.timestamp);
        try {
          solrClient.add(collection, doc);
        } catch (SolrServerException | IOException e) {
          log.warn("Error updating RRD data for {}", path, e);
        }
      });
      if (Thread.interrupted()) {
        return;
      }
      try {
        solrClient.commit(collection);
      } catch (SolrServerException e) {
        log.warn("Error committing RRD data updates", e);
      }
    } catch (IOException e) {
      log.warn("Error sending RRD data updates", e);
    }
  }

  /**
   * Check for existence of a backend.
   * @param path backend path, without the URI scheme
   * @return true when a backend exists. Note that a backend may exist only
   * in memory if it was created recently within {@link #syncPeriod}.
   * @throws IOException on Solr exception
   */
  @Override
  public boolean exists(String path) throws IOException {
    // check in-memory backends first
    if (backends.containsKey(path)) {
      return true;
    }
    if (!persistent) {
      return false;
    }
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CommonParams.Q, "{!term f=id}" + ID_PREFIX + ID_SEP + path);
      params.add(CommonParams.FQ, CommonParams.TYPE + ":" + DOC_TYPE);
      params.add(CommonParams.FL, "id");
      QueryResponse rsp = solrClient.query(collection, params);
      SolrDocumentList docs = rsp.getResults();
      if (docs == null || docs.isEmpty()) {
        return false;
      }
      if (docs.size() > 1) {
        throw new SolrServerException("Expected at most 1 doc with id '" + path + "' but got " + docs);
      }
      return true;
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  public boolean isPersistent() {
    return persistent;
  }

  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }

  @Override
  protected boolean shouldValidateHeader(String path) throws IOException {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Closing {}", hashCode());
    }
    closed = true;
    backends.values().forEach(IOUtils::closeQuietly);
    backends.clear();
    ExecutorUtil.shutdownNowAndAwaitTermination(syncService);
    syncService = null;
  }
}
