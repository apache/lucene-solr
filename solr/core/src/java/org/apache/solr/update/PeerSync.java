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

package org.apache.solr.update;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase.FROMLEADER;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.experimental */
public class PeerSync  {
  public static Logger log = LoggerFactory.getLogger(PeerSync.class);
  public boolean debug = log.isDebugEnabled();

  private List<String> replicas;
  private int nUpdates;
  private int maxUpdates;  // maximum number of updates to request before failing

  private UpdateHandler uhandler;
  private UpdateLog ulog;
  private HttpShardHandlerFactory shardHandlerFactory;
  private ShardHandler shardHandler;

  private UpdateLog.RecentUpdates recentUpdates;
  private List<Long> startingVersions;

  private List<Long> ourUpdates;
  private Set<Long> ourUpdateSet;
  private Set<Long> requestedUpdateSet;
  private long ourLowThreshold;  // 20th percentile
  private long ourHighThreshold; // 80th percentile
  private boolean cantReachIsSuccess;
  private boolean getNoVersionsIsSuccess;
  private final HttpClient client;

  // comparator that sorts by absolute value, putting highest first
  private static Comparator<Long> absComparator = new Comparator<Long>() {
    @Override
    public int compare(Long o1, Long o2) {
      long l1 = Math.abs(o1);
      long l2 = Math.abs(o2);
      if (l1 >l2) return -1;
      if (l1 < l2) return 1;
      return 0;
    }
  };

  // comparator that sorts update records by absolute value of version, putting lowest first
  private static Comparator<Object> updateRecordComparator = new Comparator<Object>() {
    @Override
    public int compare(Object o1, Object o2) {
      if (!(o1 instanceof List)) return 1;
      if (!(o2 instanceof List)) return -1;

      List lst1 = (List)o1;
      List lst2 = (List)o2;

      long l1 = Math.abs((Long)lst1.get(1));
      long l2 = Math.abs((Long)lst2.get(1));

      if (l1 >l2) return 1;
      if (l1 < l2) return -1;
      return 0;
    }
  };


  private static class SyncShardRequest extends ShardRequest {
    List<Long> reportedVersions;
    List<Long> requestedUpdates;
    Exception updateException;
  }

  public PeerSync(SolrCore core, List<String> replicas, int nUpdates) {
    this(core, replicas, nUpdates, false, true);
  }
  
  public PeerSync(SolrCore core, List<String> replicas, int nUpdates, boolean cantReachIsSuccess, boolean getNoVersionsIsSuccess) {
    this.replicas = replicas;
    this.nUpdates = nUpdates;
    this.maxUpdates = nUpdates;
    this.cantReachIsSuccess = cantReachIsSuccess;
    this.getNoVersionsIsSuccess = getNoVersionsIsSuccess;
    this.client = core.getCoreDescriptor().getCoreContainer().getUpdateShardHandler().getHttpClient();
    
    uhandler = core.getUpdateHandler();
    ulog = uhandler.getUpdateLog();
    // TODO: shutdown
    shardHandlerFactory = new HttpShardHandlerFactory();
    shardHandler = shardHandlerFactory.getShardHandler(client);
  }

  /** optional list of updates we had before possibly receiving new updates */
  public void setStartingVersions(List<Long> startingVersions) {
    this.startingVersions = startingVersions;
  }

  public long percentile(List<Long> arr, float frac) {
    int elem = (int) (arr.size() * frac);
    return Math.abs(arr.get(elem));
  }

  // start of peersync related debug messages.  includes the core name for correlation.
  private String msg() {
    ZkController zkController = uhandler.core.getCoreDescriptor().getCoreContainer().getZkController();

    String myURL = "";

    if (zkController != null) {
      myURL = zkController.getBaseUrl();
    }

    // TODO: core name turns up blank in many tests - find URL if cloud enabled?
    return "PeerSync: core="+uhandler.core.getName()+ " url="+myURL +" ";
  }

  /** Returns true if peer sync was successful, meaning that this core may not be considered to have the latest updates
   *  when considering the last N updates between it and it's peers.
   *  A commit is not performed.
   */
  public boolean sync() {
    if (ulog == null) {
      return false;
    }

    log.info(msg() + "START replicas=" + replicas + " nUpdates=" + nUpdates);

    // TODO: does it ever make sense to allow sync when buffering or applying buffered?  Someone might request that we do it...
    if (!(ulog.getState() == UpdateLog.State.ACTIVE || ulog.getState()==UpdateLog.State.REPLAYING)) {
      log.error(msg() + "ERROR, update log not in ACTIVE or REPLAY state. " + ulog);
      // return false;
    }
    
    if (debug) {
      if (startingVersions != null) {
        log.debug(msg() + "startingVersions=" + startingVersions.size() + " " + startingVersions);
      }
    }

    // Fire off the requests before getting our own recent updates (for better concurrency)
    // This also allows us to avoid getting updates we don't need... if we got our updates and then got their updates, they would
    // have newer stuff that we also had (assuming updates are going on and are being forwarded).
    for (String replica : replicas) {
      requestVersions(replica);
    }

    recentUpdates = ulog.getRecentUpdates();
    try {
      ourUpdates = recentUpdates.getVersions(nUpdates);
    } finally {
      recentUpdates.close();
    }

    Collections.sort(ourUpdates, absComparator);

    if (startingVersions != null) {
      if (startingVersions.size() == 0) {
        log.warn("no frame of reference to tell if we've missed updates");
        return false;
      }
      Collections.sort(startingVersions, absComparator);

      ourLowThreshold = percentile(startingVersions, 0.8f);
      ourHighThreshold = percentile(startingVersions, 0.2f);

      // now make sure that the starting updates overlap our updates
      // there shouldn't be reorders, so any overlap will do.

      long smallestNewUpdate = Math.abs(ourUpdates.get(ourUpdates.size()-1));

      if (Math.abs(startingVersions.get(0)) < smallestNewUpdate) {
        log.warn(msg() + "too many updates received since start - startingUpdates no longer overlaps with our currentUpdates");
        return false;
      }

      // let's merge the lists
      List<Long> newList = new ArrayList<Long>(ourUpdates);
      for (Long ver : startingVersions) {
        if (Math.abs(ver) < smallestNewUpdate) {
          newList.add(ver);
        }
      }

      ourUpdates = newList;
    }  else {

      if (ourUpdates.size() > 0) {
        ourLowThreshold = percentile(ourUpdates, 0.8f);
        ourHighThreshold = percentile(ourUpdates, 0.2f);
      }  else {
        // we have no versions and hence no frame of reference to tell if we can use a peers
        // updates to bring us into sync
        log.info(msg() + "DONE.  We have no versions.  sync failed.");
        return false;
      }
    }

    ourUpdateSet = new HashSet<Long>(ourUpdates);
    requestedUpdateSet = new HashSet<Long>(ourUpdates);

    for(;;) {
      ShardResponse srsp = shardHandler.takeCompletedOrError();
      if (srsp == null) break;
      boolean success = handleResponse(srsp);
      if (!success) {
        log.info(msg() +  "DONE. sync failed");
        shardHandler.cancelAll();
        return false;
      }
    }

    log.info(msg() +  "DONE. sync succeeded");
    return true;
  }
  
  private void requestVersions(String replica) {
    SyncShardRequest sreq = new SyncShardRequest();
    sreq.purpose = 1;
    // TODO: this sucks
    if (replica.startsWith("http://"))
      replica = replica.substring(7);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set("distrib",false);
    sreq.params.set("getVersions",nUpdates);
    shardHandler.submit(sreq, replica, sreq.params);
  }

  private boolean handleResponse(ShardResponse srsp) {
    ShardRequest sreq = srsp.getShardRequest();

    if (srsp.getException() != null) {

      // TODO: look at this more thoroughly - we don't want
      // to fail on connection exceptions, but it may make sense
      // to determine this based on the number of fails
      //
      // If the replica went down between asking for versions and asking for specific updates, that
      // shouldn't be treated as success since we counted on getting those updates back (and avoided
      // redundantly asking other replicas for them).
      if (cantReachIsSuccess && sreq.purpose == 1 && srsp.getException() instanceof SolrServerException) {
        Throwable solrException = ((SolrServerException) srsp.getException())
            .getRootCause();
        if (solrException instanceof ConnectException || solrException instanceof ConnectTimeoutException
            || solrException instanceof NoHttpResponseException || solrException instanceof SocketException) {
          log.warn(msg() + " couldn't connect to " + srsp.getShardAddress() + ", counting as success");

          return true;
        }
      }
      
      if (cantReachIsSuccess && sreq.purpose == 1 && srsp.getException() instanceof SolrException && ((SolrException) srsp.getException()).code() == 503) {
        log.warn(msg() + " got a 503 from " + srsp.getShardAddress() + ", counting as success");
        return true;
      }
      
      if (cantReachIsSuccess && sreq.purpose == 1 && srsp.getException() instanceof SolrException && ((SolrException) srsp.getException()).code() == 404) {
        log.warn(msg() + " got a 404 from " + srsp.getShardAddress() + ", counting as success");
        return true;
      }
      // TODO: at least log???
      // srsp.getException().printStackTrace(System.out);
     
      log.warn(msg() + " exception talking to " + srsp.getShardAddress() + ", failed", srsp.getException());
      
      return false;
    }

    if (sreq.purpose == 1) {
      return handleVersions(srsp);
    } else {
      return handleUpdates(srsp);
    }
  }
  
  private boolean handleVersions(ShardResponse srsp) {
    // we retrieved the last N updates from the replica
    List<Long> otherVersions = (List<Long>)srsp.getSolrResponse().getResponse().get("versions");
    // TODO: how to handle short lists?

    SyncShardRequest sreq = (SyncShardRequest) srsp.getShardRequest();
    sreq.reportedVersions =  otherVersions;

    log.info(msg() + " Received " + otherVersions.size() + " versions from " + sreq.shards[0] );

    if (otherVersions.size() == 0) {
      return getNoVersionsIsSuccess; 
    }
    
    boolean completeList = otherVersions.size() < nUpdates;  // do we have their complete list of updates?

    Collections.sort(otherVersions, absComparator);

    if (debug) {
      log.debug(msg() + " sorted versions from " + sreq.shards[0] + " = " + otherVersions);
    }
    
    long otherHigh = percentile(otherVersions, .2f);
    long otherLow = percentile(otherVersions, .8f);

    if (ourHighThreshold < otherLow) {
      // Small overlap between version windows and ours is older
      // This means that we might miss updates if we attempted to use this method.
      // Since there exists just one replica that is so much newer, we must
      // fail the sync.
      log.info(msg() + " Our versions are too old. ourHighThreshold="+ourHighThreshold + " otherLowThreshold="+otherLow);
      return false;
    }

    if (ourLowThreshold > otherHigh) {
      // Small overlap between windows and ours is newer.
      // Using this list to sync would result in requesting/replaying results we don't need
      // and possibly bringing deleted docs back to life.
      log.info(msg() + " Our versions are newer. ourLowThreshold="+ourLowThreshold + " otherHigh="+otherHigh);
      return true;
    }
    
    List<Long> toRequest = new ArrayList<Long>();
    for (Long otherVersion : otherVersions) {
      // stop when the entries get old enough that reorders may lead us to see updates we don't need
      if (!completeList && Math.abs(otherVersion) < ourLowThreshold) break;

      if (ourUpdateSet.contains(otherVersion) || requestedUpdateSet.contains(otherVersion)) {
        // we either have this update, or already requested it
        // TODO: what if the shard we previously requested this from returns failure (because it goes
        // down)
        continue;
      }

      toRequest.add(otherVersion);
      requestedUpdateSet.add(otherVersion);
    }

    sreq.requestedUpdates = toRequest;
    
    if (toRequest.isEmpty()) {
      log.info(msg() + " Our versions are newer. ourLowThreshold="+ourLowThreshold + " otherHigh="+otherHigh);

      // we had (or already requested) all the updates referenced by the replica
      return true;
    }
    
    if (toRequest.size() > maxUpdates) {
      log.info(msg() + " Failing due to needing too many updates:" + maxUpdates);
      return false;
    }

    return requestUpdates(srsp, toRequest);
  }

  private boolean requestUpdates(ShardResponse srsp, List<Long> toRequest) {
    String replica = srsp.getShardRequest().shards[0];

    log.info(msg() + "Requesting updates from " + replica + "n=" + toRequest.size() + " versions=" + toRequest);

    // reuse our original request object
    ShardRequest sreq = srsp.getShardRequest();

    sreq.purpose = 0;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set("distrib",false);
    sreq.params.set("getUpdates", StrUtils.join(toRequest, ','));
    sreq.responses.clear();  // needs to be zeroed for correct correlation to occur

    shardHandler.submit(sreq, sreq.shards[0], sreq.params);

    return true;
  }


  private boolean handleUpdates(ShardResponse srsp) {
    // we retrieved the last N updates from the replica
    List<Object> updates = (List<Object>)srsp.getSolrResponse().getResponse().get("updates");

    SyncShardRequest sreq = (SyncShardRequest) srsp.getShardRequest();
    if (updates.size() < sreq.requestedUpdates.size()) {
      log.error(msg() + " Requested " + sreq.requestedUpdates.size() + " updates from " + sreq.shards[0] + " but retrieved " + updates.size());
      return false;
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(DISTRIB_UPDATE_PARAM, FROMLEADER.toString());
    params.set("peersync",true); // debugging
    SolrQueryRequest req = new LocalSolrQueryRequest(uhandler.core, params);
    SolrQueryResponse rsp = new SolrQueryResponse();

    UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessingChain(null);
    UpdateRequestProcessor proc = processorChain.createProcessor(req, rsp);

    Collections.sort(updates, updateRecordComparator);

    Object o = null;
    long lastVersion = 0;
    try {
      // Apply oldest updates first
      for (Object obj : updates) {
        // should currently be a List<Oper,Ver,Doc/Id>
        o = obj;
        List<Object> entry = (List<Object>)o;

        if (debug) {
          log.debug(msg() + "raw update record " + o);
        }

        int oper = (Integer)entry.get(0) & UpdateLog.OPERATION_MASK;
        long version = (Long) entry.get(1);
        if (version == lastVersion && version != 0) continue;
        lastVersion = version;

        switch (oper) {
          case UpdateLog.ADD:
          {
            // byte[] idBytes = (byte[]) entry.get(2);
            SolrInputDocument sdoc = (SolrInputDocument)entry.get(entry.size()-1);
            AddUpdateCommand cmd = new AddUpdateCommand(req);
            // cmd.setIndexedId(new BytesRef(idBytes));
            cmd.solrDoc = sdoc;
            cmd.setVersion(version);
            cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
            if (debug) {
              log.debug(msg() + "add " + cmd);
            }
            proc.processAdd(cmd);
            break;
          }
          case UpdateLog.DELETE:
          {
            byte[] idBytes = (byte[]) entry.get(2);
            DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
            cmd.setIndexedId(new BytesRef(idBytes));
            cmd.setVersion(version);
            cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
            if (debug) {
              log.debug(msg() + "delete " + cmd);
            }
            proc.processDelete(cmd);
            break;
          }

          case UpdateLog.DELETE_BY_QUERY:
          {
            String query = (String)entry.get(2);
            DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
            cmd.query = query;
            cmd.setVersion(version);
            cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
            if (debug) {
              log.debug(msg() + "deleteByQuery " + cmd);
            }
            proc.processDelete(cmd);
            break;
          }

          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
        }

      }

    }
    catch (IOException e) {
      // TODO: should this be handled separately as a problem with us?
      // I guess it probably already will by causing replication to be kicked off.
      sreq.updateException = e;
      log.error(msg() + "Error applying updates from " + sreq.shards + " ,update=" + o, e);
      return false;
    }
    catch (Exception e) {
      sreq.updateException = e;
      log.error(msg() + "Error applying updates from " + sreq.shards + " ,update=" + o, e);
      return false;
    }
    finally {
      try {
        proc.finish();
      } catch (Exception e) {
        sreq.updateException = e;
        log.error(msg() + "Error applying updates from " + sreq.shards + " ,finish()", e);
        return false;
      }
    }

    return true;
  }



  /** Requests and applies recent updates from peers */
  public static void sync(SolrCore core, List<String> replicas, int nUpdates) {
    ShardHandlerFactory shardHandlerFactory = core.getCoreDescriptor().getCoreContainer().getShardHandlerFactory();

    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
   
    for (String replica : replicas) {
      ShardRequest sreq = new ShardRequest();
      sreq.shards = new String[]{replica};
      sreq.params = new ModifiableSolrParams();
      sreq.params.set("qt","/get");
      sreq.params.set("distrib", false);
      sreq.params.set("getVersions",nUpdates);
      shardHandler.submit(sreq, replica, sreq.params);
    }
    
    for (String replica : replicas) {
      ShardResponse srsp = shardHandler.takeCompletedOrError();
    }

  }
  
}
