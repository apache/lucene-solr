/**
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

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.experimental */
public class PeerSync  {
  public static Logger log = LoggerFactory.getLogger(PeerSync.class);
  public boolean debug = log.isDebugEnabled();

  private List<String> replicas;
  private int nUpdates;

  private UpdateHandler uhandler;
  private UpdateLog ulog;
  private ShardHandlerFactory shardHandlerFactory;
  private ShardHandler shardHandler;

  private UpdateLog.RecentUpdates recentUpdates;

  private List<Long> ourUpdates;
  private Set<Long> ourUpdateSet;
  private Set<Long> requestedUpdateSet;
  private long ourLowThreshold;  // 20th percentile
  private long ourHighThreshold; // 80th percentile

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
    this.replicas = replicas;
    this.nUpdates = nUpdates;

    uhandler = core.getUpdateHandler();
    ulog = uhandler.getUpdateLog();
    shardHandlerFactory = core.getCoreDescriptor().getCoreContainer().getShardHandlerFactory();
    shardHandler = shardHandlerFactory.getShardHandler();
  }

  public long percentile(List<Long> arr, float frac) {
    int elem = (int) (arr.size() * frac);
    return Math.abs(arr.get(elem));
  }

  /** Returns true if peer sync was successful, meaning that this core may not be considered to have the latest updates.
   *  A commit is not performed.
   */
  public boolean sync() {
    if (ulog == null) {
      return false;
    }

    // fire off the requests before getting our own recent updates (for better concurrency)
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

    if (ourUpdates.size() > 0) {
      ourLowThreshold = percentile(ourUpdates, 0.8f);
      ourHighThreshold = percentile(ourUpdates, 0.2f);
    }  else {
      // we have no versions and hence no frame of reference to tell if we can use a peers
      // updates to bring us into sync
      return false;
    }

    ourUpdateSet = new HashSet<Long>(ourUpdates);
    requestedUpdateSet = new HashSet<Long>(ourUpdates);

    for(;;) {
      ShardResponse srsp = shardHandler.takeCompletedOrError();
      if (srsp == null) break;
      boolean success = handleResponse(srsp);
      if (!success) {
        shardHandler.cancelAll();
        return false;
      }
    }

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
    if (srsp.getException() != null) {

      // TODO: look at this more thoroughly - we don't want
      // to fail on connection exceptions, but it may make sense
      // to determine this based on the number of fails
      if (srsp.getException() instanceof SolrServerException) {
        Throwable solrException = ((SolrServerException) srsp.getException())
            .getRootCause();
        if (solrException instanceof ConnectException
            || solrException instanceof NoHttpResponseException) {
          return true;
        }
      }
      // TODO: at least log???
      // srsp.getException().printStackTrace(System.out);
      
      return false;
    }

    ShardRequest sreq = srsp.getShardRequest();
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

    if (otherVersions.size() == 0) {
      return true;
    }

    Collections.sort(otherVersions, absComparator);

    long otherHigh = percentile(otherVersions, .2f);
    long otherLow = percentile(otherVersions, .8f);

    if (ourHighThreshold < otherLow) {
      // Small overlap between version windows and ours is older
      // This means that we might miss updates if we attempted to use this method.
      // Since there exists just one replica that is so much newer, we must
      // fail the sync.
      return false;
    }

    if (ourLowThreshold > otherHigh) {
      // Small overlap between windows and ours is newer.
      // Using this list to sync would result in requesting/replaying results we don't need
      // and possibly bringing deleted docs back to life.
      return true;
    }
    
    List<Long> toRequest = new ArrayList<Long>();
    for (Long otherVersion : otherVersions) {
      // stop when the entries get old enough that reorders may lead us to see updates we don't need
      if (Math.abs(otherVersion) < ourLowThreshold) break;

      if (ourUpdateSet.contains(otherVersion) || requestedUpdateSet.contains(otherVersion)) {
        // we either have this update, or already requested it
        continue;
      }

      toRequest.add(otherVersion);
      requestedUpdateSet.add(otherVersion);
    }

    sreq.requestedUpdates = toRequest;

    if (toRequest.isEmpty()) {
      // we had (or already requested) all the updates referenced by the replica
      return true;
    }

    return requestUpdates(srsp, toRequest);
  }

  private boolean requestUpdates(ShardResponse srsp, List<Long> toRequest) {
    String replica = srsp.getShardRequest().shards[0];

    log.info("Requesting updates from " + replica + " versions=" + toRequest);



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
      log.error("PeerSync: Requested " + sreq.requestedUpdates.size() + " updates from " + sreq.shards[0] + " but retrieved " + updates.size());
      return false;
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.SEEN_LEADER, true);
    SolrQueryRequest req = new LocalSolrQueryRequest(uhandler.core, params);
    SolrQueryResponse rsp = new SolrQueryResponse();

    RunUpdateProcessorFactory runFac = new RunUpdateProcessorFactory();
    DistributedUpdateProcessorFactory magicFac = new DistributedUpdateProcessorFactory();
    runFac.init(new NamedList());
    magicFac.init(new NamedList());

    UpdateRequestProcessor proc = magicFac.getInstance(req, rsp, runFac.getInstance(req, rsp, null));

    Collections.sort(updates, updateRecordComparator);

    Object o = null;
    long lastVersion = 0;
    try {
      // Apply oldest updates first
      for (Object obj : updates) {
        // should currently be a List<Oper,Ver,Doc/Id>
        o = obj;
        List<Object> entry = (List<Object>)o;

        int oper = (Integer)entry.get(0);
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
      log.error("Error applying updates from " + sreq.shards + " ,update=" + o, e);
      return false;
    }
    catch (Exception e) {
      sreq.updateException = e;
      log.error("Error applying updates from " + sreq.shards + " ,update=" + o, e);
      return false;
    }
    finally {
      try {
        proc.finish();
      } catch (Exception e) {
        sreq.updateException = e;
        log.error("Error applying updates from " + sreq.shards + " ,finish()", e);
        return false;
      }
    }

    return true;
  }



  /** Requests and applies recent updates from peers */
  public static void sync(SolrCore core, List<String> replicas, int nUpdates) {
    UpdateHandler uhandler = core.getUpdateHandler();

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