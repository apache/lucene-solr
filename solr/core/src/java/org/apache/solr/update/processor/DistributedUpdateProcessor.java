package org.apache.solr.update.processor;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.NullArgumentException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.cloud.HashPartitioner;
import org.apache.solr.cloud.HashPartitioner.Range;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionBucket;
import org.apache.solr.update.VersionInfo;
import org.apache.zookeeper.KeeperException;

// NOT mt-safe... create a new processor for each add thread
public class DistributedUpdateProcessor extends UpdateRequestProcessor {
  public static final String SEEN_LEADER = "leader";
  
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final UpdateRequestProcessor next;

  private static final String VERSION_FIELD = "_version_";
  private final UpdateHandler updateHandler;
  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private final boolean returnVersions = true; // todo: default to false and make configurable

  private NamedList addsResponse = null;
  private NamedList deleteResponse = null;
  private CharsRef scratch;
  private boolean isLeader = true;
  private boolean forwardToLeader = false;

  private final SchemaField idField;
  
  private final SolrCmdDistributor cmdDistrib;

  private HashPartitioner hp;

  private List<String> shards;

  private boolean zkEnabled = false;
  
  public DistributedUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    this.rsp = rsp;
    this.next = next;
    this.idField = req.getSchema().getUniqueKeyField();
    // version init

    this.updateHandler = req.getCore().getUpdateHandler();
    this.ulog = updateHandler.getUpdateLog();
    this.vinfo = ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;

    // TODO: better way to get the response, or pass back info to it?
    SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    this.req = req;
    
    this.zkEnabled  = req.getCore().getCoreDescriptor().getCoreContainer().isZooKeeperAware();
    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;
    
    cmdDistrib = new SolrCmdDistributor(req, rsp);
  }

  private List<String> setupRequest(int hash) {
    List<String> shards = null;
    
    CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
    
    CloudState cloudState = req.getCore().getCoreDescriptor().getCoreContainer().getZkController().getCloudState();
    
    String collection = coreDesc.getCloudDescriptor().getCollectionName();
    String shardId = getShard(hash, collection, cloudState); // get the right shard based on the hash...

    // if we are in zk mode...
    if (coreDesc.getCoreContainer().getZkController() != null) {
      // the leader is...
      // TODO: if there is no leader, wait and look again
      // TODO: we are reading the leader from zk every time - we should cache
      // this and watch for changes?? Just pull it from ZkController cluster state probably?

      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      
      String leaderNode = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
          + ZkStateReader.LEADER_ELECT_ZKNODE + "/" + shardId + "/leader";
      SolrZkClient zkClient = coreDesc.getCoreContainer().getZkController()
          .getZkClient();

      try {
        List<String> leaderChildren = zkClient.getChildren(leaderNode, null);
        if (leaderChildren.size() > 0) {
          String leader = leaderChildren.get(0);
          
          byte[] bytes = zkClient
              .getData(leaderNode + "/" + leader, null, null);
          ZkNodeProps zkNodeProps = ZkNodeProps.load(bytes);
          
          String leaderUrl = zkNodeProps.get("url");
          
          String nodeName = req.getCore().getCoreDescriptor()
              .getCoreContainer().getZkController().getNodeName();
          String shardZkNodeName = nodeName + "_" + req.getCore().getName();

          if (params.getBool(SEEN_LEADER, false)) {
            // we got a version, just go local - set no shardStr
            
            // still mark if i am the leader though
            if (!shardZkNodeName.equals(leader)) {
              isLeader = false;
            }
          } else if (shardZkNodeName.equals(leader)) {
            isLeader = true;
            // that means I want to forward onto my replicas...
            // so get the replicas...
            shards = getReplicaUrls(req, collection, shardId,
                shardZkNodeName);
            
            // mark that this req has been to the leader
            params.set(SEEN_LEADER, true);
          } else {
            // I need to forward onto the leader...
            shards = new ArrayList<String>(1);
            shards.add(leaderUrl);
            forwardToLeader = true;
            isLeader = false;
          }
          req.setParams(params);
        }
      } catch (KeeperException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }
    
    return shards;
  }
  
  private String getShard(int hash, String collection, CloudState cloudState) {
    // nocommit: we certainly don't want to do this every update request...
    // get the shard names
    Map<String,Slice> slices = cloudState.getSlices(collection);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find collection "
          + collection + " in " + cloudState.getCollections());
    }
    
    Set<String> shards = slices.keySet();
    List<String> shardList = new ArrayList<String>();
    shardList.addAll(shards);
    Collections.sort(shardList);
    hp = new HashPartitioner();
    List<Range> ranges = hp.partitionRange(shards.size());
    int cnt = 0;
    for (Range range : ranges) {
      if (hash < range.max) {
        return shardList.get(cnt);
      }
      cnt++;
    }
    
    throw new IllegalStateException("The HashPartitioner failed");
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    int hash = hash(cmd);
    
    if (zkEnabled) {
      shards = setupRequest(hash);
    } else {
      // even in non zk mode, tests simulate updates from a leader
      isLeader = !req.getParams().getBool(SEEN_LEADER, false);
    }
    
    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd = versionAdd(cmd, hash);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }
    
    if (shards != null) {
      cmdDistrib.distribAdd(cmd, shards);
    } else {
      // At a minimum, local updates must be protected by synchronization
      // super.processAdd(cmd);
    }
    
    
    if (returnVersions && rsp != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<String>();
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }

    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.
  }

  // must be synchronized by bucket
  private void doLocalAdd(AddUpdateCommand cmd) throws IOException {
    if (shards == null)   // TODO: temporary. a distrib update currently goes through HTTP for self
      super.processAdd(cmd);
  }

  // must be synchronized by bucket
  private void doLocalDelete(DeleteUpdateCommand cmd) throws IOException {
    if (shards == null)   // TODO: temporary. a distrib update currently goes through HTTP for self
      super.processDelete(cmd);
  }

  /**
   * @param cmd
   * @param hash
   * @return whether or not to drop this cmd
   * @throws IOException
   */
  private boolean versionAdd(AddUpdateCommand cmd, int hash) throws IOException {
    if (vinfo == null) {
      super.processAdd(cmd);
      return false;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    long versionOnUpdate = 0;
    SolrInputField versionField = cmd.getSolrInputDocument().getField(VersionInfo.VERSION_FIELD);
    if (versionField != null) {
      Object o = versionField.getValue();
      versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
    } else {
      // TODO: check for the version in the request params (this will be for user provided versions and optimistic concurrency only)
    }

    VersionBucket bucket = vinfo.bucket(hash);

    vinfo.lockForUpdate();
    try {
      synchronized (bucket) {
        // we obtain the version when synchronized and then do the add so we can ensure that
        // if version1 < version2 then version1 is actually added before version2.

        // even if we don't store the version field, synchronizing on the bucket
        // will enable us to know what version happened first, and thus enable
        // realtime-get to work reliably.
        // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
        // there may be other reasons in the future for a version on the commands
        if (versionsStored) {

          long bucketVersion = bucket.highest;

          if (isLeader) {
            long version = vinfo.getNewClock();
            cmd.setVersion(version);
            cmd.getSolrInputDocument().setField(VersionInfo.VERSION_FIELD, version);
            bucket.updateHighest(version);
          } else {
            // The leader forwarded us this update.
            cmd.setVersion(versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.add(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occured.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update.  we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered.  We need to drop this update.
                return true;
              }
            }
          }
        }

        doLocalAdd(cmd);
      }  // end synchronized (bucket)
    } finally {
      vinfo.unlockForUpdate();
    }
    return false;
  }
  
  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    int hash = 0;
    if (cmd.getIndexedId() == null) {
      // delete by query...
      // TODO: handle versioned and distributed deleteByQuery
      super.processDelete(cmd);
    } else {
      hash = hash(cmd);
    }
    if (zkEnabled) {
      shards = setupRequest(hash);
    } else {
      // even in non zk mode, tests simulate updates from a leader
      isLeader = !req.getParams().getBool(SEEN_LEADER, false);
    }
    
    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd  = versionDelete(cmd, hash);
    }
    
    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    if (shards != null) {
      cmdDistrib.distribDelete(cmd, shards);
    } else {
      // super.processDelete(cmd);
    }

    // cmd.getIndexId == null when delete by query
    if (returnVersions && rsp != null && cmd.getIndexedId() != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<String>();
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  private boolean versionDelete(DeleteUpdateCommand cmd, int hash) throws IOException {
    if (cmd == null) {
      throw new NullArgumentException("cmd is null");
    }
    
    if (vinfo == null) {
      return false;
    }

    if (cmd.id == null) {
      // delete-by-query
      // TODO: forward to all nodes in distrib mode?  or just don't bother to support?
      return false;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
    Long versionOnUpdate = versionOnUpdateS == null ? null : Long.parseLong(versionOnUpdateS);

    VersionBucket bucket = vinfo.bucket(hash);

    vinfo.lockForUpdate();
    try {

      synchronized (bucket) {
        if (versionsStored) {
          long bucketVersion = bucket.highest;

          if (isLeader) {
            long version = vinfo.getNewClock();
            cmd.setVersion(-version);
            bucket.updateHighest(version);
          } else {
            // The leader forwarded us this update.
            if (versionOnUpdate == null) {
              throw new RuntimeException("we expected to find versionOnUpdate but did not");
            }

            cmd.setVersion(versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.delete(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occured.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update.  we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered.  We need to drop this update.
                return true;
              }
            }
          }
        }

        doLocalDelete(cmd);
        return false;
      }  // end synchronized (bucket)

    } finally {
      vinfo.unlockForUpdate();
    }
  }
  
  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {

    if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
      log.info("Ignoring commit while not ACTIVE");
      return;
    }


    // nocommit: make everyone commit?
//    if (shards != null) {
//      cmdDistrib.distribCommit(cmd, shards);
//    } else {
      super.processCommit(cmd);
//    }
  }
  
  @Override
  public void finish() throws IOException {
    if (shards != null) cmdDistrib.finish(shards);
    if (next != null && shards == null) next.finish();
  }
  
  // TODO: currently this also includes the leader url
  private List<String> getReplicaUrls(SolrQueryRequest req, String collection,
      String shardId, String shardZkNodeName) {
    CloudState cloudState = req.getCore().getCoreDescriptor()
        .getCoreContainer().getZkController().getCloudState();
   
    Map<String,Slice> slices = cloudState.getSlices(collection);
    if (slices == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST, "Could not find collection in zk: " + collection);
    }
    
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST, "Could not find shardId in zk: " + shardId);
    }
    
    Map<String,ZkNodeProps> shardMap = replicas.getShards();
    List<String> urls = new ArrayList<String>();

    for (Entry<String,ZkNodeProps> entry : shardMap.entrySet()) {
      if (cloudState.liveNodesContain(entry.getValue().get(
          ZkStateReader.NODE_NAME_PROP))) {
        String replicaUrl = entry.getValue().get(ZkStateReader.URL_PROP);
        urls.add(replicaUrl);
      }
    }
    if (urls.size() == 0) {
      throw new ZooKeeperException(ErrorCode.SERVICE_UNAVAILABLE, "No available servers hosting shard " + shardId + " found");
    }
    return urls;
  }
  
  // TODO: move this to AddUpdateCommand/DeleteUpdateCommand and cache it? And
  // make the hash pluggable of course.
  // The hash also needs to be pluggable
  private int hash(AddUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
  
  private int hash(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
}
