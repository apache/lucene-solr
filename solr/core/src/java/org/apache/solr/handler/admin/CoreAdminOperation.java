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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.store.Directory;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminOp;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.*;
import static org.apache.solr.handler.admin.CoreAdminHandler.COMPLETED;
import static org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import static org.apache.solr.handler.admin.CoreAdminHandler.FAILED;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_MESSAGE;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_STATUS;
import static org.apache.solr.handler.admin.CoreAdminHandler.RUNNING;
import static org.apache.solr.handler.admin.CoreAdminHandler.buildCoreParams;
import static org.apache.solr.handler.admin.CoreAdminHandler.normalizePath;

enum CoreAdminOperation implements CoreAdminOp {

  CREATE_OP(CREATE, it -> {
    assert TestInjection.injectRandomDelayInCoreCreation();

    SolrParams params = it.req.getParams();
    log().info("core create command {}", params);
    String coreName = params.required().get(CoreAdminParams.NAME);
    Map<String, String> coreParams = buildCoreParams(params);
    CoreContainer coreContainer = it.handler.coreContainer;
    Path instancePath = coreContainer.getCoreRootDirectory().resolve(coreName);

    // TODO: Should we nuke setting odd instance paths?  They break core discovery, generally
    String instanceDir = it.req.getParams().get(CoreAdminParams.INSTANCE_DIR);
    if (instanceDir == null)
      instanceDir = it.req.getParams().get("property.instanceDir");
    if (instanceDir != null) {
      instanceDir = PropertiesUtil.substituteProperty(instanceDir, coreContainer.getContainerProperties());
      instancePath = coreContainer.getCoreRootDirectory().resolve(instanceDir).normalize();
    }

    boolean newCollection = params.getBool(CoreAdminParams.NEW_COLLECTION, false);

    coreContainer.create(coreName, instancePath, coreParams, newCollection);

    it.rsp.add("core", coreName);
  }),
  UNLOAD_OP(UNLOAD, it -> {
    SolrParams params = it.req.getParams();
    String cname = params.get(CoreAdminParams.CORE);
    boolean deleteIndexDir = params.getBool(CoreAdminParams.DELETE_INDEX, false);
    boolean deleteDataDir = params.getBool(CoreAdminParams.DELETE_DATA_DIR, false);
    boolean deleteInstanceDir = params.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, false);
    it.handler.coreContainer.unload(cname, deleteIndexDir, deleteDataDir, deleteInstanceDir);

    assert TestInjection.injectNonExistentCoreExceptionAfterUnload(cname);

  }),
  RELOAD_OP(RELOAD, it -> {
    SolrParams params = it.req.getParams();
    String cname = params.get(CoreAdminParams.CORE);

    if (cname == null || !it.handler.coreContainer.getCoreNames().contains(cname)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Core with core name [" + cname + "] does not exist.");
    }

    try {
      it.handler.coreContainer.reload(cname);
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error handling 'reload' action", ex);
    }
  }),
  STATUS_OP(STATUS, new StatusOp()),
  SWAP_OP(SWAP, it -> {
    final SolrParams params = it.req.getParams();
    final String cname = params.get(CoreAdminParams.CORE);
    String other = params.required().get(CoreAdminParams.OTHER);
    it.handler.coreContainer.swap(cname, other);
  }),

  RENAME_OP(RENAME, it -> {
    SolrParams params = it.req.getParams();
    String name = params.get(CoreAdminParams.OTHER);
    String cname = params.get(CoreAdminParams.CORE);

    if (cname.equals(name)) return;

    it.handler.coreContainer.rename(cname, name);
  }),

  MERGEINDEXES_OP(MERGEINDEXES, new MergeIndexesOp()),

  SPLIT_OP(SPLIT, new SplitOp()),

  PREPRECOVERY_OP(PREPRECOVERY, new PrepRecoveryOp()),

  REQUESTRECOVERY_OP(REQUESTRECOVERY, it -> {
    final SolrParams params = it.req.getParams();
    log().info("It has been requested that we recover: core=" + params.get(CoreAdminParams.CORE));
    new Thread(() -> {
      String cname = params.get(CoreAdminParams.CORE);
      if (cname == null) {
        cname = "";
      }
      try (SolrCore core = it.handler.coreContainer.getCore(cname)) {
        if (core != null) {
          core.getUpdateHandler().getSolrCoreState().doRecovery(it.handler.coreContainer, core.getCoreDescriptor());
        } else {
          SolrException.log(log(), "Could not find core to call recovery:" + cname);
        }
      }
    }).start();

  }),
  REQUESTSYNCSHARD_OP(REQUESTSYNCSHARD, new RequestSyncShardOp()),

  REQUESTBUFFERUPDATES_OP(REQUESTBUFFERUPDATES, it -> {
    SolrParams params = it.req.getParams();
    String cname = params.get(CoreAdminParams.NAME, "");
    log().info("Starting to buffer updates on core:" + cname);

    try (SolrCore core = it.handler.coreContainer.getCore(cname)) {
      if (core == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Core [" + cname + "] does not exist");
      UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
      if (updateLog.getState() != UpdateLog.State.ACTIVE) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Core " + cname + " not in active state");
      }
      updateLog.bufferUpdates();
      it.rsp.add("core", cname);
      it.rsp.add("status", "BUFFERING");
    } catch (Throwable e) {
      if (e instanceof SolrException)
        throw (SolrException) e;
      else
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not start buffering updates", e);
    } finally {
      if (it.req != null) it.req.close();
    }
  }),
  REQUESTAPPLYUPDATES_OP(REQUESTAPPLYUPDATES, new RequestApplyUpdatesOp()),

  REQUESTSTATUS_OP(REQUESTSTATUS, it -> {
    SolrParams params = it.req.getParams();
    String requestId = params.get(CoreAdminParams.REQUESTID);
    log().info("Checking request status for : " + requestId);

    if (it.handler.getRequestStatusMap(RUNNING).containsKey(requestId)) {
      it.rsp.add(RESPONSE_STATUS, RUNNING);
    } else if (it.handler.getRequestStatusMap(COMPLETED).containsKey(requestId)) {
      it.rsp.add(RESPONSE_STATUS, COMPLETED);
      it.rsp.add(RESPONSE, it.handler.getRequestStatusMap(COMPLETED).get(requestId).getRspObject());
    } else if (it.handler.getRequestStatusMap(FAILED).containsKey(requestId)) {
      it.rsp.add(RESPONSE_STATUS, FAILED);
      it.rsp.add(RESPONSE, it.handler.getRequestStatusMap(FAILED).get(requestId).getRspObject());
    } else {
      it.rsp.add(RESPONSE_STATUS, "notfound");
      it.rsp.add(RESPONSE_MESSAGE, "No task found in running, completed or failed tasks");
    }
  }),

  OVERSEEROP_OP(OVERSEEROP, it -> {
    ZkController zkController = it.handler.coreContainer.getZkController();
    if (zkController != null) {
      String op = it.req.getParams().get("op");
      String electionNode = it.req.getParams().get("electionNode");
      if (electionNode != null) {
        zkController.rejoinOverseerElection(electionNode, "rejoinAtHead".equals(op));
      } else {
        log().info("electionNode is required param");
      }
    }
  }),

  REJOINLEADERELECTION_OP(REJOINLEADERELECTION, it -> {
    ZkController zkController = it.handler.coreContainer.getZkController();

    if (zkController != null) {
      zkController.rejoinShardLeaderElection(it.req.getParams());
    } else {
      log().warn("zkController is null in CoreAdminHandler.handleRequestInternal:REJOINLEADERELECTION. No action taken.");
    }
  }),
  INVOKE_OP(INVOKE, new InvokeOp()),
  FORCEPREPAREFORLEADERSHIP_OP(FORCEPREPAREFORLEADERSHIP, it -> {
    final SolrParams params = it.req.getParams();

    log().info("I have been forcefully prepare myself for leadership.");
    ZkController zkController = it.handler.coreContainer.getZkController();
    if (zkController == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
    }

    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      throw new IllegalArgumentException(CoreAdminParams.CORE + " is required");
    }
    try (SolrCore core = it.handler.coreContainer.getCore(cname)) {

      // Setting the last published state for this core to be ACTIVE
      if (core != null) {
        core.getCoreDescriptor().getCloudDescriptor().setLastPublished(Replica.State.ACTIVE);
        log().info("Setting the last published state for this core, {}, to {}", core.getName(), Replica.State.ACTIVE);
      } else {
        SolrException.log(log(), "Could not find core: " + cname);
      }
    }
  }),

  BACKUPCORE_OP(BACKUPCORE, new BackupCoreOp()),
  RESTORECORE_OP(RESTORECORE, new RestoreCoreOp()),
  CREATESNAPSHOT_OP(CREATESNAPSHOT, new CreateSnapshotOp()),
  DELETESNAPSHOT_OP(DELETESNAPSHOT, new DeleteSnapshotOp()),
  LISTSNAPSHOTS_OP(LISTSNAPSHOTS, it -> {
    CoreContainer cc = it.handler.getCoreContainer();
    final SolrParams params = it.req.getParams();

    String cname = params.required().get(CoreAdminParams.CORE);
    try ( SolrCore core = cc.getCore(cname) ) {
      if (core == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to locate core " + cname);
      }

      SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();
      NamedList result = new NamedList();
      for (String name : mgr.listSnapshots()) {
        Optional<SnapshotMetaData> metadata = mgr.getSnapshotMetaData(name);
        if ( metadata.isPresent() ) {
          NamedList<String> props = new NamedList<>();
          props.add(SolrSnapshotManager.GENERATION_NUM, String.valueOf(metadata.get().getGenerationNumber()));
          props.add(SolrSnapshotManager.INDEX_DIR_PATH, metadata.get().getIndexDirPath());
          result.add(name, props);
        }
      }
      it.rsp.add(SolrSnapshotManager.SNAPSHOTS_INFO, result);
    }
  });

  final CoreAdminParams.CoreAdminAction action;
  final CoreAdminOp fun;

  CoreAdminOperation(CoreAdminParams.CoreAdminAction action, CoreAdminOp fun) {
    this.action = action;
    this.fun = fun;
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Logger log() {
    return log;
  }




  /**
   * Returns the core status for a particular core.
   * @param cores - the enclosing core container
   * @param cname - the core to return
   * @param isIndexInfoNeeded - add what may be expensive index information. NOT returned if the core is not loaded
   * @return - a named list of key/value pairs from the core.
   * @throws IOException - LukeRequestHandler can throw an I/O exception
   */
  static NamedList<Object> getCoreStatus(CoreContainer cores, String cname, boolean isIndexInfoNeeded) throws IOException {
    NamedList<Object> info = new SimpleOrderedMap<>();

    if (!cores.isLoaded(cname)) { // Lazily-loaded core, fill in what we can.
      // It would be a real mistake to load the cores just to get the status
      CoreDescriptor desc = cores.getUnloadedCoreDescriptor(cname);
      if (desc != null) {
        info.add(NAME, desc.getName());
        info.add("instanceDir", desc.getInstanceDir());
        // None of the following are guaranteed to be present in a not-yet-loaded core.
        String tmp = desc.getDataDir();
        if (StringUtils.isNotBlank(tmp)) info.add("dataDir", tmp);
        tmp = desc.getConfigName();
        if (StringUtils.isNotBlank(tmp)) info.add("config", tmp);
        tmp = desc.getSchemaName();
        if (StringUtils.isNotBlank(tmp)) info.add("schema", tmp);
        info.add("isLoaded", "false");
      }
    } else {
      try (SolrCore core = cores.getCore(cname)) {
        if (core != null) {
          info.add(NAME, core.getName());
          info.add("instanceDir", core.getResourceLoader().getInstancePath().toString());
          info.add("dataDir", normalizePath(core.getDataDir()));
          info.add("config", core.getConfigResource());
          info.add("schema", core.getSchemaResource());
          info.add("startTime", core.getStartTimeStamp());
          info.add("uptime", core.getUptimeMs());
          if (cores.isZooKeeperAware()) {
            info.add("lastPublished", core.getCoreDescriptor().getCloudDescriptor().getLastPublished().toString().toLowerCase(Locale.ROOT));
          }
          if (isIndexInfoNeeded) {
            RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
            try {
              SimpleOrderedMap<Object> indexInfo = LukeRequestHandler.getIndexInfo(searcher.get().getIndexReader());
              long size = getIndexSize(core);
              indexInfo.add("sizeInBytes", size);
              indexInfo.add("size", NumberUtils.readableSize(size));
              info.add("index", indexInfo);
            } finally {
              searcher.decref();
            }
          }
        }
      }
    }
    return info;
  }

  static long getIndexSize(SolrCore core) {
    Directory dir;
    long size = 0;
    try {

      dir = core.getDirectoryFactory().get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);

      try {
        size = core.getDirectoryFactory().size(dir);
      } finally {
        core.getDirectoryFactory().release(dir);
      }
    } catch (IOException e) {
      SolrException.log(log, "IO error while trying to get the size of the Directory", e);
    }
    return size;
  }

  @Override
  public void execute(CallInfo it) throws Exception {
    fun.execute(it);
  }

}
