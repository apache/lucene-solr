package org.apache.solr.handler.admin;

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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.SyncStrategy;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.SplitIndexCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.CREATE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.FORCEPREPAREFORLEADERSHIP;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.INVOKE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.MERGEINDEXES;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.OVERSEEROP;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.PERSIST;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.PREPRECOVERY;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REJOINLEADERELECTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.RELOAD;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.RENAME;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTBUFFERUPDATES;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTRECOVERY;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTSTATUS;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTSYNCSHARD;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.SPLIT;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.STATUS;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.SWAP;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.UNLOAD;
import static org.apache.solr.handler.admin.CoreAdminHandler.*;

enum CoreAdminOperation {
  CREATE_OP(CREATE) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();
      log.info("core create command {}", params);
      String coreName = params.required().get(CoreAdminParams.NAME);
      Map<String, String> coreParams = buildCoreParams(params);
      CoreContainer coreContainer = callInfo.handler.coreContainer;
      Path instancePath = coreContainer.getCoreRootDirectory().resolve(coreName);

      // TODO: Should we nuke setting odd instance paths?  They break core discovery, generally
      String instanceDir = callInfo.req.getParams().get(CoreAdminParams.INSTANCE_DIR);
      if (instanceDir == null)
        instanceDir = callInfo.req.getParams().get("property.instanceDir");
      if (instanceDir != null) {
        instanceDir = PropertiesUtil.substituteProperty(instanceDir, coreContainer.getContainerProperties());
        instancePath = coreContainer.getCoreRootDirectory().resolve(instanceDir).normalize();
      }

      coreContainer.create(coreName, instancePath, coreParams);

      callInfo.rsp.add("core", coreName);
    }
  },
  UNLOAD_OP(UNLOAD) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();
      String cname = params.get(CoreAdminParams.CORE);
      boolean deleteIndexDir = params.getBool(CoreAdminParams.DELETE_INDEX, false);
      boolean deleteDataDir = params.getBool(CoreAdminParams.DELETE_DATA_DIR, false);
      boolean deleteInstanceDir = params.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, false);
      callInfo.handler.coreContainer.unload(cname, deleteIndexDir, deleteDataDir, deleteInstanceDir);
    }
  },
  RELOAD_OP(RELOAD) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();
      String cname = params.get(CoreAdminParams.CORE);

      if (cname == null || !callInfo.handler.coreContainer.getCoreNames().contains(cname)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core with core name [" + cname + "] does not exist.");
      }

      try {
        callInfo.handler.coreContainer.reload(cname);
      } catch (Exception ex) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error handling 'reload' action", ex);
      }
    }
  },

  PERSIST_OP(PERSIST) {
    @Override
    public void call(CallInfo callInfo) {
      callInfo.rsp.add("message", "The PERSIST action has been deprecated");
    }
  },
  STATUS_OP(STATUS) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();

      String cname = params.get(CoreAdminParams.CORE);
      String indexInfo = params.get(CoreAdminParams.INDEX_INFO);
      boolean isIndexInfoNeeded = Boolean.parseBoolean(null == indexInfo ? "true" : indexInfo);
      NamedList<Object> status = new SimpleOrderedMap<>();
      Map<String, Exception> failures = new HashMap<>();
      for (Map.Entry<String, CoreContainer.CoreLoadFailure> failure : callInfo.handler.coreContainer.getCoreInitFailures().entrySet()) {
        failures.put(failure.getKey(), failure.getValue().exception);
      }
      try {
        if (cname == null) {
          for (String name : callInfo.handler.coreContainer.getAllCoreNames()) {
            status.add(name, getCoreStatus(callInfo.handler.coreContainer, name, isIndexInfoNeeded));
          }
          callInfo.rsp.add("initFailures", failures);
        } else {
          failures = failures.containsKey(cname)
              ? Collections.singletonMap(cname, failures.get(cname))
              : Collections.<String, Exception>emptyMap();
          callInfo.rsp.add("initFailures", failures);
          status.add(cname, getCoreStatus(callInfo.handler.coreContainer, cname, isIndexInfoNeeded));
        }
        callInfo.rsp.add("status", status);
      } catch (Exception ex) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error handling 'status' action ", ex);
      }
    }


  },

  SWAP_OP(SWAP) {
    @Override
    public void call(CallInfo callInfo) {
      final SolrParams params = callInfo.req.getParams();
      final SolrParams required = params.required();

      final String cname = params.get(CoreAdminParams.CORE);
      String other = required.get(CoreAdminParams.OTHER);
      callInfo.handler.coreContainer.swap(cname, other);

    }
  },
  RENAME_OP(RENAME) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();

      String name = params.get(CoreAdminParams.OTHER);
      String cname = params.get(CoreAdminParams.CORE);

      if (cname.equals(name)) return;

      callInfo.handler.coreContainer.rename(cname, name);
    }
  },
  MERGEINDEXES_OP(MERGEINDEXES) {
    @Override
    public void call(CallInfo callInfo) throws Exception {
      SolrParams params = callInfo.req.getParams();
      String cname = params.required().get(CoreAdminParams.CORE);
      SolrCore core = callInfo.handler.coreContainer.getCore(cname);
      SolrQueryRequest wrappedReq = null;

      List<SolrCore> sourceCores = Lists.newArrayList();
      List<RefCounted<SolrIndexSearcher>> searchers = Lists.newArrayList();
      // stores readers created from indexDir param values
      List<DirectoryReader> readersToBeClosed = Lists.newArrayList();
      List<Directory> dirsToBeReleased = Lists.newArrayList();
      if (core != null) {
        try {
          String[] dirNames = params.getParams(CoreAdminParams.INDEX_DIR);
          if (dirNames == null || dirNames.length == 0) {
            String[] sources = params.getParams("srcCore");
            if (sources == null || sources.length == 0)
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "At least one indexDir or srcCore must be specified");

            for (int i = 0; i < sources.length; i++) {
              String source = sources[i];
              SolrCore srcCore = callInfo.handler.coreContainer.getCore(source);
              if (srcCore == null)
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Core: " + source + " does not exist");
              sourceCores.add(srcCore);
            }
          } else {
            DirectoryFactory dirFactory = core.getDirectoryFactory();
            for (int i = 0; i < dirNames.length; i++) {
              Directory dir = dirFactory.get(dirNames[i], DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
              dirsToBeReleased.add(dir);
              // TODO: why doesn't this use the IR factory? what is going on here?
              readersToBeClosed.add(DirectoryReader.open(dir));
            }
          }

          List<DirectoryReader> readers = null;
          if (readersToBeClosed.size() > 0) {
            readers = readersToBeClosed;
          } else {
            readers = Lists.newArrayList();
            for (SolrCore solrCore : sourceCores) {
              // record the searchers so that we can decref
              RefCounted<SolrIndexSearcher> searcher = solrCore.getSearcher();
              searchers.add(searcher);
              readers.add(searcher.get().getIndexReader());
            }
          }

          UpdateRequestProcessorChain processorChain =
              core.getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));
          wrappedReq = new LocalSolrQueryRequest(core, callInfo.req.getParams());
          UpdateRequestProcessor processor =
              processorChain.createProcessor(wrappedReq, callInfo.rsp);
          processor.processMergeIndexes(new MergeIndexesCommand(readers, callInfo.req));
        } catch (Exception e) {
          // log and rethrow so that if the finally fails we don't lose the original problem
          log.error("ERROR executing merge:", e);
          throw e;
        } finally {
          for (RefCounted<SolrIndexSearcher> searcher : searchers) {
            if (searcher != null) searcher.decref();
          }
          for (SolrCore solrCore : sourceCores) {
            if (solrCore != null) solrCore.close();
          }
          IOUtils.closeWhileHandlingException(readersToBeClosed);
          for (Directory dir : dirsToBeReleased) {
            DirectoryFactory dirFactory = core.getDirectoryFactory();
            dirFactory.release(dir);
          }
          if (wrappedReq != null) wrappedReq.close();
          core.close();
        }
      }

    }
  },
  SPLIT_OP(SPLIT) {
    @Override
    public void call(CallInfo callInfo) throws IOException {
      SolrParams params = callInfo.req.getParams();
      List<DocRouter.Range> ranges = null;

      String[] pathsArr = params.getParams(PATH);
      String rangesStr = params.get(CoreAdminParams.RANGES);    // ranges=a-b,c-d,e-f
      if (rangesStr != null) {
        String[] rangesArr = rangesStr.split(",");
        if (rangesArr.length == 0) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There must be at least one range specified to split an index");
        } else {
          ranges = new ArrayList<>(rangesArr.length);
          for (String r : rangesArr) {
            try {
              ranges.add(DocRouter.DEFAULT.fromString(r));
            } catch (Exception e) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Exception parsing hexadecimal hash range: " + r, e);
            }
          }
        }
      }
      String splitKey = params.get("split.key");
      String[] newCoreNames = params.getParams("targetCore");
      String cname = params.get(CoreAdminParams.CORE, "");

      if ((pathsArr == null || pathsArr.length == 0) && (newCoreNames == null || newCoreNames.length == 0)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either path or targetCore param must be specified");
      }

      log.info("Invoked split action for core: " + cname);
      SolrCore core = callInfo.handler.coreContainer.getCore(cname);
      SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
      List<SolrCore> newCores = null;

      try {
        // TODO: allow use of rangesStr in the future
        List<String> paths = null;
        int partitions = pathsArr != null ? pathsArr.length : newCoreNames.length;

        DocRouter router = null;
        String routeFieldName = null;
        if (callInfo.handler.coreContainer.isZooKeeperAware()) {
          ClusterState clusterState = callInfo.handler.coreContainer.getZkController().getClusterState();
          String collectionName = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
          DocCollection collection = clusterState.getCollection(collectionName);
          String sliceName = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
          Slice slice = clusterState.getSlice(collectionName, sliceName);
          router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;
          if (ranges == null) {
            DocRouter.Range currentRange = slice.getRange();
            ranges = currentRange != null ? router.partitionRange(partitions, currentRange) : null;
          }
          Object routerObj = collection.get(DOC_ROUTER); // for back-compat with Solr 4.4
          if (routerObj != null && routerObj instanceof Map) {
            Map routerProps = (Map) routerObj;
            routeFieldName = (String) routerProps.get("field");
          }
        }

        if (pathsArr == null) {
          newCores = new ArrayList<>(partitions);
          for (String newCoreName : newCoreNames) {
            SolrCore newcore = callInfo.handler.coreContainer.getCore(newCoreName);
            if (newcore != null) {
              newCores.add(newcore);
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core with core name " + newCoreName + " expected but doesn't exist.");
            }
          }
        } else {
          paths = Arrays.asList(pathsArr);
        }


        SplitIndexCommand cmd = new SplitIndexCommand(req, paths, newCores, ranges, router, routeFieldName, splitKey);
        core.getUpdateHandler().split(cmd);

        // After the split has completed, someone (here?) should start the process of replaying the buffered updates.

      } catch (Exception e) {
        log.error("ERROR executing split:", e);
        throw new RuntimeException(e);

      } finally {
        if (req != null) req.close();
        if (core != null) core.close();
        if (newCores != null) {
          for (SolrCore newCore : newCores) {
            newCore.close();
          }
        }
      }

    }
  },
  PREPRECOVERY_OP(PREPRECOVERY) {
    @Override
    public void call(CallInfo callInfo) throws InterruptedException, IOException, KeeperException {
      final SolrParams params = callInfo.req.getParams();

      String cname = params.get(CoreAdminParams.CORE);
      if (cname == null) {
        cname = "";
      }

      String nodeName = params.get("nodeName");
      String coreNodeName = params.get("coreNodeName");
      Replica.State waitForState = Replica.State.getState(params.get(ZkStateReader.STATE_PROP));
      Boolean checkLive = params.getBool("checkLive");
      Boolean onlyIfLeader = params.getBool("onlyIfLeader");
      Boolean onlyIfLeaderActive = params.getBool("onlyIfLeaderActive");

      log.info("Going to wait for coreNodeName: " + coreNodeName + ", state: " + waitForState
          + ", checkLive: " + checkLive + ", onlyIfLeader: " + onlyIfLeader
          + ", onlyIfLeaderActive: " + onlyIfLeaderActive);

      int maxTries = 0;
      Replica.State state = null;
      boolean live = false;
      int retry = 0;
      while (true) {
        CoreContainer coreContainer = callInfo.handler.coreContainer;
        try (SolrCore core = coreContainer.getCore(cname)) {
          if (core == null && retry == 30) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "core not found:"
                + cname);
          }
          if (core != null) {
            if (onlyIfLeader != null && onlyIfLeader) {
              if (!core.getCoreDescriptor().getCloudDescriptor().isLeader()) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "We are not the leader");
              }
            }

            // wait until we are sure the recovering node is ready
            // to accept updates
            CloudDescriptor cloudDescriptor = core.getCoreDescriptor()
                .getCloudDescriptor();

            if (retry % 15 == 0) {
              if (retry > 0 && log.isInfoEnabled())
                log.info("After " + retry + " seconds, core " + cname + " (" +
                    cloudDescriptor.getShardId() + " of " +
                    cloudDescriptor.getCollectionName() + ") still does not have state: " +
                    waitForState + "; forcing ClusterState update from ZooKeeper");

              // force a cluster state update
              coreContainer.getZkController().getZkStateReader().updateClusterState();
            }

            if (maxTries == 0) {
              // wait long enough for the leader conflict to work itself out plus a little extra
              int conflictWaitMs = coreContainer.getZkController().getLeaderConflictResolveWait();
              maxTries = (int) Math.round(conflictWaitMs / 1000) + 3;
              log.info("Will wait a max of " + maxTries + " seconds to see " + cname + " (" +
                  cloudDescriptor.getShardId() + " of " +
                  cloudDescriptor.getCollectionName() + ") have state: " + waitForState);
            }

            ClusterState clusterState = coreContainer.getZkController().getClusterState();
            String collection = cloudDescriptor.getCollectionName();
            Slice slice = clusterState.getSlice(collection, cloudDescriptor.getShardId());
            if (slice != null) {
              final Replica replica = slice.getReplicasMap().get(coreNodeName);
              if (replica != null) {
                state = replica.getState();
                live = clusterState.liveNodesContain(nodeName);

                final Replica.State localState = cloudDescriptor.getLastPublished();

                // TODO: This is funky but I've seen this in testing where the replica asks the
                // leader to be in recovery? Need to track down how that happens ... in the meantime,
                // this is a safeguard
                boolean leaderDoesNotNeedRecovery = (onlyIfLeader != null &&
                    onlyIfLeader &&
                    core.getName().equals(replica.getStr("core")) &&
                    waitForState == Replica.State.RECOVERING &&
                    localState == Replica.State.ACTIVE &&
                    state == Replica.State.ACTIVE);

                if (leaderDoesNotNeedRecovery) {
                  log.warn("Leader " + core.getName() + " ignoring request to be in the recovering state because it is live and active.");
                }

                boolean onlyIfActiveCheckResult = onlyIfLeaderActive != null && onlyIfLeaderActive && localState != Replica.State.ACTIVE;
                log.info("In WaitForState(" + waitForState + "): collection=" + collection + ", shard=" + slice.getName() +
                    ", thisCore=" + core.getName() + ", leaderDoesNotNeedRecovery=" + leaderDoesNotNeedRecovery +
                    ", isLeader? " + core.getCoreDescriptor().getCloudDescriptor().isLeader() +
                    ", live=" + live + ", checkLive=" + checkLive + ", currentState=" + state.toString() + ", localState=" + localState + ", nodeName=" + nodeName +
                    ", coreNodeName=" + coreNodeName + ", onlyIfActiveCheckResult=" + onlyIfActiveCheckResult + ", nodeProps: " + replica);

                if (!onlyIfActiveCheckResult && replica != null && (state == waitForState || leaderDoesNotNeedRecovery)) {
                  if (checkLive == null) {
                    break;
                  } else if (checkLive && live) {
                    break;
                  } else if (!checkLive && !live) {
                    break;
                  }
                }
              }
            }
          }

          if (retry++ == maxTries) {
            String collection = null;
            String leaderInfo = null;
            String shardId = null;
            try {
              CloudDescriptor cloudDescriptor =
                  core.getCoreDescriptor().getCloudDescriptor();
              collection = cloudDescriptor.getCollectionName();
              shardId = cloudDescriptor.getShardId();
              leaderInfo = coreContainer.getZkController().
                  getZkStateReader().getLeaderUrl(collection, shardId, 5000);
            } catch (Exception exc) {
              leaderInfo = "Not available due to: " + exc;
            }

            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "I was asked to wait on state " + waitForState + " for "
                    + shardId + " in " + collection + " on " + nodeName
                    + " but I still do not see the requested state. I see state: "
                    + state.toString() + " live:" + live + " leader from ZK: " + leaderInfo
            );
          }

          if (coreContainer.isShutDown()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Solr is shutting down");
          }

          // solrcloud_debug
          if (log.isDebugEnabled()) {
            try {
              LocalSolrQueryRequest r = new LocalSolrQueryRequest(core,
                  new ModifiableSolrParams());
              CommitUpdateCommand commitCmd = new CommitUpdateCommand(r, false);
              commitCmd.softCommit = true;
              core.getUpdateHandler().commit(commitCmd);
              RefCounted<SolrIndexSearcher> searchHolder = core
                  .getNewestSearcher(false);
              SolrIndexSearcher searcher = searchHolder.get();
              try {
                log.debug(core.getCoreDescriptor().getCoreContainer()
                    .getZkController().getNodeName()
                    + " to replicate "
                    + searcher.search(new MatchAllDocsQuery(), 1).totalHits
                    + " gen:"
                    + core.getDeletionPolicy().getLatestCommit().getGeneration()
                    + " data:" + core.getDataDir());
              } finally {
                searchHolder.decref();
              }
            } catch (Exception e) {
              log.debug("Error in solrcloud_debug block", e);
            }
          }
        }
        Thread.sleep(1000);
      }

      log.info("Waited coreNodeName: " + coreNodeName + ", state: " + waitForState
          + ", checkLive: " + checkLive + ", onlyIfLeader: " + onlyIfLeader + " for: " + retry + " seconds.");
    }
  },
  REQUESTRECOVERY_OP(REQUESTRECOVERY) {
    @Override
    public void call(final CallInfo callInfo) throws IOException {
      final SolrParams params = callInfo.req.getParams();
      log.info("It has been requested that we recover: core="+params.get(CoreAdminParams.CORE));
      Thread thread = new Thread() {
        @Override
        public void run() {
          String cname = params.get(CoreAdminParams.CORE);
          if (cname == null) {
            cname = "";
          }
          try (SolrCore core = callInfo.handler.coreContainer.getCore(cname)) {
            if (core != null) {
              core.getUpdateHandler().getSolrCoreState().doRecovery(callInfo.handler.coreContainer, core.getCoreDescriptor());
            } else {
              SolrException.log(log, "Could not find core to call recovery:" + cname);
            }
          }
        }
      };

      thread.start();
    }
  },
  REQUESTSYNCSHARD_OP(REQUESTSYNCSHARD) {
    @Override
    public void call(CallInfo callInfo) throws IOException {
      final SolrParams params = callInfo.req.getParams();

      log.info("I have been requested to sync up my shard");
      ZkController zkController = callInfo.handler.coreContainer.getZkController();
      if (zkController == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
      }

      String cname = params.get(CoreAdminParams.CORE);
      if (cname == null) {
        throw new IllegalArgumentException(CoreAdminParams.CORE + " is required");
      }

      SyncStrategy syncStrategy = null;
      try (SolrCore core = callInfo.handler.coreContainer.getCore(cname)) {

        if (core != null) {
          syncStrategy = new SyncStrategy(core.getCoreDescriptor().getCoreContainer());

          Map<String, Object> props = new HashMap<>();
          props.put(ZkStateReader.BASE_URL_PROP, zkController.getBaseUrl());
          props.put(ZkStateReader.CORE_NAME_PROP, cname);
          props.put(ZkStateReader.NODE_NAME_PROP, zkController.getNodeName());

          boolean success = syncStrategy.sync(zkController, core, new ZkNodeProps(props), true);
          // solrcloud_debug
          if (log.isDebugEnabled()) {
            try {
              RefCounted<SolrIndexSearcher> searchHolder = core
                  .getNewestSearcher(false);
              SolrIndexSearcher searcher = searchHolder.get();
              try {
                log.debug(core.getCoreDescriptor().getCoreContainer()
                    .getZkController().getNodeName()
                    + " synched "
                    + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
              } finally {
                searchHolder.decref();
              }
            } catch (Exception e) {
              log.debug("Error in solrcloud_debug block", e);
            }
          }
          if (!success) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Sync Failed");
          }
        } else {
          SolrException.log(log, "Could not find core to call sync:" + cname);
        }
      } finally {
        // no recoveryStrat close for now
        if (syncStrategy != null) {
          syncStrategy.close();
        }
      }


    }
  },
  REQUESTBUFFERUPDATES_OP(REQUESTBUFFERUPDATES) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();
      String cname = params.get(CoreAdminParams.NAME, "");
      log.info("Starting to buffer updates on core:" + cname);

      try (SolrCore core = callInfo.handler.coreContainer.getCore(cname)) {
        if (core == null)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core [" + cname + "] does not exist");
        UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
        if (updateLog.getState() != UpdateLog.State.ACTIVE)  {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Core " + cname + " not in active state");
        }
        updateLog.bufferUpdates();
        callInfo.rsp.add("core", cname);
        callInfo.rsp.add("status", "BUFFERING");
      } catch (Throwable e) {
        if (e instanceof SolrException)
          throw (SolrException)e;
        else
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not start buffering updates", e);
      } finally {
        if (callInfo.req != null) callInfo.req.close();
      }
    }
  },
  REQUESTAPPLYUPDATES_OP(REQUESTAPPLYUPDATES) {
    @Override
    public void call(CallInfo callInfo) {
      {
        SolrParams params = callInfo.req.getParams();
        String cname = params.get(CoreAdminParams.NAME, "");
        log.info("Applying buffered updates on core: " + cname);
        CoreContainer coreContainer = callInfo.handler.coreContainer;
        try (SolrCore core = coreContainer.getCore(cname)) {
          if (core == null)
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core [" + cname + "] not found");
          UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
          if (updateLog.getState() != UpdateLog.State.BUFFERING) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Core " + cname + " not in buffering state");
          }
          Future<UpdateLog.RecoveryInfo> future = updateLog.applyBufferedUpdates();
          if (future == null) {
            log.info("No buffered updates available. core=" + cname);
            callInfo.rsp.add("core", cname);
            callInfo.rsp.add("status", "EMPTY_BUFFER");
            return;
          }
          UpdateLog.RecoveryInfo report = future.get();
          if (report.failed) {
            SolrException.log(log, "Replay failed");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Replay failed");
          }
          coreContainer.getZkController().publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
          callInfo.rsp.add("core", cname);
          callInfo.rsp.add("status", "BUFFER_APPLIED");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Recovery was interrupted", e);
        } catch (Exception e) {
          if (e instanceof SolrException)
            throw (SolrException) e;
          else
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not apply buffered updates", e);
        } finally {
          if (callInfo.req != null) callInfo.req.close();
        }

      }
    }
  },
  REQUESTSTATUS_OP(REQUESTSTATUS) {
    @Override
    public void call(CallInfo callInfo) {
      SolrParams params = callInfo.req.getParams();
      String requestId = params.get(CoreAdminParams.REQUESTID);
      log.info("Checking request status for : " + requestId);

      if (callInfo.handler.mapContainsTask(RUNNING, requestId)) {
        callInfo.rsp.add(RESPONSE_STATUS, RUNNING);
      } else if (callInfo.handler.mapContainsTask(COMPLETED, requestId)) {
        callInfo.rsp.add(RESPONSE_STATUS, COMPLETED);
        callInfo.rsp.add(RESPONSE, callInfo.handler.getRequestStatusMap(COMPLETED).get(requestId).getRspObject());
      } else if (callInfo.handler.mapContainsTask(FAILED, requestId)) {
        callInfo.rsp.add(RESPONSE_STATUS, FAILED);
        callInfo.rsp.add(RESPONSE, callInfo.handler.getRequestStatusMap(FAILED).get(requestId).getRspObject());
      } else {
        callInfo.rsp.add(RESPONSE_STATUS, "notfound");
        callInfo.rsp.add(RESPONSE_MESSAGE, "No task found in running, completed or failed tasks");
      }

    }
  },
  OVERSEEROP_OP(OVERSEEROP) {
    @Override
    public void call(CallInfo callInfo) {
      ZkController zkController = callInfo.handler.coreContainer.getZkController();
      if (zkController != null) {
        String op = callInfo.req.getParams().get("op");
        String electionNode = callInfo.req.getParams().get("electionNode");
        if (electionNode != null) {
          zkController.rejoinOverseerElection(electionNode, "rejoinAtHead".equals(op));
        } else {
          log.info("electionNode is required param");
        }
      }
    }
  },
  REJOINLEADERELECTION_OP(REJOINLEADERELECTION) {
    @Override
    public void call(CallInfo callInfo) {
      ZkController zkController = callInfo.handler.coreContainer.getZkController();

      if (zkController != null) {
        zkController.rejoinShardLeaderElection(callInfo.req.getParams());
      } else {
        log.warn("zkController is null in CoreAdminHandler.handleRequestInternal:REJOINLEADERELECTION. No action taken.");
      }
    }
  },
  INVOKE_OP(INVOKE) {
    @Override
    public void call(CallInfo callInfo) throws Exception {
      String[] klas = callInfo.req.getParams().getParams("class");
      if (klas == null || klas.length == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "class is a required param");
      }
      for (String c : klas) {
        Map<String, Object> result = invokeAClass(callInfo.req, c);
        callInfo.rsp.add(c, result);
      }

    }

    Map<String, Object> invokeAClass(SolrQueryRequest req, String c) {
      SolrResourceLoader loader = null;
      if (req.getCore() != null) loader = req.getCore().getResourceLoader();
      else if (req.getContext().get(CoreContainer.class.getName()) != null) {
        CoreContainer cc = (CoreContainer) req.getContext().get(CoreContainer.class.getName());
        loader = cc.getResourceLoader();
      }

      Invocable invokable = loader.newInstance(c, Invocable.class);
      Map<String, Object> result = invokable.invoke(req);
      log.info("Invocable_invoked {}", result);
      return result;
    }
  },
  FORCEPREPAREFORLEADERSHIP_OP(FORCEPREPAREFORLEADERSHIP) {
    @Override
    public void call(CallInfo callInfo) throws IOException {
      final SolrParams params = callInfo.req.getParams();

      log.info("I have been forcefully prepare myself for leadership.");
      ZkController zkController = callInfo.handler.coreContainer.getZkController();
      if (zkController == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
      }

      String cname = params.get(CoreAdminParams.CORE);
      if (cname == null) {
        throw new IllegalArgumentException(CoreAdminParams.CORE + " is required");
      }
      try (SolrCore core = callInfo.handler.coreContainer.getCore(cname)) {

        // Setting the last published state for this core to be ACTIVE
        if (core != null) {
          core.getCoreDescriptor().getCloudDescriptor().setLastPublished(Replica.State.ACTIVE);
          log.info("Setting the last published state for this core, {}, to {}", core.getName(), Replica.State.ACTIVE);
        } else {
          SolrException.log(log, "Could not find core: " + cname);
        }
      }

    }
  };
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CoreAdminParams.CoreAdminAction action;

  public abstract void call(CallInfo callInfo) throws Exception;

  CoreAdminOperation(CoreAdminParams.CoreAdminAction action) {
    this.action = action;
  }
  /**
   * Returns the core status for a particular core.
   * @param cores - the enclosing core container
   * @param cname - the core to return
   * @param isIndexInfoNeeded - add what may be expensive index information. NOT returned if the core is not loaded
   * @return - a named list of key/value pairs from the core.
   * @throws IOException - LukeRequestHandler can throw an I/O exception
   */
  NamedList<Object> getCoreStatus(CoreContainer cores, String cname, boolean isIndexInfoNeeded)  throws IOException {
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

  long getIndexSize(SolrCore core) {
    Directory dir;
    long size = 0;
    try {

      dir = core.getDirectoryFactory().get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);

      try {
        size = DirectoryFactory.sizeOfDirectory(dir);
      } finally {
        core.getDirectoryFactory().release(dir);
      }
    } catch (IOException e) {
      SolrException.log(log, "IO error while trying to get the size of the Directory", e);
    }
    return size;
  }
}