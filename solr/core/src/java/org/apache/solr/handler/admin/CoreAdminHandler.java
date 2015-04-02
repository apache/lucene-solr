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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.SyncStrategy;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.SplitIndexCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PATH;

/**
 *
 * @since solr 1.3
 */
public class CoreAdminHandler extends RequestHandlerBase {
  protected static Logger log = LoggerFactory.getLogger(CoreAdminHandler.class);
  protected final CoreContainer coreContainer;
  protected final Map<String, Map<String, TaskObject>> requestStatusMap;

  protected final ExecutorService parallelExecutor = Executors.newFixedThreadPool(50,
      new DefaultSolrThreadFactory("parallelCoreAdminExecutor"));

  protected static int MAX_TRACKED_REQUESTS = 100;
  public static String RUNNING = "running";
  public static String COMPLETED = "completed";
  public static String FAILED = "failed";
  public static String RESPONSE = "Response";
  public static String RESPONSE_STATUS = "STATUS";
  public static String RESPONSE_MESSAGE = "msg";

  public CoreAdminHandler() {
    super();
    // Unlike most request handlers, CoreContainer initialization 
    // should happen in the constructor...  
    this.coreContainer = null;
    HashMap<String, Map<String, TaskObject>> map = new HashMap<>(3, 1.0f);
    map.put(RUNNING, Collections.synchronizedMap(new LinkedHashMap<String, TaskObject>()));
    map.put(COMPLETED, Collections.synchronizedMap(new LinkedHashMap<String, TaskObject>()));
    map.put(FAILED, Collections.synchronizedMap(new LinkedHashMap<String, TaskObject>()));
    requestStatusMap = Collections.unmodifiableMap(map);
  }


  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public CoreAdminHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    HashMap<String, Map<String, TaskObject>> map = new HashMap<>(3, 1.0f);
    map.put(RUNNING, Collections.synchronizedMap(new LinkedHashMap<String, TaskObject>()));
    map.put(COMPLETED, Collections.synchronizedMap(new LinkedHashMap<String, TaskObject>()));
    map.put(FAILED, Collections.synchronizedMap(new LinkedHashMap<String, TaskObject>()));
    requestStatusMap = Collections.unmodifiableMap(map);
  }


  @Override
  final public void init(NamedList args) {
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "CoreAdminHandler should not be configured in solrconf.xml\n" +
                    "it is a special Handler configured directly by the RequestDispatcher");
  }

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance that created this
   * handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    CoreContainer cores = getCoreContainer();
    if (cores == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Core container instance missing");
    }
    //boolean doPersist = false;
    String taskId = req.getParams().get("async");
    TaskObject taskObject = new TaskObject(taskId);

    if(taskId != null) {
      // Put the tasks into the maps for tracking
      if (getMap(RUNNING).containsKey(taskId) || getMap(COMPLETED).containsKey(taskId) || getMap(FAILED).containsKey(taskId)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Duplicate request with the same requestid found.");
      }

      addTask(RUNNING, taskObject);
    }

    // Pick the action
    SolrParams params = req.getParams();
    CoreAdminAction action = CoreAdminAction.STATUS;
    String a = params.get(CoreAdminParams.ACTION);
    if (a != null) {
      action = CoreAdminAction.get(a);
      if (action == null) {
        this.handleCustomAction(req, rsp);
      }
    }

    if (taskId == null) {
      handleRequestInternal(req, rsp, action);
    } else {
      ParallelCoreAdminHandlerThread parallelHandlerThread = new ParallelCoreAdminHandlerThread(req, rsp, action, taskObject);
      parallelExecutor.execute(parallelHandlerThread);
    }
  }

  protected void handleRequestInternal(SolrQueryRequest req, SolrQueryResponse rsp, CoreAdminAction action) throws Exception {
    if (action != null) {
      switch (action) {
        case CREATE: {
          this.handleCreateAction(req, rsp);
          break;
        }

        case RENAME: {
          this.handleRenameAction(req, rsp);
          break;
        }

        case UNLOAD: {
          this.handleUnloadAction(req, rsp);
          break;
        }

        case STATUS: {
          this.handleStatusAction(req, rsp);
          break;

        }

        case PERSIST: {
          this.handlePersistAction(req, rsp);
          break;
        }

        case RELOAD: {
          this.handleReloadAction(req, rsp);
          break;
        }

        case SWAP: {
          this.handleSwapAction(req, rsp);
          break;
        }

        case MERGEINDEXES: {
          this.handleMergeAction(req, rsp);
          break;
        }

        case SPLIT: {
          this.handleSplitAction(req, rsp);
          break;
        }

        case PREPRECOVERY: {
          this.handleWaitForStateAction(req, rsp);
          break;
        }

        case REQUESTRECOVERY: {
          this.handleRequestRecoveryAction(req, rsp);
          break;
        }

        case REQUESTSYNCSHARD: {
          this.handleRequestSyncAction(req, rsp);
          break;
        }

        // todo : Can this be done by the regular RecoveryStrategy route?
        case REQUESTAPPLYUPDATES: {
          this.handleRequestApplyUpdatesAction(req, rsp);
          break;
        }
        case REQUESTBUFFERUPDATES:  {
          this.handleRequestBufferUpdatesAction(req, rsp);
          break;
        }
        case REQUESTSTATUS: {
          this.handleRequestActionStatus(req, rsp);
          break;
        }
        case OVERSEEROP:{
          ZkController zkController = coreContainer.getZkController();
          if(zkController != null){
           String op = req.getParams().get("op");
           String electionNode = req.getParams().get("electionNode");
           if(electionNode != null) {
             zkController.rejoinOverseerElection(electionNode, "rejoinAtHead".equals(op));
           } else {
             log.info("electionNode is required param");
           }
          }
          break;
        }
        default: {
          this.handleCustomAction(req, rsp);
          break;
        }
        case LOAD:
          break;

        case REJOINLEADERELECTION:
          ZkController zkController = coreContainer.getZkController();

          if (zkController != null) {
            zkController.rejoinShardLeaderElection(req.getParams());
          } else {
            log.warn("zkController is null in CoreAdminHandler.handleRequestInternal:REJOINLEADERELCTIONS. No action taken.");
          }
          break;
      }
    }
    rsp.setHttpCaching(false);
  }


  /**
   * Handle the core admin SPLIT action.
   */
  protected void handleSplitAction(SolrQueryRequest adminReq, SolrQueryResponse rsp) throws IOException {
    SolrParams params = adminReq.getParams();
    List<DocRouter.Range> ranges = null;

    String[] pathsArr = params.getParams(PATH);
    String rangesStr = params.get(CoreAdminParams.RANGES);    // ranges=a-b,c-d,e-f
    if (rangesStr != null)  {
      String[] rangesArr = rangesStr.split(",");
      if (rangesArr.length == 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "There must be at least one range specified to split an index");
      } else  {
        ranges = new ArrayList<>(rangesArr.length);
        for (String r : rangesArr) {
          try {
            ranges.add(DocRouter.DEFAULT.fromString(r));
          } catch (Exception e) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Exception parsing hexadecimal hash range: " + r, e);
          }
        }
      }
    }
    String splitKey = params.get("split.key");
    String[] newCoreNames = params.getParams("targetCore");
    String cname = params.get(CoreAdminParams.CORE, "");

    if ((pathsArr == null || pathsArr.length == 0) && (newCoreNames == null || newCoreNames.length == 0)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Either path or targetCore param must be specified");
    }

    log.info("Invoked split action for core: " + cname);
    SolrCore core = coreContainer.getCore(cname);
    SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
    List<SolrCore> newCores = null;

    try {
      // TODO: allow use of rangesStr in the future
      List<String> paths = null;
      int partitions = pathsArr != null ? pathsArr.length : newCoreNames.length;

      DocRouter router = null;
      String routeFieldName = null;
      if (coreContainer.isZooKeeperAware()) {
        ClusterState clusterState = coreContainer.getZkController().getClusterState();
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
          SolrCore newcore = coreContainer.getCore(newCoreName);
          if (newcore != null) {
            newCores.add(newcore);
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Core with core name " + newCoreName + " expected but doesn't exist.");
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


  protected void handleMergeAction(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    SolrCore core = coreContainer.getCore(cname);
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
            throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                "At least one indexDir or srcCore must be specified");

          for (int i = 0; i < sources.length; i++) {
            String source = sources[i];
            SolrCore srcCore = coreContainer.getCore(source);
            if (srcCore == null)
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "Core: " + source + " does not exist");
            sourceCores.add(srcCore);
          }
        } else  {
          DirectoryFactory dirFactory = core.getDirectoryFactory();
          for (int i = 0; i < dirNames.length; i++) {
            Directory dir = dirFactory.get(dirNames[i], DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
            dirsToBeReleased.add(dir);
            // TODO: why doesn't this use the IR factory? what is going on here?
            readersToBeClosed.add(DirectoryReader.open(dir));
          }
        }

        List<DirectoryReader> readers = null;
        if (readersToBeClosed.size() > 0)  {
          readers = readersToBeClosed;
        } else {
          readers = Lists.newArrayList();
          for (SolrCore solrCore: sourceCores) {
            // record the searchers so that we can decref
            RefCounted<SolrIndexSearcher> searcher = solrCore.getSearcher();
            searchers.add(searcher);
            readers.add(searcher.get().getIndexReader());
          }
        }

        UpdateRequestProcessorChain processorChain =
                core.getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));
        wrappedReq = new LocalSolrQueryRequest(core, req.getParams());
        UpdateRequestProcessor processor =
                processorChain.createProcessor(wrappedReq, rsp);
        processor.processMergeIndexes(new MergeIndexesCommand(readers, req));
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

  /**
   * Handle Custom Action.
   * <p>
   * This method could be overridden by derived classes to handle custom actions. <br> By default - this method throws a
   * solr exception. Derived classes are free to write their derivation if necessary.
   */
  protected void handleCustomAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported operation: " +
            req.getParams().get(CoreAdminParams.ACTION));
  }

  public static ImmutableMap<String, String> paramToProp = ImmutableMap.<String, String>builder()
      .put(CoreAdminParams.CONFIG, CoreDescriptor.CORE_CONFIG)
      .put(CoreAdminParams.SCHEMA, CoreDescriptor.CORE_SCHEMA)
      .put(CoreAdminParams.DATA_DIR, CoreDescriptor.CORE_DATADIR)
      .put(CoreAdminParams.ULOG_DIR, CoreDescriptor.CORE_ULOGDIR)
      .put(CoreAdminParams.CONFIGSET, CoreDescriptor.CORE_CONFIGSET)
      .put(CoreAdminParams.LOAD_ON_STARTUP, CoreDescriptor.CORE_LOADONSTARTUP)
      .put(CoreAdminParams.TRANSIENT, CoreDescriptor.CORE_TRANSIENT)
      .put(CoreAdminParams.SHARD, CoreDescriptor.CORE_SHARD)
      .put(CoreAdminParams.COLLECTION, CoreDescriptor.CORE_COLLECTION)
      .put(CoreAdminParams.ROLES, CoreDescriptor.CORE_ROLES)
      .put(CoreAdminParams.CORE_NODE_NAME, CoreDescriptor.CORE_NODE_NAME)
      .put(ZkStateReader.NUM_SHARDS_PROP, CloudDescriptor.NUM_SHARDS)
      .build();

  public static ImmutableMap<String, String> cloudParamToProp;

  protected static CoreDescriptor buildCoreDescriptor(SolrParams params, CoreContainer container) {

    String name = checkNotEmpty(params.get(CoreAdminParams.NAME),
        "Missing parameter [" + CoreAdminParams.NAME + "]");

    Properties coreProps = new Properties();
    for (String param : paramToProp.keySet()) {
      String value = params.get(param, null);
      if (StringUtils.isNotEmpty(value)) {
        coreProps.setProperty(paramToProp.get(param), value);
      }
    }
    Iterator<String> paramsIt = params.getParameterNamesIterator();
    while (paramsIt.hasNext()) {
      String param = paramsIt.next();
      if (!param.startsWith(CoreAdminParams.PROPERTY_PREFIX))
        continue;
      String propName = param.substring(CoreAdminParams.PROPERTY_PREFIX.length());
      String propValue = params.get(param);
      coreProps.setProperty(propName, propValue);
    }

    String instancedir = params.get(CoreAdminParams.INSTANCE_DIR);
    if (StringUtils.isEmpty(instancedir) && coreProps.getProperty(CoreAdminParams.INSTANCE_DIR) != null) {
      instancedir = coreProps.getProperty(CoreAdminParams.INSTANCE_DIR);
    } else if (StringUtils.isEmpty(instancedir)){
      instancedir = name; // will be resolved later against solr.home
      //instancedir = container.getSolrHome() + "/" + name;
    }

    return new CoreDescriptor(container, name, instancedir, coreProps, params);
  }

  private static String checkNotEmpty(String value, String message) {
    if (StringUtils.isEmpty(value))
      throw new SolrException(ErrorCode.BAD_REQUEST, message);
    return value;
  }

  /**
   * Handle 'CREATE' action.
   *
   * @throws SolrException in case of a configuration error.
   */
  protected void handleCreateAction(SolrQueryRequest req, SolrQueryResponse rsp) throws SolrException {

    SolrParams params = req.getParams();
    log.info("core create command {}", params);
    CoreDescriptor dcore = buildCoreDescriptor(params, coreContainer);

    if (coreContainer.getAllCoreNames().contains(dcore.getName())) {
      log.warn("Creating a core with existing name is not allowed");
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Core with name '" + dcore.getName() + "' already exists.");
    }

    // TODO this should be moved into CoreContainer, really...
    boolean preExisitingZkEntry = false;
    try {
      if (coreContainer.getZkController() != null) {
        if (!Overseer.isLegacy(coreContainer.getZkController().getZkStateReader().getClusterProps())) {
          if (dcore.getCloudDescriptor().getCoreNodeName() == null) {
            throw new SolrException(ErrorCode.SERVER_ERROR,
                "non legacy mode coreNodeName missing " + params);
            
          }
        }
        
        preExisitingZkEntry = checkIfCoreNodeNameAlreadyExists(dcore);

      }
      
      SolrCore core = coreContainer.create(dcore);
      
      // only write out the descriptor if the core is successfully created
      coreContainer.getCoresLocator().create(coreContainer, dcore);

      rsp.add("core", core.getName());
    }
    catch (Exception ex) {
      if (coreContainer.isZooKeeperAware() && dcore != null && !preExisitingZkEntry) {
        try {
          coreContainer.getZkController().unregister(dcore.getName(), dcore,null);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          SolrException.log(log, null, e);
        } catch (KeeperException e) {
          SolrException.log(log, null, e);
        }
      }
      
      Throwable tc = ex;
      Throwable c = null;
      do {
        tc = tc.getCause();
        if (tc != null) {
          c = tc;
        }
      } while (tc != null);
      
      String rootMsg = "";
      if (c != null) {
        rootMsg = " Caused by: " + c.getMessage();
      }
      
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Error CREATEing SolrCore '" + dcore.getName() + "': " +
          ex.getMessage() + rootMsg, ex);
    }
  }


  private boolean checkIfCoreNodeNameAlreadyExists(CoreDescriptor dcore) {
    ZkStateReader zkStateReader = coreContainer.getZkController()
        .getZkStateReader();
    DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(dcore.getCollectionName());
    if (collection != null) {
      Collection<Slice> slices = collection.getSlices();
      
      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          if (replica.getName().equals(
              dcore.getCloudDescriptor().getCoreNodeName())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Handle "RENAME" Action
   */
  protected void handleRenameAction(SolrQueryRequest req, SolrQueryResponse rsp) throws SolrException {
    SolrParams params = req.getParams();

    String name = params.get(CoreAdminParams.OTHER);
    String cname = params.get(CoreAdminParams.CORE);

    if (cname.equals(name)) return;

    coreContainer.rename(cname, name);

  }

  /**
   * Handle "UNLOAD" Action
   */
  protected void handleUnloadAction(SolrQueryRequest req, SolrQueryResponse rsp) throws SolrException {

    SolrParams params = req.getParams();
    String cname = params.get(CoreAdminParams.CORE);
    boolean deleteIndexDir = params.getBool(CoreAdminParams.DELETE_INDEX, false);
    boolean deleteDataDir = params.getBool(CoreAdminParams.DELETE_DATA_DIR, false);
    boolean deleteInstanceDir = params.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, false);

    coreContainer.unload(cname, deleteIndexDir, deleteDataDir, deleteInstanceDir);

  }

  /**
   * Handle "STATUS" action
   */
  protected void handleStatusAction(SolrQueryRequest req, SolrQueryResponse rsp)
          throws SolrException {
    SolrParams params = req.getParams();

    String cname = params.get(CoreAdminParams.CORE);
    String indexInfo = params.get(CoreAdminParams.INDEX_INFO);
    boolean isIndexInfoNeeded = Boolean.parseBoolean(null == indexInfo ? "true" : indexInfo);
    NamedList<Object> status = new SimpleOrderedMap<>();
    Map<String, Exception> failures = new HashMap<>();
    for (Map.Entry<String, CoreContainer.CoreLoadFailure> failure : coreContainer.getCoreInitFailures().entrySet()) {
      failures.put(failure.getKey(), failure.getValue().exception);
    }
    try {
      if (cname == null) {
        for (String name : coreContainer.getAllCoreNames()) {
          status.add(name, getCoreStatus(coreContainer, name, isIndexInfoNeeded));
        }
        rsp.add("initFailures", failures);
      } else {
        failures = failures.containsKey(cname)
          ? Collections.singletonMap(cname, failures.get(cname))
          : Collections.<String, Exception>emptyMap();
        rsp.add("initFailures", failures);
        status.add(cname, getCoreStatus(coreContainer, cname, isIndexInfoNeeded));
      }
      rsp.add("status", status);
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Error handling 'status' action ", ex);
    }
  }

  /**
   * Handler "PERSIST" action
   */
  protected void handlePersistAction(SolrQueryRequest req, SolrQueryResponse rsp)
          throws SolrException {
    rsp.add("message", "The PERSIST action has been deprecated");
  }

  /**
   * Handler "RELOAD" action
   */
  protected void handleReloadAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    String cname = params.get(CoreAdminParams.CORE);

    if(!coreContainer.getCoreNames().contains(cname)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Core with core name [" + cname + "] does not exist.");
    }

    try {
      coreContainer.reload(cname);
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error handling 'reload' action", ex);
    }
  }

  /**
   * Handle "REQUESTSTATUS" action
   */
  protected void handleRequestActionStatus(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    String requestId = params.get(CoreAdminParams.REQUESTID);
    log.info("Checking request status for : " + requestId);

    if (mapContainsTask(RUNNING, requestId)) {
      rsp.add(RESPONSE_STATUS, RUNNING);
    } else if(mapContainsTask(COMPLETED, requestId)) {
      rsp.add(RESPONSE_STATUS, COMPLETED);
      rsp.add(RESPONSE, getMap(COMPLETED).get(requestId).getRspObject());
    } else if(mapContainsTask(FAILED, requestId)) {
      rsp.add(RESPONSE_STATUS, FAILED);
      rsp.add(RESPONSE, getMap(FAILED).get(requestId).getRspObject());
    } else {
      rsp.add(RESPONSE_STATUS, "notfound");
      rsp.add(RESPONSE_MESSAGE, "No task found in running, completed or failed tasks");
    }
  }

  /**
   * Handle "SWAP" action
   */
  protected void handleSwapAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    final SolrParams params = req.getParams();
    final SolrParams required = params.required();

    final String cname = params.get(CoreAdminParams.CORE);
    String other = required.get(CoreAdminParams.OTHER);
    coreContainer.swap(cname, other);

  }
  
  protected void handleRequestRecoveryAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws IOException {
    final SolrParams params = req.getParams();
    log.info("It has been requested that we recover: core="+params.get(CoreAdminParams.CORE));
    Thread thread = new Thread() {
      @Override
      public void run() {
        String cname = params.get(CoreAdminParams.CORE);
        if (cname == null) {
          cname = "";
        }
        try (SolrCore core = coreContainer.getCore(cname)) {
          if (core != null) {
            core.getUpdateHandler().getSolrCoreState().doRecovery(coreContainer, core.getCoreDescriptor());
          } else {
            SolrException.log(log, "Could not find core to call recovery:" + cname);
          }
        }
      }
    };
    
    thread.start();
  }
  
  protected void handleRequestSyncAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws IOException {
    final SolrParams params = req.getParams();

    log.info("I have been requested to sync up my shard");
    ZkController zkController = coreContainer.getZkController();
    if (zkController == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
    }
    
    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      throw new IllegalArgumentException(CoreAdminParams.CORE + " is required");
    }

    SyncStrategy syncStrategy = null;
    try (SolrCore core = coreContainer.getCore(cname)) {

      if (core != null) {
        syncStrategy = new SyncStrategy(core.getCoreDescriptor().getCoreContainer());
        
        Map<String,Object> props = new HashMap<>();
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
          throw new SolrException(ErrorCode.SERVER_ERROR, "Sync Failed");
        }
      } else {
        SolrException.log(log, "Cound not find core to call sync:" + cname);
      }
    } finally {
      // no recoveryStrat close for now
      if (syncStrategy != null) {
        syncStrategy.close();
      }
    }
    

  }
  
  protected void handleWaitForStateAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws IOException, InterruptedException, KeeperException {
    final SolrParams params = req.getParams();
    
    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      cname = "";
    }
    
    String nodeName = params.get("nodeName");
    String coreNodeName = params.get("coreNodeName");
    String waitForState = params.get("state");
    Boolean checkLive = params.getBool("checkLive");
    Boolean onlyIfLeader = params.getBool("onlyIfLeader");
    Boolean onlyIfLeaderActive = params.getBool("onlyIfLeaderActive");

    log.info("Going to wait for coreNodeName: " + coreNodeName + ", state: " + waitForState
        + ", checkLive: " + checkLive + ", onlyIfLeader: " + onlyIfLeader
        + ", onlyIfLeaderActive: "+onlyIfLeaderActive);

    int maxTries = 0; 
    String state = null;
    boolean live = false;
    int retry = 0;
    while (true) {
      try (SolrCore core = coreContainer.getCore(cname)) {
        if (core == null && retry == 30) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "core not found:"
              + cname);
        }
        if (core != null) {
          if (onlyIfLeader != null && onlyIfLeader) {
           if (!core.getCoreDescriptor().getCloudDescriptor().isLeader()) {
             throw new SolrException(ErrorCode.BAD_REQUEST, "We are not the leader");
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
            coreContainer.getZkController().getZkStateReader().updateClusterState(true);
          }

          if (maxTries == 0) {
            // wait long enough for the leader conflict to work itself out plus a little extra
            int conflictWaitMs = coreContainer.getZkController().getLeaderConflictResolveWait();
            maxTries = (int) Math.round(conflictWaitMs / 1000) + 3;
            log.info("Will wait a max of " + maxTries + " seconds to see " + cname + " (" +
                cloudDescriptor.getShardId() + " of " +
                cloudDescriptor.getCollectionName() + ") have state: " + waitForState);
          }
          
          ClusterState clusterState = coreContainer.getZkController()
              .getClusterState();
          String collection = cloudDescriptor.getCollectionName();
          Slice slice = clusterState.getSlice(collection,
              cloudDescriptor.getShardId());
          if (slice != null) {
            ZkNodeProps nodeProps = slice.getReplicasMap().get(coreNodeName);
            if (nodeProps != null) {
              state = nodeProps.getStr(ZkStateReader.STATE_PROP);
              live = clusterState.liveNodesContain(nodeName);
              
              String localState = cloudDescriptor.getLastPublished();

              // TODO: This is funky but I've seen this in testing where the replica asks the
              // leader to be in recovery? Need to track down how that happens ... in the meantime,
              // this is a safeguard 
              boolean leaderDoesNotNeedRecovery = (onlyIfLeader != null && 
                  onlyIfLeader && 
                  core.getName().equals(nodeProps.getStr("core")) &&
                  ZkStateReader.RECOVERING.equals(waitForState) && 
                  ZkStateReader.ACTIVE.equals(localState) && 
                  ZkStateReader.ACTIVE.equals(state));
              
              if (leaderDoesNotNeedRecovery) {
                log.warn("Leader "+core.getName()+" ignoring request to be in the recovering state because it is live and active.");
              }              
              
              boolean onlyIfActiveCheckResult = onlyIfLeaderActive != null && onlyIfLeaderActive && (localState == null || !localState.equals(ZkStateReader.ACTIVE));
              log.info("In WaitForState("+waitForState+"): collection="+collection+", shard="+slice.getName()+
                  ", thisCore="+core.getName()+", leaderDoesNotNeedRecovery="+leaderDoesNotNeedRecovery+
                  ", isLeader? "+core.getCoreDescriptor().getCloudDescriptor().isLeader()+
                  ", live="+live+", checkLive="+checkLive+", currentState="+state+", localState="+localState+", nodeName="+nodeName+
                  ", coreNodeName="+coreNodeName+", onlyIfActiveCheckResult="+onlyIfActiveCheckResult+", nodeProps: "+nodeProps);

              if (!onlyIfActiveCheckResult && nodeProps != null && (state.equals(waitForState) || leaderDoesNotNeedRecovery)) {
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

          throw new SolrException(ErrorCode.BAD_REQUEST,
              "I was asked to wait on state " + waitForState + " for "
                  + shardId + " in " + collection + " on " + nodeName
                  + " but I still do not see the requested state. I see state: "
                  + state + " live:" + live + " leader from ZK: " + leaderInfo
          );
        }
        
        if (coreContainer.isShutDown()) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
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

  private void handleRequestApplyUpdatesAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    String cname = params.get(CoreAdminParams.NAME, "");
    log.info("Applying buffered updates on core: " + cname);
    try (SolrCore core = coreContainer.getCore(cname)) {
      if (core == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Core [" + cname + "] not found");
      UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
      if (updateLog.getState() != UpdateLog.State.BUFFERING)  {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Core " + cname + " not in buffering state");
      }
      Future<UpdateLog.RecoveryInfo> future = updateLog.applyBufferedUpdates();
      if (future == null) {
        log.info("No buffered updates available. core=" + cname);
        rsp.add("core", cname);
        rsp.add("status", "EMPTY_BUFFER");
        return;
      }
      UpdateLog.RecoveryInfo report = future.get();
      if (report.failed) {
        SolrException.log(log, "Replay failed");
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
      }
      coreContainer.getZkController().publish(core.getCoreDescriptor(), ZkStateReader.ACTIVE);
      rsp.add("core", cname);
      rsp.add("status", "BUFFER_APPLIED");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Recovery was interrupted", e);
    } catch (Exception e) {
      if (e instanceof SolrException)
        throw (SolrException)e;
      else
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not apply buffered updates", e);
    } finally {
      if (req != null) req.close();
    }
    
  }

  private void handleRequestBufferUpdatesAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    String cname = params.get(CoreAdminParams.NAME, "");
    log.info("Starting to buffer updates on core:" + cname);

    try (SolrCore core = coreContainer.getCore(cname)) {
      if (core == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Core [" + cname + "] does not exist");
      UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
      if (updateLog.getState() != UpdateLog.State.ACTIVE)  {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Core " + cname + " not in active state");
      }
      updateLog.bufferUpdates();
      rsp.add("core", cname);
      rsp.add("status", "BUFFERING");
    } catch (Throwable e) {
      if (e instanceof SolrException)
        throw (SolrException)e;
      else
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not start buffering updates", e);
    } finally {
      if (req != null) req.close();
    }
  }

  /**
   * Returns the core status for a particular core.
   * @param cores - the enclosing core container
   * @param cname - the core to return
   * @param isIndexInfoNeeded - add what may be expensive index information. NOT returned if the core is not loaded
   * @return - a named list of key/value pairs from the core.
   * @throws IOException - LukeRequestHandler can throw an I/O exception
   */
  protected NamedList<Object> getCoreStatus(CoreContainer cores, String cname, boolean isIndexInfoNeeded)  throws IOException {
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
          info.add("instanceDir", normalizePath(core.getResourceLoader().getInstanceDir()));
          info.add("dataDir", normalizePath(core.getDataDir()));
          info.add("config", core.getConfigResource());
          info.add("schema", core.getSchemaResource());
          info.add("startTime", new Date(core.getStartTime()));
          info.add("uptime", System.currentTimeMillis() - core.getStartTime());
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

  private long getIndexSize(SolrCore core) {
    Directory dir;
    long size = 0;
    try {

      dir = core.getDirectoryFactory().get(core.getIndexDir(), DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType); 

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

  protected static String normalizePath(String path) {
    if (path == null)
      return null;
    path = path.replace('/', File.separatorChar);
    path = path.replace('\\', File.separatorChar);
    return path;
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage Multiple Solr Cores";
  }

  /**
   * Class to implement multi-threaded CoreAdminHandler behaviour.
   * This accepts all of the context from handleRequestBody.
   */
  protected class ParallelCoreAdminHandlerThread implements Runnable {
    SolrQueryRequest req;
    SolrQueryResponse rsp;
    CoreAdminAction action;
    TaskObject taskObject;

    public ParallelCoreAdminHandlerThread (SolrQueryRequest req, SolrQueryResponse rsp,
                                           CoreAdminAction action, TaskObject taskObject){
      this.req = req;
      this.rsp = rsp;
      this.action = action;
      this.taskObject = taskObject;
    }

    public void run() {
      boolean exceptionCaught = false;
      try {
        handleRequestInternal(req, rsp, action);
        taskObject.setRspObject(rsp);
      } catch (Exception e) {
        exceptionCaught = true;
        taskObject.setRspObjectFromException(e);
      } finally {
        removeTask("running", taskObject.taskId);
        if(exceptionCaught) {
          addTask("failed", taskObject, true);
        } else
          addTask("completed", taskObject, true);
      }

    }

  }

  /**
   * Helper class to manage the tasks to be tracked.
   * This contains the taskId, request and the response (if available).
   */
  private class TaskObject {
    String taskId;
    String rspInfo;

    public TaskObject(String taskId) {
      this.taskId = taskId;
    }

    public String getRspObject() {
      return rspInfo;
    }

    public void setRspObject(SolrQueryResponse rspObject) {
      this.rspInfo = rspObject.getToLogAsString("TaskId: " + this.taskId + " ");
    }

    public void setRspObjectFromException(Exception e) {
      this.rspInfo = e.getMessage();
    }
  }

  /**
   * Helper method to add a task to a tracking map.
   */
  protected void addTask(String map, TaskObject o, boolean limit) {
    synchronized (getMap(map)) {
      if(limit && getMap(map).size() == MAX_TRACKED_REQUESTS) {
        String key = getMap(map).entrySet().iterator().next().getKey();
        getMap(map).remove(key);
      }
      addTask(map, o);
    }
  }


  protected void addTask(String map, TaskObject o) {
    synchronized (getMap(map)) {
      getMap(map).put(o.taskId, o);
    }
  }

  /**
   * Helper method to remove a task from a tracking map.
   */
  protected void removeTask(String map, String taskId) {
    synchronized (getMap(map)) {
      getMap(map).remove(taskId);
    }
  }

  /**
   * Helper method to check if a map contains a taskObject with the given taskId.
   */
  protected boolean mapContainsTask(String map, String taskId) {
    return getMap(map).containsKey(taskId);
  }

  /**
   * Helper method to get a TaskObject given a map and a taskId.
   */
  protected TaskObject getTask(String map, String taskId) {
    return getMap(map).get(taskId);
  }

  /**
   * Helper method to get a request status map given the name.
   */
  private Map<String, TaskObject> getMap(String map) {
    return requestStatusMap.get(map);
  }

  /**
   * Method to ensure shutting down of the ThreadPool Executor.
   */
  public void shutdown() {
    if (parallelExecutor != null && !parallelExecutor.isShutdown())
      ExecutorUtil.shutdownAndAwaitTermination(parallelExecutor);
  }
}
