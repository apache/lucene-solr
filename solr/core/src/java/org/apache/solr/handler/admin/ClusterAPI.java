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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.http.client.HttpClient;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.CallInfo;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PackageBag;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.SimplePostTool;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.solr.common.params.CommonParams.PACKAGES;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.core.BlobRepository.sha256Digest;
import static org.apache.solr.core.ConfigOverlay.ZNODEVER;
import static org.apache.solr.security.PermissionNameProvider.Name.BLOB_WRITE;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.PKG_EDIT;
import static org.apache.solr.security.PermissionNameProvider.Name.PKG_READ;

//implements  v2 only APIs at /cluster/* end point
public class ClusterAPI {
  private final CoreContainer coreContainer;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  ClusterAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  //sync the cluster props in every node
  void syncClusterProps(CallInfo info) throws IOException {
    Stat stat = new Stat();
    Map<String, Object> clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient()).getClusterProperties(stat);
    try {
      coreContainer.getPackageBag().onChange(clusterProperties);
    } catch (SolrException e) {
      log.error("error executing command : " + info.command.jsonStr(), e);
      throw e;
    } catch (Exception e) {
      log.error("error executing command : " + info.command.jsonStr(), e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error executing command : ", e);
    }
    log.info("current version of clusterprops.json is {} , trying to get every node to update ", stat.getVersion());
    log.debug("The current clusterprops.json:  {}", clusterProperties);
    waitForStateSync(stat.getVersion(), coreContainer);
  }

  void waitForStateSync(int expectedVersion, CoreContainer coreContainer) {
    final RTimer timer = new RTimer();
    int waitTimeSecs = 30;
    // get a list of active replica cores to query for the schema zk version (skipping this core of course)
    List<ClusterAPI.PerNodeCallable> concurrentTasks = new ArrayList<>();

    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    for (String nodeName : zkStateReader.getClusterState().getLiveNodes()) {
      PerNodeCallable e = new PerNodeCallable(coreContainer.getUpdateShardHandler().getDefaultHttpClient(), zkStateReader.getBaseUrlForNodeName(nodeName), expectedVersion, waitTimeSecs);
      concurrentTasks.add(e);
    }
    if (concurrentTasks.isEmpty()) return; // nothing to wait for ...

    log.info("Waiting up to {} secs for {} nodes to update clusterprops to be of version {} ",
        waitTimeSecs, concurrentTasks.size(), expectedVersion);
    SolrConfigHandler.execInparallel(concurrentTasks, parallelExecutor -> {
      try {
        List<String> failedList = SolrConfigHandler.executeAll(expectedVersion, waitTimeSecs, concurrentTasks, parallelExecutor);

        // if any tasks haven't completed within the specified timeout, it's an error
        if (failedList != null)
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              formatString("{0} out of {1} the property {2} to be of version {3} within {4} seconds! Failed cores: {5}",
                  failedList.size(), concurrentTasks.size() + 1, expectedVersion, 30, failedList));
      } catch (InterruptedException e) {
        log.warn(formatString(
            "Request was interrupted . trying to set the clusterprops to version {0} to propagate to {1} nodes ",
            expectedVersion, concurrentTasks.size()));
        Thread.currentThread().interrupt();

      }
    });

    log.info("Took {}ms to update the clusterprops to be of version {}  on {} nodes",
        timer.getTime(), expectedVersion, concurrentTasks.size());

  }

  public List<Api> getAllApis() {
    List<Api> result = new ArrayList<>();
    result.add(new AnnotatedApi(new ClusterAPI.ListNodes()));
    result.add(new AnnotatedApi(new ClusterAPI.BlobWrite()));
    result.add(new AnnotatedApi(new ClusterAPI.PkgRead()));
    result.add(new AnnotatedApi(new ClusterAPI.PkgEdit()));
    result.add(new AnnotatedApi(new ClusterAPI.ClusterCommands()));
    return result;
  }

  @EndPoint(
      spec = "cluster.packages.Commands",
      permission = PKG_EDIT
  )
  public class PkgEdit {

    @Command(name = "add")
    public void add(CallInfo callInfo) throws Exception {
      if (addUpdatePackage(callInfo)) {
        syncClusterProps(callInfo);
      }

    }

    @Command(name = "update")
    public void update(CallInfo callInfo) throws Exception {
      if (addUpdatePackage(callInfo)) {
        syncClusterProps(callInfo);
      }
    }

    @Command(name = "delete")
    public void delPkg(CallInfo info) throws Exception {
      if (deletePackage(info)) {
        syncClusterProps(info);
      }
    }


    boolean deletePackage(CallInfo params) throws Exception {
      if (checkEnabled(params)) return false;
      String name = params.command.getStr(CommandOperation.ROOT_OBJ);
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      Map<String, Object> props = clusterProperties.getClusterProperties();
      List<String> pathToLib = asList(PACKAGES, name);
      Map existing = (Map) Utils.getObjectByPath(props, false, pathToLib);
      if (existing == null) {
        params.command.addError("No such package : " + name);
        return false;
      }
      Map delta = new LinkedHashMap();
      Utils.setObjectByPath(delta, pathToLib, null, true);
      clusterProperties.setClusterProperties(delta);
      return true;
    }


    boolean checkEnabled(CallInfo info) {
      if (!PackageBag.enablePackage) {
        info.command.addError("node not started with enable.package=true");
        return true;
      }
      return false;
    }

    boolean addUpdatePackage(CallInfo params) throws Exception {
      if (checkEnabled(params)) return false;
      CommandOperation op = params.command;
      String name = op.getStr("name");
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      Map<String, Object> props = clusterProperties.getClusterProperties();
      List<String> pathToLib = asList(PACKAGES, name);
      Map existing = (Map) Utils.getObjectByPath(props, false, pathToLib);
      Map<String, Object> dataMap = Utils.getDeepCopy(op.getDataMap(), 3);
      PackageBag.PackageInfo packageInfo = new PackageBag.PackageInfo(dataMap, 0);

      if ("add".equals(op.name)) {
        if (existing != null) {
          op.addError(StrUtils.formatString("The package with a name ''{0}'' already exists ", name));
          return false;
        }
      } else {// this is an update command
        if (existing == null) {
          op.addError(StrUtils.formatString("The package with a name ''{0}'' does not exist", name));
          return false;
        }
        PackageBag.PackageInfo oldInfo = new PackageBag.PackageInfo(existing, 1);
        if (Objects.equals(oldInfo, packageInfo)) {
          op.addError("Trying to update a package with the same data");
          return false;
        }
      }
      try {
        List<String> errs = packageInfo.validate(coreContainer);
        if (!errs.isEmpty()) {
          for (String err : errs) op.addError(err);
          return false;
        }
      } catch (FileNotFoundException fnfe) {
        op.addError(fnfe.getMessage());
        return false;

      } catch (SolrException e) {
        log.error("Error loading package ", e);
        op.addError(e.getMessage());
        return false;
      }

      Map delta = new LinkedHashMap();
      Utils.setObjectByPath(delta, pathToLib, packageInfo, true);
      clusterProperties.setClusterProperties(delta);
      return true;

    }

  }

  @EndPoint(
      spec = "cluster.packages.GET",
      permission = PKG_READ
  )
  public class PkgRead {
    @Command
    public void list(CallInfo info) throws IOException {
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      info.rsp.add(PACKAGES, clusterProperties.getClusterProperty(PACKAGES, MapWriter.EMPTY));
    }
  }

  @EndPoint(spec = "cluster.blob",
      permission = BLOB_WRITE)
  public class BlobWrite {
    @Command
    public void add(CallInfo info) {
      Iterable<ContentStream> streams = info.req.getContentStreams();
      if (streams == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no payload");
      String sha256 = null;
      ContentStream stream = streams.iterator().next();
      try {
        String name = info.req.getParams().get(CommonParams.NAME);
        if (name != null) validateName(name);
        ByteBuffer buf = SimplePostTool.inputStreamToByteArray(stream.getStream());
        sha256 = sha256Digest(buf);
        String blobId = name == null ? sha256 : sha256 + "-" + name;
        coreContainer.getBlobStore().distributeBlob(buf, blobId);
        info.rsp.add(CommonParams.ID, blobId);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

  }

  @EndPoint(spec = "cluster.nodes",
      permission = COLL_READ_PERM)

  public class ListNodes {
    @Command
    public void list(CallInfo info) {
      info.rsp.add("nodes", coreContainer.getZkController().getClusterState().getLiveNodes());
    }
  }

  @EndPoint(spec = "cluster.Commands",
      permission = COLL_EDIT_PERM)
  public class ClusterCommands {
    @Command(name = "add-requesthandler")
    public void addHandler(CallInfo info) throws Exception {
      if (addRequestHandler(info)) syncClusterProps(info);
    }

    @Command(name = "delete-requesthandler")
    public void delHandler(CallInfo info) throws Exception {
      if (deleteReqHandler(info)) syncClusterProps(info);
    }

    @Command(name = "set-obj-property")
    public void setObj(CallInfo info) {
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      try {
        clusterProperties.setClusterProperties(info.command.getDataMap());
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }

    }


    boolean addRequestHandler(CallInfo info) throws Exception {
      Map data = info.command.getDataMap();
      String name = (String) data.get("name");
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      Map<String, Object> map = clusterProperties.getClusterProperties();
      if (Utils.getObjectByPath(map, false, asList(SolrRequestHandler.TYPE, name)) != null) {
        info.command.addError("A requestHandler already exists with the said name");
        return false;
      }
      Map m = new LinkedHashMap();
      Utils.setObjectByPath(m, asList(SolrRequestHandler.TYPE, name), data, true);
      clusterProperties.setClusterProperties(m);
      return true;
    }

    boolean deleteReqHandler(CallInfo info) throws Exception {
      String name = info.command.getStr("");
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      Map<String, Object> map = clusterProperties.getClusterProperties();
      if (Utils.getObjectByPath(map, false, asList(SolrRequestHandler.TYPE, name)) == null) {
        info.command.addError("NO such requestHandler with name :");
        return false;
      }
      Map m = new LinkedHashMap();
      Utils.setObjectByPath(m, asList(SolrRequestHandler.TYPE, name), null, true);
      clusterProperties.setClusterProperties(m);
      return true;
    }

  }

  static class PerNodeCallable extends SolrConfigHandler.PerReplicaCallable {
    private final HttpClient httpClient;
    final String v2Url;

    static final List<String> path = Arrays.asList("metadata", CommonParams.VERSION);

    PerNodeCallable(HttpClient httpClient, String baseUrl, int expectedversion, int waitTime) {
      super(baseUrl, ZNODEVER, expectedversion, waitTime);
      this.httpClient = httpClient;
      v2Url = baseUrl.replace("/solr", "/api") + "/node/ext?wt=javabin&omitHeader=true";
    }

    @Override
    protected boolean verifyResponse(MapWriter mw, int attempts) {
      remoteVersion = (Number) mw._get(path, -1);
      if (remoteVersion.intValue() >= expectedZkVersion) return true;
      log.info(formatString("Could not get expectedVersion {0} from {1} , remote val= {2}   after {3} attempts", expectedZkVersion, coreUrl, remoteVersion, attempts));

      return false;
    }


    @Override
    public Boolean call() throws Exception {
      final RTimer timer = new RTimer();
      int attempts = 0;

      // eventually, this loop will get killed by the ExecutorService's timeout
      while (true) {
        try {
          long timeElapsed = (long) timer.getTime() / 1000;
          if (timeElapsed >= maxWait) {
            return false;
          }
          log.debug("Time elapsed : {} secs, maxWait {}", timeElapsed, maxWait);
          Thread.sleep(100);
          MapWriter resp = (MapWriter) Utils.executeGET(httpClient, v2Url, Utils.JAVABINCONSUMER);
          if (verifyResponse(resp, attempts)) {

            break;
          }
          attempts++;
        } catch (Exception e) {
          if (e instanceof InterruptedException) {
            break; // stop looping
          } else {
            log.warn("Failed to execute " + v2Url + " due to: " + e);
          }
        }
      }
      return true;
    }

  }

  static final String INVALIDCHARS = " /\\#&*\n\t%@~`=+^$><?{}[]|:;!";

  public static void validateName(String name) {
    for (int i = 0; i < name.length(); i++) {
      for (int j = 0; j < INVALIDCHARS.length(); j++) {
        if (name.charAt(i) == INVALIDCHARS.charAt(j))
          throw new IllegalArgumentException("Unsupported char in file name: " + name);
      }
    }
  }


}
