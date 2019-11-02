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

package org.apache.solr.pkg;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_PKGS_PATH;
import static org.apache.solr.security.PermissionNameProvider.Name.PACKAGE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.PACKAGE_READ_PERM;

/**This implements the public end points (/api/cluster/package) of package API.
 *
 */
public class PackageAPI {
  public static final String PACKAGES = "packages";
  public final boolean enablePackages = Boolean.parseBoolean(System.getProperty("enable.packages", "false"));
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CoreContainer coreContainer;
  private final ObjectMapper mapper = new ObjectMapper();
  private final PackageLoader packageLoader;
  Packages pkgs;

  public final Edit editAPI = new Edit();
  public final Read readAPI = new Read();

  public PackageAPI(CoreContainer coreContainer, PackageLoader loader) {
    this.coreContainer = coreContainer;
    this.packageLoader = loader;
    mapper.setAnnotationIntrospector(SolrJacksonAnnotationInspector.INSTANCE);
    pkgs = new Packages();
    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
    try {
      registerListener(zkClient);
    } catch (KeeperException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
    }
  }

  private void registerListener(SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    String path = SOLR_PKGS_PATH;
    zkClient.exists(path,
        new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            // session events are not change events, and do not remove the watcher
            if (Event.EventType.None.equals(event.getType())) {
              return;
            }
            try {
              synchronized (this) {
                log.debug("Updating [{}] ... ", path);

                // remake watch
                final Watcher thisWatch = this;
                final Stat stat = new Stat();
                final byte[] data = zkClient.getData(path, thisWatch, stat, true);
                pkgs = readPkgsFromZk(data, stat);
                packageLoader.refreshPackageConf();
              }
            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
              log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
            } catch (KeeperException e) {
              log.error("A ZK error has occurred", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.warn("Interrupted", e);
            }
          }

        }, true);
  }


  private Packages readPkgsFromZk(byte[] data, Stat stat) throws KeeperException, InterruptedException {

    if (data == null || stat == null) {
      stat = new Stat();
      data = coreContainer.getZkController().getZkClient()
          .getData(ZkStateReader.CLUSTER_PROPS, null, stat, true);

    }
    Packages packages = null;
    if (data == null || data.length == 0) {
      packages = new Packages();
    } else {
      try {
        packages = mapper.readValue(data, Packages.class);
        packages.znodeVersion = stat.getVersion();
      } catch (IOException e) {
        //invalid data in packages
        //TODO handle properly;
        return new Packages();
      }
    }
    return packages;
  }


  public static class Packages implements ReflectMapWriter {
    @JsonProperty
    public int znodeVersion = -1;

    @JsonProperty
    public Map<String, List<PkgVersion>> packages = new LinkedHashMap<>();


    public Packages copy() {
      Packages p = new Packages();
      p.znodeVersion = this.znodeVersion;
      p.packages = new LinkedHashMap<>();
      packages.forEach((s, versions) ->
          p.packages.put(s, new ArrayList<>(versions)));
      return p;
    }
  }

  public static class PkgVersion implements ReflectMapWriter {

    @JsonProperty
    public String version;

    @JsonProperty
    public List<String> files;

    public PkgVersion() {
    }

    public PkgVersion(Package.AddVersion addVersion) {
      this.version = addVersion.version;
      this.files = addVersion.files;
    }


    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PkgVersion) {
        PkgVersion that = (PkgVersion) obj;
        return Objects.equals(this.version, that.version)
            && Objects.equals(this.files, that.files);

      }
      return false;
    }
  }


  @EndPoint(method = SolrRequest.METHOD.POST,
      path = "/cluster/package",
      permission = PACKAGE_EDIT_PERM)
  public class Edit {

    @Command(name = "refresh")
    public void refresh(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<String> payload) {
      String p = payload.get();
      if (p == null) {
        payload.addError("Package null");
        return;
      }
      PackageLoader.Package pkg = coreContainer.getPackageLoader().getPackage(p);
      if (pkg == null) {
        payload.addError("No such package: " + p);
        return;
      }

      for (String s : coreContainer.getPackageStoreAPI().shuffledNodes()) {
        Utils.executeGET(coreContainer.getUpdateShardHandler().getDefaultHttpClient(),
            coreContainer.getZkController().zkStateReader.getBaseUrlForNodeName(s).replace("/solr", "/api") + "/cluster/package?wt=javabin&omitHeader=true&refreshPackage=" + p,
            Utils.JAVABINCONSUMER);
      }


    }


    @Command(name = "add")
    public void add(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<Package.AddVersion> payload) {
      if (!checkEnabled(payload)) return;
      Package.AddVersion add = payload.get();
      if (add.files.isEmpty()) {
        payload.addError("No files specified");
        return;
      }
      PackageStoreAPI packageStoreAPI = coreContainer.getPackageStoreAPI();
      packageStoreAPI.validateFiles(add.files, true, s -> payload.addError(s));
      if (payload.hasError()) return;
      Packages[] finalState = new Packages[1];
      try {
        coreContainer.getZkController().getZkClient().atomicUpdate(SOLR_PKGS_PATH, (stat, bytes) -> {
          Packages packages = null;
          try {
            packages = bytes == null ? new Packages() : mapper.readValue(bytes, Packages.class);
            packages = packages.copy();
          } catch (IOException e) {
            log.error("Error deserializing packages.json", e);
            packages = new Packages();
          }
          packages.packages.computeIfAbsent(add.pkg, Utils.NEW_ARRAYLIST_FUN).add(new PkgVersion(add));
          packages.znodeVersion = stat.getVersion() ;
          finalState[0] = packages;
          return Utils.toJSON(packages);
        });
      } catch (KeeperException | InterruptedException e) {
        finalState[0] = null;
        handleZkErr(e);
      }
      if (finalState[0] != null) {
//        succeeded in updating
        pkgs = finalState[0];
        notifyAllNodesToSync(pkgs.znodeVersion);
        packageLoader.refreshPackageConf();
      }

    }

    @Command(name = "delete")
    public void del(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<Package.DelVersion> payload) {
      if (!checkEnabled(payload)) return;
      Package.DelVersion delVersion = payload.get();
      try {
        coreContainer.getZkController().getZkClient().atomicUpdate(SOLR_PKGS_PATH, (stat, bytes) -> {
          Packages packages = null;
          try {
            packages = mapper.readValue(bytes, Packages.class);
            packages = packages.copy();
          } catch (IOException e) {
            packages = new Packages();
          }

          List<PkgVersion> versions = packages.packages.get(delVersion.pkg);
          if (versions == null || versions.isEmpty()) {
            payload.addError("No such package: " + delVersion.pkg);
            return null;// no change
          }
          int idxToremove = -1;
          for (int i = 0; i < versions.size(); i++) {
            if (Objects.equals(versions.get(i).version, delVersion.version)) {
              idxToremove = i;
              break;
            }
          }
          if (idxToremove == -1) {
            payload.addError("No such version: " + delVersion.version);
            return null;
          }
          versions.remove(idxToremove);
          packages.znodeVersion = stat.getVersion() + 1;
          return Utils.toJSON(packages);
        });
      } catch (KeeperException | InterruptedException e) {
        handleZkErr(e);

      }


    }

  }

  private boolean checkEnabled(CommandOperation payload) {
    if (!enablePackages) {
      payload.addError("Package loading is not enabled , Start your nodes with -Denable.packages=true");
      return false;
    }
    return true;
  }

  @EndPoint(
      method = SolrRequest.METHOD.GET,
      path = {"/cluster/package/",
          "/cluster/package/{name}"},
      permission = PACKAGE_READ_PERM
  )
  public class Read {
    @Command()
    public void get(SolrQueryRequest req, SolrQueryResponse rsp) {
      String refresh = req.getParams().get("refreshPackage");
      if (refresh != null) {
        packageLoader.notifyListeners(refresh);
        return;
      }

      int expectedVersion = req.getParams().getInt("expectedVersion", -1);
      if (expectedVersion != -1) {
        syncToVersion(expectedVersion);
      }
      String name = req.getPathTemplateValues().get("name");
      if (name == null) {
        rsp.add("result", pkgs);
      } else {
        rsp.add("result", Collections.singletonMap(name, pkgs.packages.get(name)));
      }
    }

    private void syncToVersion(int expectedVersion) {
      int origVersion = pkgs.znodeVersion;
      for (int i = 0; i < 10; i++) {
        log.debug("my version is {} , and expected version {}", pkgs.znodeVersion, expectedVersion);
        if (pkgs.znodeVersion >= expectedVersion) {
          if (origVersion < pkgs.znodeVersion) {
            packageLoader.refreshPackageConf();
          }
          return;
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        try {
          pkgs = readPkgsFromZk(null, null);
        } catch (KeeperException | InterruptedException e) {
          handleZkErr(e);

        }

      }

    }


  }

  void notifyAllNodesToSync(int expected) {
    for (String s : coreContainer.getPackageStoreAPI().shuffledNodes()) {
      Utils.executeGET(coreContainer.getUpdateShardHandler().getDefaultHttpClient(),
          coreContainer.getZkController().zkStateReader.getBaseUrlForNodeName(s).replace("/solr", "/api") + "/cluster/package?wt=javabin&omitHeader=true&expectedVersion" + expected,
          Utils.JAVABINCONSUMER);
    }
  }

  public void handleZkErr(Exception e) {
    log.error("Error reading package config from zookeeper", SolrZkClient.checkInterrupted(e));
  }


}
