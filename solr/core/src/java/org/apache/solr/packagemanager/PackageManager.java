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

package org.apache.solr.packagemanager;

import static org.apache.solr.packagemanager.PackageUtils.getMapper;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.http.client.methods.HttpDelete;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.filestore.DistribPackageStore;
import org.apache.solr.packagemanager.SolrPackage.Command;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.packagemanager.SolrPackage.Plugin;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.util.SolrCLI;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * Handles most of the management of packages that are already installed in Solr.
 */
public class PackageManager implements Closeable {

  final String solrBaseUrl;
  final HttpSolrClient solrClient;
  final SolrZkClient zkClient;

  private Map<String, List<SolrPackageInstance>> packages = null;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public PackageManager(HttpSolrClient solrClient, String solrBaseUrl, String zkHost) {
    this.solrBaseUrl = solrBaseUrl;
    this.solrClient = solrClient;
    this.zkClient = new SolrZkClient(zkHost, 30000);
    log.info("Done initializing a zkClient instance...");
  }

  @Override
  public void close() throws IOException {
    if (zkClient != null) {
      zkClient.close();
    }
  }

  public void uninstall(String packageName, String version) {
    SolrPackageInstance packageInstance = getPackageInstance(packageName, version);
    if (packageInstance == null) {
      PackageUtils.printRed("Package " + packageName + ":" + version + " doesn't exist. Use the install command to install this package version first.");
      System.exit(1);
    }

    // Make sure that this package instance is not deployed on any collection
    Map<String, String> collectionsDeployedOn = getDeployedCollections(packageName);
    for (String collection: collectionsDeployedOn.keySet()) {
      if (version.equals(collectionsDeployedOn.get(collection))) {
        PackageUtils.printRed("Package " + packageName + " is currently deployed on collection: " + collection + ". Undeploy the package with undeploy <package-name> -collections <collection1>[,<collection2>,...] before attempting to uninstall the package.");
        System.exit(1);
      }
    }

    // Make sure that no plugin from this package instance has been deployed as cluster level plugins
    Map<String, SolrPackageInstance> clusterPackages = getPackagesDeployedAsClusterLevelPlugins();
    for (String clusterPackageName: clusterPackages.keySet()) {
      SolrPackageInstance clusterPackageInstance = clusterPackages.get(clusterPackageName);
      if (packageName.equals(clusterPackageName) && version.equals(clusterPackageInstance.version)) {
        PackageUtils.printRed("Package " + packageName + "is currently deployed as a cluster-level plugin (" + clusterPackageInstance.getCustomData() + "). Undeploy the package with undeploy <package-name> -collections <collection1>[,<collection2>,...] before uninstalling the package.");
        System.exit(1);
      }
    }

    // Delete the package by calling the Package API and remove the Jar

    PackageUtils.printGreen("Executing Package API to remove this package...");
    Package.DelVersion del = new Package.DelVersion();
    del.version = version;
    del.pkg = packageName;

    V2Request req = new V2Request.Builder(PackageUtils.PACKAGE_PATH)
            .forceV2(true)
            .withMethod(SolrRequest.METHOD.POST)
            .withPayload(Collections.singletonMap("delete", del))
            .build();

    try {
      V2Response resp = req.process(solrClient);
      PackageUtils.printGreen("Response: " + resp.jsonStr());
    } catch (SolrServerException | IOException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }

    PackageUtils.printGreen("Executing Package Store API to remove the " + packageName + " package...");

    List<String> filesToDelete = new ArrayList<>(packageInstance.files);
    filesToDelete.add(String.format(Locale.ROOT, "/package/%s/%s/%s", packageName, version, "manifest.json"));
    for (String filePath: filesToDelete) {
      DistribPackageStore.deleteZKFileEntry(zkClient, filePath);
      String path = solrClient.getBaseURL() + "/api/cluster/files" + filePath;
      PackageUtils.printGreen("Deleting " + path);
      HttpDelete httpDel = new HttpDelete(path);
      Utils.executeHttpMethod(solrClient.getHttpClient(), path, Utils.JSONCONSUMER, httpDel);
    }

    PackageUtils.printGreen("Package uninstalled: " + packageName + ":" + version + ":-)");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public List<SolrPackageInstance> fetchInstalledPackageInstances() throws SolrException {
    log.info("Getting packages from packages.json...");
    List<SolrPackageInstance> ret = new ArrayList<SolrPackageInstance>();
    packages = new HashMap<String, List<SolrPackageInstance>>();
    try {
      Map packagesZnodeMap = null;

      if (zkClient.exists(ZkStateReader.SOLR_PKGS_PATH, true) == true) {
        packagesZnodeMap = (Map)getMapper().readValue(
            new String(zkClient.getData(ZkStateReader.SOLR_PKGS_PATH, null, null, true), "UTF-8"), Map.class).get("packages");
        for (Object packageName: packagesZnodeMap.keySet()) {
          List pkg = (List)packagesZnodeMap.get(packageName);
          for (Map pkgVersion: (List<Map>)pkg) {
            Manifest manifest = PackageUtils.fetchManifest(solrClient, solrBaseUrl, pkgVersion.get("manifest").toString(), pkgVersion.get("manifestSHA512").toString());
            List<Plugin> solrPlugins = manifest.plugins;
            SolrPackageInstance pkgInstance = new SolrPackageInstance(packageName.toString(), null,
                    pkgVersion.get("version").toString(), manifest, solrPlugins, manifest.parameterDefaults);
            if (pkgVersion.containsKey("files")) {
              pkgInstance.files = (List) pkgVersion.get("files");
            }
            List<SolrPackageInstance> list = packages.containsKey(packageName) ? packages.get(packageName) : new ArrayList<SolrPackageInstance>();
            list.add(pkgInstance);
            packages.put(packageName.toString(), list);
            ret.add(pkgInstance);
          }
        }
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
    log.info("Got packages: {}", ret);
    return ret;
  }

  @SuppressWarnings({"unchecked"})
  public Map<String, SolrPackageInstance> getPackagesDeployed(String collection) {
    Map<String, String> packages = null;
    try {
      NavigableObject result = (NavigableObject) Utils.executeGET(solrClient.getHttpClient(),
          solrBaseUrl + PackageUtils.getCollectionParamsPath(collection) + "/PKG_VERSIONS?omitHeader=true&wt=javabin", Utils.JAVABINCONSUMER);
      packages = (Map<String, String>) result._get("/response/params/PKG_VERSIONS", Collections.emptyMap());
    } catch (PathNotFoundException ex) {
      // Don't worry if PKG_VERSION wasn't found. It just means this collection was never touched by the package manager.
    }
    if (packages == null) return Collections.emptyMap();
    Map<String, SolrPackageInstance> ret = new HashMap<>();
    for (String packageName: packages.keySet()) {
      if (Strings.isNullOrEmpty(packageName) == false && // There can be an empty key, storing the version here
          packages.get(packageName) != null) { // null means the package was undeployed from this package before
        ret.put(packageName, getPackageInstance(packageName, packages.get(packageName)));
      }
    }
    return ret;
  }

  /**
   * Get a map of packages (key: package name, value: package instance) that have their plugins deployed as cluster level plugins.
   * The returned packages also contain the "pluginMeta" from "clusterprops.json" as custom data.
   */
  @SuppressWarnings({"unchecked"})
  public Map<String, SolrPackageInstance> getPackagesDeployedAsClusterLevelPlugins() {
    Map<String, String> packageVersions = new HashMap<>();
    MultiValuedMap<String, PluginMeta> packagePlugins = new HashSetValuedHashMap<>(); // map of package name to multiple values of pluginMeta (Map<String, String>)
    @SuppressWarnings({"unchecked"})
    Map<String, Object> result;
    try {
      result = (Map<String, Object>) Utils.executeGET(solrClient.getHttpClient(),
               solrBaseUrl + PackageUtils.CLUSTERPROPS_PATH, Utils.JSONCONSUMER);
    } catch (SolrException ex) {
      if (ex.code() == ErrorCode.NOT_FOUND.code) {
        result = Collections.emptyMap(); // Cluster props doesn't exist, that means there are no cluster level plugins installed.
      } else {
        throw ex;
      }
    }
    @SuppressWarnings({"unchecked"})
    Map<String, Object> clusterPlugins = (Map<String, Object>) result.getOrDefault("plugin", Collections.emptyMap());
    for (String key : clusterPlugins.keySet()) {
      // Map<String, String> pluginMeta = (Map<String, String>) clusterPlugins.get(key);
      PluginMeta pluginMeta;
      try {
        pluginMeta = PackageUtils.getMapper().readValue(Utils.toJSON(clusterPlugins.get(key)), PluginMeta.class);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Exception while fetching plugins from /clusterprops.json in ZK.", e);
      }
      if (pluginMeta.klass.contains(":")) {
        String packageName = pluginMeta.klass.substring(0, pluginMeta.klass.indexOf(':'));
        packageVersions.put(packageName, pluginMeta.version);
        packagePlugins.put(packageName, pluginMeta);
      }
    }
    Map<String, SolrPackageInstance> ret = new HashMap<>();
    for (String packageName: packageVersions.keySet()) {
      if (Strings.isNullOrEmpty(packageName) == false && // There can be an empty key, storing the version here
          packageVersions.get(packageName) != null) { // null means the package was undeployed from this package before
        ret.put(packageName, getPackageInstance(packageName, packageVersions.get(packageName)));
        ret.get(packageName).setCustomData(packagePlugins.get(packageName));
      }
    }
    return ret;
  }

  private void ensureCollectionsExist(List<String> collections) {
    try {
      List<String> existingCollections = zkClient.getChildren("/collections", null, true);
      Set<String> nonExistent = new HashSet<>(collections);
      nonExistent.removeAll(existingCollections);
      if (nonExistent.isEmpty() == false) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Collection(s) doesn't exist: " + nonExistent.toString());
      }
    } catch (KeeperException | InterruptedException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to fetch list of collections from ZK.");
    }
  }
  
  private boolean deployPackage(SolrPackageInstance packageInstance, boolean pegToLatest, boolean isUpdate, boolean noprompt,
      List<String> collections, boolean shouldDeployClusterPlugins, String[] overrides) {

    // Install plugins of type "cluster"
    boolean clusterSuccess = true;
    if (shouldDeployClusterPlugins) {
      clusterSuccess = deployClusterPackage(packageInstance, isUpdate, noprompt, overrides);
    }
    
    // Install plugins of type "collection"
    Pair<List<String>, List<String>> deployResult = deployCollectionPackage(packageInstance, pegToLatest, isUpdate, noprompt, collections, overrides);
    List<String> deployedCollections = deployResult.first();
    List<String> previouslyDeployedOnCollections = deployResult.second();
    
    // Verify
    boolean verifySuccess = true;
    // Verify that package was successfully deployed
    verifySuccess = verify(packageInstance, deployedCollections, shouldDeployClusterPlugins, overrides);
    if (verifySuccess) {
      PackageUtils.printGreen("Deployed on " + deployedCollections + " and verified package: " + packageInstance.name + ", version: " + packageInstance.version);
    }

    return clusterSuccess && previouslyDeployedOnCollections.isEmpty() && verifySuccess;
  }

  /**
   * @return list of collections on which packages deployed on
   */
  private Pair<List<String>, List<String>> deployCollectionPackage(SolrPackageInstance packageInstance, boolean pegToLatest, boolean isUpdate,
      boolean noprompt, List<String> collections, String[] overrides) {
    List<String> previouslyDeployed =  new ArrayList<>(); // collections where package is already deployed in
    for (String collection: collections) {
      SolrPackageInstance deployedPackage = getPackagesDeployed(collection).get(packageInstance.name);
      if (packageInstance.equals(deployedPackage)) {
        if (!pegToLatest) {
          PackageUtils.printRed("Package " + packageInstance + " already deployed on "+collection);
          previouslyDeployed.add(collection);
          continue;
        }
      } else {
        if (deployedPackage != null && !isUpdate) {
          PackageUtils.printRed("Package " + deployedPackage + " already deployed on "+collection+". To update to "+packageInstance+", pass --update parameter.");
          previouslyDeployed.add(collection);
          continue;
        }
      }

      Map<String,String> collectionParameterOverrides = getCollectionParameterOverrides(packageInstance, isUpdate, overrides, collection);

      // Get package params
      try {
        @SuppressWarnings("unchecked")
        boolean packageParamsExist = ((Map<Object, Object>)PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + PackageUtils.getCollectionParamsPath(collection) + "/packages", Map.class)
            .getOrDefault("response", Collections.emptyMap())).containsKey("params");
        SolrCLI.postJsonToSolr(solrClient, PackageUtils.getCollectionParamsPath(collection),
            getMapper().writeValueAsString(Collections.singletonMap(packageParamsExist? "update": "set",
                Collections.singletonMap("packages", Collections.singletonMap(packageInstance.name, collectionParameterOverrides)))));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, PackageUtils.getCollectionParamsPath(collection),
            "{set:{PKG_VERSIONS:{" + packageInstance.name+": '" + (pegToLatest? PackageLoader.LATEST: packageInstance.version)+"'}}}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }

      // If updating, refresh the package version for this to take effect
      if (isUpdate || pegToLatest) {
        try {
          SolrCLI.postJsonToSolr(solrClient, PackageUtils.PACKAGE_PATH, "{\"refresh\": \"" + packageInstance.name + "\"}");
        } catch (Exception ex) {
          throw new SolrException(ErrorCode.SERVER_ERROR, ex);
        }
      }

      // If it is a fresh deploy on a collection, run setup commands all the plugins in the package
      if (!isUpdate) {
        for (Plugin plugin: packageInstance.plugins) {
          if ("collection".equalsIgnoreCase(plugin.type) == false || collections.isEmpty()) continue;
          Map<String, String> systemParams = PackageUtils.map("collection", collection, "package-name", packageInstance.name, "package-version", packageInstance.version, "plugin-name", plugin.name);

          Command cmd = plugin.setupCommand;
          if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
            if ("POST".equalsIgnoreCase(cmd.method)) {
              try {
                String payload = PackageUtils.resolve(getMapper().writeValueAsString(cmd.payload), packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
                String path = PackageUtils.resolve(cmd.path, packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
                PackageUtils.printGreen("Executing " + payload + " for path:" + path);
                boolean shouldExecute = prompt(noprompt);
                if (shouldExecute) {
                  SolrCLI.postJsonToSolr(solrClient, path, payload);
                }
              } catch (Exception ex) {
                throw new SolrException(ErrorCode.SERVER_ERROR, ex);
              }
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST, "Non-POST method not supported for setup commands");
            }
          } else {
            PackageUtils.printRed("There is no setup command to execute for plugin: " + plugin.name);
          }
        }
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, PackageUtils.getCollectionParamsPath(collection),
            "{update:{PKG_VERSIONS:{'" + packageInstance.name + "' : '" + (pegToLatest? PackageLoader.LATEST: packageInstance.version) + "'}}}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }
    }

    if (previouslyDeployed.isEmpty() == false) {
      PackageUtils.printRed("Already Deployed on " + previouslyDeployed + ", package: " + packageInstance.name + ", version: " + packageInstance.version);
    }

    List<String> deployedCollections = collections.stream().filter(c -> !previouslyDeployed.contains(c)).collect(Collectors.toList());
    return new Pair<List<String>, List<String>>(deployedCollections, previouslyDeployed);
  }

  @SuppressWarnings("unchecked")
  private boolean deployClusterPackage(SolrPackageInstance packageInstance, boolean isUpdate, boolean noprompt, String[] overrides) {
    boolean clusterPluginFailed = false;
    int numberOfClusterPluginsDeployed = 0;

    if (isUpdate) {
      for (Plugin plugin: packageInstance.plugins) {
        if ("cluster".equalsIgnoreCase(plugin.type) == false) continue;
        SolrPackageInstance deployedPackage = getPackagesDeployedAsClusterLevelPlugins().get(packageInstance.name);
        if (deployedPackage == null) {
          PackageUtils.printRed("Cluster level plugin " + plugin.name + " from package " + packageInstance.name + " not deployed. To deploy, remove the --update parameter.");
          clusterPluginFailed = true;
          continue;
        }
        for (PluginMeta pluginMeta: (List<PluginMeta>)deployedPackage.getCustomData()) {
          PackageUtils.printGreen("Updating this plugin: " + pluginMeta);
          try {
            pluginMeta.version = packageInstance.version; // just update the version, let the other metadata same
            String postBody = "{\"update\": " + Utils.toJSONString(pluginMeta) + "}";
            PackageUtils.printGreen("Posting " + postBody + " to " + PackageUtils.CLUSTER_PLUGINS_PATH);
            SolrCLI.postJsonToSolr(solrClient, PackageUtils.CLUSTER_PLUGINS_PATH, postBody);
          } catch (Exception e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
        numberOfClusterPluginsDeployed++;
      }
      if (numberOfClusterPluginsDeployed > 0) {
        PackageUtils.printGreen(numberOfClusterPluginsDeployed + " cluster level plugins updated.");
      } else {
        PackageUtils.printRed("No cluster level plugin updated.");
        clusterPluginFailed = true;
      }
    } else {
      for (Plugin plugin: packageInstance.plugins) {
        if ("cluster".equalsIgnoreCase(plugin.type) == false) continue;
        // Check if this cluster level plugin is already deployed
        {
          Map<String, Object> clusterprops = null;
          try {
            clusterprops = PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + PackageUtils.CLUSTERPROPS_PATH, Map.class);
          } catch (SolrException ex) {
            if (ex.code() == ErrorCode.NOT_FOUND.code) {
              // Ignore this, as clusterprops may not have been created yet. This means package isn't already installed.
            } else throw ex;
          }
          if (clusterprops != null) {
            Object pkg = ((Map<String, Object>)clusterprops.getOrDefault("plugin", Collections.emptyMap())).get(packageInstance.name+":"+plugin.name);
            if (pkg != null) {
              PackageUtils.printRed("Cluster level plugin " + plugin.name + " from package " + packageInstance.name + " already deployed. To update to " + packageInstance + ", pass --update parameter.");
              clusterPluginFailed = true;
              continue;
            }
          }
        }

        // Lets setup this plugin now
        Map<String, String> systemParams = PackageUtils.map("package-name", packageInstance.name, "package-version", packageInstance.version, "plugin-name", plugin.name);
        Command cmd = plugin.setupCommand;
        if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
          if ("POST".equalsIgnoreCase(cmd.method)) {
            try {
              Map<String, String> overridesMap = getParameterOverrides(overrides);
              String payload = PackageUtils.resolve(getMapper().writeValueAsString(cmd.payload), packageInstance.parameterDefaults, overridesMap, systemParams);
              String path = PackageUtils.resolve(cmd.path, packageInstance.parameterDefaults, overridesMap, systemParams);
              PackageUtils.printGreen("Executing " + payload + " for path:" + path);
              boolean shouldExecute = prompt(noprompt);
              if (shouldExecute) {
                SolrCLI.postJsonToSolr(solrClient, path, payload);
                numberOfClusterPluginsDeployed++;
              }
            } catch (Exception ex) {
              throw new SolrException(ErrorCode.SERVER_ERROR, ex);
            }
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Non-POST method not supported for setup commands");
          }
        } else {
          PackageUtils.printRed("There is no setup command to execute for plugin: " + plugin.name);
        }
      }
      if (numberOfClusterPluginsDeployed > 0) {
        PackageUtils.printGreen(numberOfClusterPluginsDeployed + " cluster level plugins setup.");
      } else {
        PackageUtils.printRed("No cluster level plugin setup.");
        clusterPluginFailed = true;
      }
    }
    return !clusterPluginFailed;
  }

  private boolean prompt(boolean noprompt) {
    boolean shouldExecute = true;
    if (!noprompt) { // show a prompt asking user to execute the setup command for the plugin
      PackageUtils.print(PackageUtils.YELLOW, "Execute this command. (If you choose no, you can manually deploy/undeploy this plugin later) (y/n): ");
      try (Scanner scanner = new Scanner(System.in, "UTF-8")) {
        String userInput = scanner.next();
        if ("no".trim().equalsIgnoreCase(userInput) || "n".trim().equalsIgnoreCase(userInput)) {
          shouldExecute = false;
          PackageUtils.printRed("Skipping setup command for deploying (deployment verification may fail)."
              + " Please run this step manually or refer to package documentation.");
        }
      }
    }
    return shouldExecute;
  }


  /**
   * Parse a map of overrides based on user provided values in format "key1=val1"
   */
  private Map<String,String> getParameterOverrides(String[] overrides) {
    return getCollectionParameterOverrides(null, false, overrides, null);
  }
  
  /**
   * Resolve parameter overrides by overlaying provided overrides with collection level overrides already in a deployed package.
   */
  private Map<String,String> getCollectionParameterOverrides(SolrPackageInstance packageInstance, boolean isUpdate, String[] overrides, String collection) {
    Map<String, String> collectionParameterOverrides = isUpdate? getPackageParams(packageInstance.name, collection): new HashMap<String,String>();
    if (overrides != null) {
      for (String override: overrides) {
        collectionParameterOverrides.put(override.split("=")[0], override.split("=")[1]);
      }
    }
    return collectionParameterOverrides;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  Map<String, String> getPackageParams(String packageName, String collection) {
    try {
      return (Map<String, String>)((Map)((Map)((Map)
          PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + PackageUtils.getCollectionParamsPath(collection) + "/packages", Map.class)
          .get("response"))
          .get("params"))
          .get("packages")).get(packageName);
    } catch (Exception ex) {
      // This should be because there are no parameters. Be tolerant here.
      return Collections.emptyMap();
    }
  }

  /**
   * Given a package and list of collections, verify if the package is installed
   * in those collections. It uses the verify command of every plugin in the package (if defined).
   * 
   * @param overrides are needed only when shouldDeployClusterPlugins is true, since collection level plugins will get their overrides from ZK (collection params API)
   */
  public boolean verify(SolrPackageInstance pkg, List<String> collections, boolean shouldDeployClusterPlugins, String overrides[]) {
    boolean success = true;
    for (Plugin plugin: pkg.plugins) {
      Command cmd = plugin.verifyCommand;
      if (plugin.verifyCommand != null && !Strings.isNullOrEmpty(cmd.path)) {
        if ("cluster".equalsIgnoreCase(plugin.type)) {
          if (!shouldDeployClusterPlugins) continue; // Plugins of type "cluster"
          Map<String, String> overridesMap = getParameterOverrides(overrides);
          Map<String, String> systemParams = PackageUtils.map("package-name", pkg.name, "package-version", pkg.version, "plugin-name", plugin.name);
          String url = solrBaseUrl + PackageUtils.resolve(cmd.path, pkg.parameterDefaults, overridesMap, systemParams);
          PackageUtils.printGreen("Executing " + url + " for cluster level plugin");

          if ("GET".equalsIgnoreCase(cmd.method)) {
            String response = PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), url);
            PackageUtils.printGreen(response);
            String actualValue = null;
            try {
              actualValue = JsonPath.parse(response, PackageUtils.jsonPathConfiguration())
                .read(PackageUtils.resolve(cmd.condition, pkg.parameterDefaults, overridesMap, systemParams));
            } catch (PathNotFoundException ex) {
              PackageUtils.printRed("Failed to deploy plugin: " + plugin.name);
              success = false;
            }
            if (actualValue != null) {
              String expectedValue = PackageUtils.resolve(cmd.expected, pkg.parameterDefaults, overridesMap, systemParams);
              PackageUtils.printGreen("Actual: " + actualValue + ", expected: " + expectedValue);
              if (!expectedValue.equals(actualValue)) {
                PackageUtils.printRed("Failed to deploy plugin: " + plugin.name);
                success = false;
              }
            }
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Non-GET method not supported for verify commands");
          }          
        } else {
          // Plugins of type "collection"
          for (String collection: collections) {
            Map<String, String> collectionParameterOverrides = getPackageParams(pkg.name, collection);
  
            Map<String, String> systemParams = PackageUtils.map("collection", collection, "package-name", pkg.name, "package-version", pkg.version, "plugin-name", plugin.name);
            String url = solrBaseUrl + PackageUtils.resolve(cmd.path, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
            PackageUtils.printGreen("Executing " + url + " for collection:" + collection);
  
            if ("GET".equalsIgnoreCase(cmd.method)) {
              String response = PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), url);
              PackageUtils.printGreen(response);
              String actualValue = null;
              try {
                actualValue = JsonPath.parse(response, PackageUtils.jsonPathConfiguration())
                    .read(PackageUtils.resolve(cmd.condition, pkg.parameterDefaults, collectionParameterOverrides, systemParams));
              } catch (PathNotFoundException ex) {
                PackageUtils.printRed("Failed to deploy plugin: " + plugin.name);
                success = false;
              }
              if (actualValue != null) {
                String expectedValue = PackageUtils.resolve(cmd.expected, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
                PackageUtils.printGreen("Actual: " + actualValue + ", expected: "+expectedValue);
                if (!expectedValue.equals(actualValue)) {
                  PackageUtils.printRed("Failed to deploy plugin: " + plugin.name);
                  success = false;
                }
              }
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST, "Non-GET method not supported for verify commands");
            }
          }
        }
      }
    }
    return success;
  }

  /**
   * Get the installed instance of a specific version of a package. If version is null, PackageUtils.LATEST or PackagePluginHolder.LATEST,
   * then it returns the highest version available in the system for the package.
   */
  public SolrPackageInstance getPackageInstance(String packageName, String version) {
    fetchInstalledPackageInstances();
    List<SolrPackageInstance> versions = packages.get(packageName);
    SolrPackageInstance latest = null;
    if (versions != null && !versions.isEmpty()) {
      latest = versions.get(0);
      for (int i=0; i < versions.size(); i++) {
        SolrPackageInstance pkg = versions.get(i);
        if (pkg.version.equals(version)) {
          return pkg;
        }
        if (PackageUtils.compareVersions(latest.version, pkg.version) <= 0) {
          latest = pkg;
        }
      }
    }
    if (version == null || version.equalsIgnoreCase(PackageUtils.LATEST) || version.equalsIgnoreCase(PackageLoader.LATEST)) {
      return latest;
    } else return null;
  }

  /**
   * Deploys a version of a package to a list of collections.
   * @param version If null, the most recent version is deployed. 
   *    EXPERT FEATURE: If version is PackageUtils.LATEST, this collection will be auto updated whenever a newer version of this package is installed.
   * @param isUpdate Is this a fresh deployment or is it an update (i.e. there is already a version of this package deployed on this collection)
   * @param noprompt If true, don't prompt before executing setup commands.
   */
  public void deploy(String packageName, String version, String[] collections, boolean shouldInstallClusterPlugins, String[] parameters,
      boolean isUpdate, boolean noprompt) throws SolrException {
    ensureCollectionsExist(Arrays.asList(collections));

    boolean pegToLatest = PackageUtils.LATEST.equals(version); // User wants to peg this package's version to the latest installed (for auto-update, i.e. no explicit deploy step)
    SolrPackageInstance packageInstance = getPackageInstance(packageName, version);
    if (packageInstance == null) {
      PackageUtils.printRed("Package instance doesn't exist: " + packageName + ":" + version + ". Use install command to install this version first.");
      System.exit(1);
    }
    if (version == null) version = packageInstance.version;

    Manifest manifest = packageInstance.manifest;
    if (PackageUtils.checkVersionConstraint(RepositoryManager.systemVersion, manifest.versionConstraint) == false) {
      PackageUtils.printRed("Version incompatible! Solr version: "
          + RepositoryManager.systemVersion + ", package version constraint: " + manifest.versionConstraint);
      System.exit(1);
    }

    boolean res = deployPackage(packageInstance, pegToLatest, isUpdate, noprompt,
        Arrays.asList(collections), shouldInstallClusterPlugins, parameters);
    PackageUtils.print(res? PackageUtils.GREEN: PackageUtils.RED, res? "Deployment successful": "Deployment failed");
  }

  /**
   * Undeploys a package from given collections.
   */
  public void undeploy(String packageName, String[] collections, boolean shouldUndeployClusterPlugins) throws SolrException {
    ensureCollectionsExist(Arrays.asList(collections));
    
    // Undeploy cluster level plugins
    if (shouldUndeployClusterPlugins) {
      SolrPackageInstance deployedPackage = getPackagesDeployedAsClusterLevelPlugins().get(packageName);
      if (deployedPackage == null) {
        PackageUtils.printRed("Cluster level plugins from package "+packageName+" not deployed.");
      } else {
        for (Plugin plugin: deployedPackage.plugins) {
          if (!shouldUndeployClusterPlugins || "cluster".equalsIgnoreCase(plugin.type) == false) continue;
            
          Map<String, String> systemParams = PackageUtils.map("package-name", deployedPackage.name, "package-version", deployedPackage.version, "plugin-name", plugin.name);
          Command cmd = plugin.uninstallCommand;
          if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
            if ("POST".equalsIgnoreCase(cmd.method)) {
              try {
                String payload = PackageUtils.resolve(getMapper().writeValueAsString(cmd.payload), deployedPackage.parameterDefaults, Collections.emptyMap(), systemParams);
                String path = PackageUtils.resolve(cmd.path, deployedPackage.parameterDefaults, Collections.emptyMap(), systemParams);
                PackageUtils.printGreen("Executing " + payload + " for path:" + path);
                SolrCLI.postJsonToSolr(solrClient, path, payload);
              } catch (Exception ex) {
                throw new SolrException(ErrorCode.SERVER_ERROR, ex);
              }
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST, "Non-POST method not supported for uninstall commands");
            }
          } else {
            PackageUtils.printRed("There is no uninstall command to execute for plugin: " + plugin.name);
          }

        }
      }
    }
    // Undeploy collection level plugins
    for (String collection: collections) {
      SolrPackageInstance deployedPackage = getPackagesDeployed(collection).get(packageName);
      if (deployedPackage == null) {
        PackageUtils.printRed("Package "+packageName+" not deployed on collection "+collection);
        continue;
      }
      Map<String, String> collectionParameterOverrides = getPackageParams(packageName, collection);

      // Run the uninstall command for all plugins
      for (Plugin plugin: deployedPackage.plugins) {
        if ("collection".equalsIgnoreCase(plugin.type) == false) continue;

        Map<String, String> systemParams = PackageUtils.map("collection", collection, "package-name", deployedPackage.name, "package-version", deployedPackage.version, "plugin-name", plugin.name);
        Command cmd = plugin.uninstallCommand;
        if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
          if ("POST".equalsIgnoreCase(cmd.method)) {
            try {
              String payload = PackageUtils.resolve(getMapper().writeValueAsString(cmd.payload), deployedPackage.parameterDefaults, collectionParameterOverrides, systemParams);
              String path = PackageUtils.resolve(cmd.path, deployedPackage.parameterDefaults, collectionParameterOverrides, systemParams);
              PackageUtils.printGreen("Executing " + payload + " for path:" + path);
              SolrCLI.postJsonToSolr(solrClient, path, payload);
            } catch (Exception ex) {
              throw new SolrException(ErrorCode.SERVER_ERROR, ex);
            }
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Non-POST method not supported for uninstall commands");
          }
        } else {
          PackageUtils.printRed("There is no uninstall command to execute for plugin: " + plugin.name);
        }
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, PackageUtils.getCollectionParamsPath(collection),
            "{set: {PKG_VERSIONS: {"+packageName+": null}}}"); // Is it better to "unset"? If so, build support in params API for "unset"
        SolrCLI.postJsonToSolr(solrClient, PackageUtils.PACKAGE_PATH, "{\"refresh\": \"" + packageName + "\"}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }

      // TODO: Also better to remove the package parameters PKG_VERSION etc.
    }
  }

  /**
   * Given a package, return a map of collections where this package is
   * installed to the installed version (which can be {@link PackageLoader#LATEST})
   */
  public Map<String, String> getDeployedCollections(String packageName) {
    List<String> allCollections;
    try {
      allCollections = zkClient.getChildren(ZkStateReader.COLLECTIONS_ZKNODE, null, true);
    } catch (KeeperException | InterruptedException e) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, e);
    }
    Map<String, String> deployed = new HashMap<String, String>();
    for (String collection: allCollections) {
      // Check package version installed
      String paramsJson = PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), solrBaseUrl + PackageUtils.getCollectionParamsPath(collection) + "/PKG_VERSIONS?omitHeader=true");
      String version = null;
      try {
        version = JsonPath.parse(paramsJson, PackageUtils.jsonPathConfiguration())
            .read("$['response'].['params'].['PKG_VERSIONS'].['"+packageName+"'])");
      } catch (PathNotFoundException ex) {
        // Don't worry if PKG_VERSION wasn't found. It just means this collection was never touched by the package manager.
      }
      if (version != null) {
        deployed.put(collection, version);
      }
    }
    return deployed;
  }

}
