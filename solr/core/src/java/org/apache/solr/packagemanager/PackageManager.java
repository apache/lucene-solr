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
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.solr.packagemanager.SolrPackage.Command;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.packagemanager.SolrPackage.Plugin;
import org.apache.solr.pkg.PackagePluginHolder;
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

  public List<SolrPackageInstance> fetchInstalledPackageInstances() throws SolrException {
    log.info("Getting packages from packages.json...");
    List<SolrPackageInstance> ret = new ArrayList<SolrPackageInstance>();
    packages = new HashMap<String, List<SolrPackageInstance>>();
    try {
      Map packagesZnodeMap = null;

      if (zkClient.exists("/packages.json", true) == true) {
        packagesZnodeMap = (Map)getMapper().readValue(
            new String(zkClient.getData("/packages.json", null, null, true), "UTF-8"), Map.class).get("packages");
        for (Object packageName: packagesZnodeMap.keySet()) {
          List pkg = (List)packagesZnodeMap.get(packageName);
          for (Map pkgVersion: (List<Map>)pkg) {
            Manifest manifest = PackageUtils.fetchManifest(solrClient, solrBaseUrl, pkgVersion.get("manifest").toString(), pkgVersion.get("manifestSHA512").toString());
            List<Plugin> solrplugins = manifest.plugins;
            SolrPackageInstance pkgInstance = new SolrPackageInstance(packageName.toString(), null, 
                pkgVersion.get("version").toString(), manifest, solrplugins, manifest.parameterDefaults);
            List<SolrPackageInstance> list = packages.containsKey(packageName)? packages.get(packageName): new ArrayList<SolrPackageInstance>();
            list.add(pkgInstance);
            packages.put(packageName.toString(), list);
            ret.add(pkgInstance);
          }
        }
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
    log.info("Got packages: "+ret);
    return ret;
  }

  public Map<String, SolrPackageInstance> getPackagesDeployed(String collection) {
    Map<String, String> packages = null;
    try {
      NavigableObject result = (NavigableObject) Utils.executeGET(solrClient.getHttpClient(),
          solrBaseUrl+"/api/collections/"+collection+"/config/params/PKG_VERSIONS?omitHeader=true&wt=javabin", Utils.JAVABINCONSUMER);
      packages = (Map<String, String>) result._get("/response/params/PKG_VERSIONS", Collections.emptyMap());
    } catch (PathNotFoundException ex) {
      // Don't worry if PKG_VERSION wasn't found. It just means this collection was never touched by the package manager.
    }
    if (packages == null) return Collections.emptyMap();
    Map<String, SolrPackageInstance> ret = new HashMap<String, SolrPackageInstance>();
    for (String packageName: packages.keySet()) {
      if (Strings.isNullOrEmpty(packageName) == false) { // There can be an empty key, storing the version here
        ret.put(packageName, getPackageInstance(packageName, packages.get(packageName)));
      }
    }
    return ret;
  }

  private boolean deployPackage(SolrPackageInstance packageInstance, boolean pegToLatest, boolean isUpdate, boolean noprompt,
      List<String> collections, String overrides[]) {
    for (String collection: collections) {

      SolrPackageInstance deployedPackage = getPackagesDeployed(collection).get(packageInstance.name);
      if (packageInstance.equals(deployedPackage)) {
        if (!pegToLatest) {
          PackageUtils.printRed("Package " + packageInstance + " already deployed on "+collection);
          continue;
        }
      } else {
        if (deployedPackage != null && !isUpdate) {
          PackageUtils.printRed("Package " + deployedPackage + " already deployed on "+collection+". To update to "+packageInstance+", pass --update parameter.");
          continue;
        }
      }

      Map<String,String> collectionParameterOverrides = getCollectionParameterOverrides(packageInstance, isUpdate, overrides, collection);

      // Get package params
      try {
        boolean packageParamsExist = ((Map)PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/abc/config/params/packages", Map.class)
            .getOrDefault("response", Collections.emptyMap())).containsKey("params");
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            getMapper().writeValueAsString(Collections.singletonMap(packageParamsExist? "update": "set",
                Collections.singletonMap("packages", Collections.singletonMap(packageInstance.name, collectionParameterOverrides)))));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            "{set:{PKG_VERSIONS:{" + packageInstance.name+": '" + (pegToLatest? PackagePluginHolder.LATEST: packageInstance.version)+"'}}}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }

      // If updating, refresh the package version for this to take effect
      if (isUpdate || pegToLatest) {
        try {
          SolrCLI.postJsonToSolr(solrClient, "/api/cluster/package", "{\"refresh\": \"" + packageInstance.name + "\"}");
        } catch (Exception ex) {
          throw new SolrException(ErrorCode.SERVER_ERROR, ex);
        }
      }

      // If it is a fresh deploy on a collection, run setup commands all the plugins in the package
      if (!isUpdate) {
        Map<String, String> systemParams = Map.of("collection", collection, "package-name", packageInstance.name, "package-version", packageInstance.version);

        for (Plugin plugin: packageInstance.plugins) {
          Command cmd = plugin.setupCommand;
          if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
            if ("POST".equalsIgnoreCase(cmd.method)) {
              try {
                String payload = PackageUtils.resolve(getMapper().writeValueAsString(cmd.payload), packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
                String path = PackageUtils.resolve(cmd.path, packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
                PackageUtils.printGreen("Executing " + payload + " for path:" + path);
                boolean shouldExecute = true;
                if (!noprompt) { // show a prompt asking user to execute the setup command for the plugin
                  PackageUtils.print(PackageUtils.YELLOW, "Execute this command (y/n): ");
                  String userInput = new Scanner(System.in, "UTF-8").next();
                  if (!"yes".equalsIgnoreCase(userInput) && !"y".equalsIgnoreCase(userInput)) {
                    shouldExecute = false;
                    PackageUtils.printRed("Skipping setup command for deploying (deployment verification may fail)."
                        + " Please run this step manually or refer to package documentation.");
                  }
                }
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
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            "{update:{PKG_VERSIONS:{'" + packageInstance.name + "' : '" + (pegToLatest? PackagePluginHolder.LATEST: packageInstance.version) + "'}}}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }
    }

    // Verify that package was successfully deployed
    boolean success = verify(packageInstance, collections);
    if (success) {
      PackageUtils.printGreen("Deployed and verified package: " + packageInstance.name + ", version: " + packageInstance.version);
    }
    return success;
  }

  private Map<String,String> getCollectionParameterOverrides(SolrPackageInstance packageInstance, boolean isUpdate,
      String[] overrides, String collection) {
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
          PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/" + collection + "/config/params/packages", Map.class)
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
   */
  public boolean verify(SolrPackageInstance pkg, List<String> collections) {
    boolean success = true;
    for (Plugin plugin: pkg.plugins) {
      PackageUtils.printGreen(plugin.verifyCommand);
      for (String collection: collections) {
        Map<String, String> collectionParameterOverrides = getPackageParams(pkg.name, collection);
        Command cmd = plugin.verifyCommand;

        Map<String, String> systemParams = Map.of("collection", collection, "package-name", pkg.name, "package-version", pkg.version);
        String url = solrBaseUrl + PackageUtils.resolve(cmd.path, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
        PackageUtils.printGreen("Executing " + url + " for collection:" + collection);

        if ("GET".equalsIgnoreCase(cmd.method)) {
          String response = PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), url);
          PackageUtils.printGreen(response);
          String actualValue = JsonPath.parse(response, PackageUtils.jsonPathConfiguration())
              .read(PackageUtils.resolve(cmd.condition, pkg.parameterDefaults, collectionParameterOverrides, systemParams));
          String expectedValue = PackageUtils.resolve(cmd.expected, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
          PackageUtils.printGreen("Actual: "+actualValue+", expected: "+expectedValue);
          if (!expectedValue.equals(actualValue)) {
            PackageUtils.printRed("Failed to deploy plugin: " + plugin.name);
            success = false;
          }
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Non-GET method not supported for setup commands");
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
      for (int i=0; i<versions.size(); i++) {
        SolrPackageInstance pkg = versions.get(i);
        if (pkg.version.equals(version)) {
          return pkg;
        }
        if (PackageUtils.compareVersions(latest.version, pkg.version) <= 0) {
          latest = pkg;
        }
      }
    }
    if (version == null || version.equalsIgnoreCase(PackageUtils.LATEST) || version.equalsIgnoreCase(PackagePluginHolder.LATEST)) {
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
  public void deploy(String packageName, String version, String[] collections, String[] parameters,
      boolean isUpdate, boolean noprompt) throws SolrException {
    boolean pegToLatest = PackageUtils.LATEST.equals(version); // User wants to peg this package's version to the latest installed (for auto-update, i.e. no explicit deploy step)
    SolrPackageInstance packageInstance = getPackageInstance(packageName, version);
    if (packageInstance == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Package instance doesn't exist: " + packageName + ":" + null +
          ". Use install command to install this version first.");
    }
    if (version == null) version = packageInstance.version;

    Manifest manifest = packageInstance.manifest;
    if (PackageUtils.checkVersionConstraint(RepositoryManager.systemVersion, manifest.versionConstraint) == false) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Version incompatible! Solr version: "
          + RepositoryManager.systemVersion + ", package version constraint: " + manifest.versionConstraint);
    }

    boolean res = deployPackage(packageInstance, pegToLatest, isUpdate, noprompt,
        Arrays.asList(collections), parameters);
    PackageUtils.print(res? PackageUtils.GREEN: PackageUtils.RED, res? "Deployment successful": "Deployment failed");
  }

  /**
   * Undeploys a packge from given collections.
   */
  public void undeploy(String packageName, String[] collections) throws SolrException {
    for (String collection: collections) {
      SolrPackageInstance deployedPackage = getPackagesDeployed(collection).get(packageName);
      Map<String, String> collectionParameterOverrides = getPackageParams(packageName, collection);

      // Run the uninstall command for all plugins
      Map<String, String> systemParams = Map.of("collection", collection, "package-name", deployedPackage.name, "package-version", deployedPackage.version);

      for (Plugin plugin: deployedPackage.plugins) {
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
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params", "{set: {PKG_VERSIONS: {"+packageName+": null}}}");
        SolrCLI.postJsonToSolr(solrClient, "/api/cluster/package", "{\"refresh\": \"" + packageName + "\"}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }

      // TODO: Also better to remove the package parameters
    }
  }

  /**
   * Given a package, return a map of collections where this package is
   * installed to the installed version (which can be {@link PackagePluginHolder#LATEST})
   */
  public Map<String, String> getDeployedCollections(String packageName) {
    List<String> allCollections;
    try {
      allCollections = zkClient.getChildren("/collections", null, true);
    } catch (KeeperException | InterruptedException e) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, e);
    }
    Map<String, String> deployed = new HashMap<String, String>();
    for (String collection: allCollections) {
      // Check package version installed
      String paramsJson = PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/" + collection + "/config/params/PKG_VERSIONS?omitHeader=true");
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
