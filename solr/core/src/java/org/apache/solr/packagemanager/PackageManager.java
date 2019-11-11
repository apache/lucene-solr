package org.apache.solr.packagemanager;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.packagemanager.SolrPackage.Command;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.packagemanager.SolrPackage.Plugin;
import org.apache.solr.util.SolrCLI;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * Handles most of the management of packages that as installed in Solr.
 */
public class PackageManager implements Closeable {

  final String solrBaseUrl;
  final HttpSolrClient solrClient;
  final SolrZkClient zkClient;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public PackageManager(HttpSolrClient solrClient, String solrBaseUrl, String zkHost) {
    this.solrBaseUrl = solrBaseUrl;
    this.solrClient = solrClient;
    this.zkClient = new SolrZkClient(zkHost, 30000);
    log.info("Done initializing a zkClient instance...");
  }

  private Map<String, List<SolrPackageInstance>> packages = null;

  public List<SolrPackageInstance> fetchInstalledPackageInstances() throws SolrException {
    log.info("Getting packages from packages.json...");
    List<SolrPackageInstance> ret = new ArrayList<SolrPackageInstance>();
    packages = new HashMap<String, List<SolrPackageInstance>>();
    try {
      Map packagesZnodeMap = null;

      // nocommit create a bean representing the structure of the packages.json
      if (zkClient.exists("/packages.json", true) == true) {
        packagesZnodeMap = (Map)new ObjectMapper().readValue(
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

  // nocommit create a bean for this response?
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

  public Map<String, SolrPackageInstance> getPackagesDeployed(String collection) {
    String paramsJson = PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl+"/api/collections/"+collection+"/config/params/PKG_VERSIONS?omitHeader=true");
    Map<String, String> packages = null;
    try {
      packages = JsonPath.parse(paramsJson, PackageUtils.jsonPathConfiguration())
          .read("$['response'].['params'].['PKG_VERSIONS'])", Map.class);
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

  public boolean deployPackage(SolrPackageInstance packageInstance, boolean pegToLatest, boolean isUpdate, List<String> collections, String overrides[]) {
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

      // nocommit factor this shit away
      Map<String, String> collectionParameterOverrides = isUpdate? getPackageParams(packageInstance.name, collection): new HashMap<String,String>();
      if (overrides != null) {
        for (String override: overrides) {
          collectionParameterOverrides.put(override.split("=")[0], override.split("=")[1]);
        }
      }

      // Get package params
      try {
        boolean packageParamsExist = ((Map)PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/abc/config/params/packages", Map.class)
            .getOrDefault("response", Collections.emptyMap())).containsKey("params");
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            new ObjectMapper().writeValueAsString(Collections.singletonMap(packageParamsExist? "update": "set",
                Collections.singletonMap("packages", Collections.singletonMap(packageInstance.name, collectionParameterOverrides)))));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            "{set:{PKG_VERSIONS:{" + packageInstance.name+": '" + (pegToLatest? "$LATEST": packageInstance.version)+"'}}}");
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

        for (Plugin plugin: packageInstance.getPlugins()) {
          Command cmd = plugin.setupCommand;
          if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
            if ("POST".equalsIgnoreCase(cmd.method)) {
              try {
                String payload = PackageUtils.resolve(new ObjectMapper().writeValueAsString(cmd.payload), packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
                String path = PackageUtils.resolve(cmd.path, packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
                PackageUtils.printGreen("Executing " + payload + " for path:" + path);
                // nocommit prompt and better message
                SolrCLI.postJsonToSolr(solrClient, path, payload);
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
            "{update:{PKG_VERSIONS:{'"+packageInstance.name+"' : '"+(pegToLatest? "$LATEST": packageInstance.version)+"'}}}");
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

  //nocommit should this be private?
  public boolean verify(SolrPackageInstance pkg, List<String> collections) {
    // verify deployment succeeded?
    boolean success = true;
    for (Plugin p: pkg.getPlugins()) {
      PackageUtils.printGreen(p.verifyCommand);
      for (String collection: collections) {
        // nocommit print the resolved command
        PackageUtils.printGreen("Executing " + p.verifyCommand + " for collection:" + collection);
        Map<String, String> collectionParameterOverrides = getPackageParams(pkg.name, collection);

        Command cmd = p.verifyCommand;

        Map<String, String> systemParams = new HashMap<String,String>();
        systemParams.put("collection", collection);
        systemParams.put("package-name", pkg.name);
        systemParams.put("package-version", pkg.version);
        String url = solrBaseUrl + PackageUtils.resolve(cmd.path, pkg.parameterDefaults, collectionParameterOverrides, systemParams);

        if ("GET".equalsIgnoreCase(cmd.method)) {
          String response = PackageUtils.getJson(solrClient.getHttpClient(), url);
          PackageUtils.printGreen(response);
          String actualValue = JsonPath.parse(response, PackageUtils.jsonPathConfiguration())
              .read(PackageUtils.resolve(cmd.condition, pkg.parameterDefaults, collectionParameterOverrides, systemParams));
          String expectedValue = PackageUtils.resolve(cmd.expected, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
          PackageUtils.printGreen("Actual: "+actualValue+", expected: "+expectedValue);
          if (!expectedValue.equals(actualValue)) {
            PackageUtils.printRed("Failed to deploy plugin: "+p.name);
            success = false;
          }
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Non-GET method not supported for setup commands");
        }
      }
    }
    return success;
  }

  // nocommit: javadocs should mention that version==null or "latest" will return latest version installed
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
    if (version == null || version.equalsIgnoreCase("latest") || version.equalsIgnoreCase("$LATEST")) {
      return latest;
    } else return null;
  }

  @Override
  public void close() throws IOException {
    if (zkClient != null) {
      zkClient.close();
    }
    if (solrClient != null) {
      solrClient.close();
    }
  }

  public void deploy(String packageName, String version, boolean isUpdate, String[] collections, String[] parameters) throws SolrException {
    boolean pegToLatest = "latest".equals(version); // User wants to peg this package's version to the latest installed (for auto-update, i.e. no explicit deploy step)
    SolrPackageInstance packageInstance = getPackageInstance(packageName, version);
    // nocommit if not found, exception here
    if (version == null) version = packageInstance.getVersion();

    Manifest manifest = packageInstance.manifest;
    if (PackageUtils.checkVersionConstraint(RepositoryManager.systemVersion, manifest.minSolrVersion, manifest.maxSolrVersion) == false) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Version incompatible! Solr version: "
          + RepositoryManager.systemVersion+", package minSolrVersion: " + manifest.minSolrVersion
          + ", maxSolrVersion: "+manifest.maxSolrVersion);
    }

    PackageUtils.printGreen(deployPackage(packageInstance, pegToLatest, isUpdate,
        Arrays.asList(collections), parameters));
  }

  public void undeploy(String packageName, String[] collections) throws SolrException {
    for (String collection: collections) {
      SolrPackageInstance deployedPackage = getPackagesDeployed(collection).get(packageName);
      Map<String, String> collectionParameterOverrides = getPackageParams(packageName, collection);

      // Get package params
      try {
        boolean packageParamsExist = ((Map)PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/abc/config/params/packages", Map.class)
            .getOrDefault("response", Collections.emptyMap())).containsKey("params");
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            new ObjectMapper().writeValueAsString(Collections.singletonMap(packageParamsExist? "update": "set",
                Collections.singletonMap("packages", Collections.singletonMap(packageName, collectionParameterOverrides)))));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }


      // Run the uninstall command for all plugins
      Map<String, String> systemParams = Map.of("collection", collection, "package-name", deployedPackage.name, "package-version", deployedPackage.version);

      for (Plugin plugin: deployedPackage.getPlugins()) {
        Command cmd = plugin.uninstallCommand;
        if (cmd != null && !Strings.isNullOrEmpty(cmd.method)) {
          if ("POST".equalsIgnoreCase(cmd.method)) {
            try {
              String payload = PackageUtils.resolve(new ObjectMapper().writeValueAsString(cmd.payload), deployedPackage.parameterDefaults, collectionParameterOverrides, systemParams);
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

    }
  }

  public void listInstalled(List args) {
    for (SolrPackageInstance pkg: fetchInstalledPackageInstances()) {
      PackageUtils.printGreen(pkg.getPackageName()+" ("+pkg.getVersion()+")");
    }
  }

  // javadoc
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
      String paramsJson = PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/" + collection + "/config/params/PKG_VERSIONS?omitHeader=true");
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
