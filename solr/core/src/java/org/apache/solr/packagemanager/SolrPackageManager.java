package org.apache.solr.packagemanager;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

public class SolrPackageManager implements Closeable {

  final String solrBaseUrl;
  final HttpSolrClient solrClient;
  final SolrZkClient zkClient;
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public SolrPackageManager(HttpSolrClient solrClient, String solrBaseUrl, String zkHost) {
    this.solrBaseUrl = solrBaseUrl;
    this.solrClient = solrClient;
    this.zkClient = new SolrZkClient(zkHost, 30000);
    log.info("Done initializing a zkClient instance...");
  }

  Map<String, List<SolrPackageInstance>> packages = null;

  // nocommit: Add SHA512 checking
  Manifest fetchManifest(String manifestFilePath) throws MalformedURLException, IOException {
    Manifest manifest = PackageUtils.getJson(solrClient.getHttpClient(),
        solrBaseUrl + "/api/node/files" + manifestFilePath, Manifest.class);
    log.info("Now manifest: "+manifest);
    return manifest;
  }

  public List<SolrPackageInstance> fetchPackages() throws SolrException {
    log.info("Getting packages from clusterprops...");
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
            Manifest manifest = fetchManifest(pkgVersion.get("manifest").toString());
            List<Plugin> solrplugins = manifest.plugins;
            SolrPackageInstance pkgInstance = new SolrPackageInstance(packageName.toString(), null, 
                pkgVersion.get("version").toString(), solrplugins, manifest.parameterDefaults);
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
      return (Map<String, String>)((Map)((Map)((Map)
          PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/" + collection + "/config/params/packages", Map.class)
            .get("response"))
              .get("params"))
                .get("packages")).get(packageName);
  }

  public boolean deployPackage(SolrPackageInstance packageInstance, boolean pegToLatest, boolean isUpdate, List<String> collections, String overrides[]) {
    for (String collection: collections) {
      Map<String, String> collectionParameterOverrides = isUpdate? getPackageParams(packageInstance.name, collection): new HashMap<String,String>();
      if (overrides != null) {
        for (String override: overrides) {
          collectionParameterOverrides.put(override.split("=")[0], override.split("=")[1]);
        }
      }

      // Get package params
      try {
        boolean packageParamsExist = ((Map)((Map)
            PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl + "/api/collections/abc/config/params/packages", Map.class)
            ).get("response")).containsKey("params");
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/" + collection + "/config/params",
            new ObjectMapper().writeValueAsString(
                Map.of(packageParamsExist? "update": "set", 
                    Map.of("packages", Map.of(packageInstance.name, collectionParameterOverrides)))));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/"+collection+"/config/params", "{set:{PKG_VERSIONS:{"+packageInstance.name+" : '"+(pegToLatest? "$LATEST": packageInstance.version)+"'}}}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }

      // If updating, refresh the package version for this to take effect
      if (isUpdate || pegToLatest) {
        try {
          SolrCLI.postJsonToSolr(solrClient, "/api/cluster/package", "{\"refresh\" : \""+packageInstance.name+"\"}");
        } catch (Exception ex) {
          throw new SolrException(ErrorCode.SERVER_ERROR, ex);
        }
      }

      // Setup/update all the plugins in the package
      for (Plugin p: packageInstance.getPlugins()) {
        System.out.println(isUpdate? p.updateCommand: p.setupCommand);

        Map<String, String> systemParams = new HashMap<String,String>();
        systemParams.put("collection", collection);
        systemParams.put("package-name", packageInstance.name);
        systemParams.put("package-version", packageInstance.version);

        String cmd = resolve(isUpdate? p.updateCommand: p.setupCommand, packageInstance.parameterDefaults, collectionParameterOverrides, systemParams);
        if (cmd != null && !"".equals(cmd.trim())) {
          try {
            System.out.println("Executing " + cmd + " for collection:" + collection);
            SolrCLI.postJsonToSolr(solrClient, "/solr/"+collection+"/config", cmd);
          } catch (Exception ex) {
            throw new SolrException(ErrorCode.SERVER_ERROR, ex);
          }
        }
      }

      // Set the package version in the collection's parameters
      try {
        SolrCLI.postJsonToSolr(solrClient, "/api/collections/"+collection+"/config/params", "{update:{PKG_VERSIONS:{'"+packageInstance.name+"' : '"+(pegToLatest? "$LATEST": packageInstance.version)+"'}}}");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ex);
      }

    }

    // Verify that package was successfully deployed
    boolean success = verify(packageInstance, collections);
    if (success) {
      System.out.println("Deployed and verified package: " + packageInstance.name + ", version: " + packageInstance.version);
    }
    return success;
  }

  private String resolve(String str, Map<String, String> defaults, Map<String, String> overrides, Map<String, String> systemParams) {
    if (str == null) return null;
    for (String param: defaults.keySet()) {
      str = str.replaceAll("\\$\\{"+param+"\\}", overrides.containsKey(param)? overrides.get(param): defaults.get(param));
    }
    for (String param: overrides.keySet()) {
      str = str.replaceAll("\\$\\{"+param+"\\}", overrides.get(param));
    }
    for (String param: systemParams.keySet()) {
      str = str.replaceAll("\\$\\{"+param+"\\}", systemParams.get(param));
    }
    return str;
  }

  //nocommit should this be private?
  public boolean verify(SolrPackageInstance pkg, List<String> collections) {
    // verify deployment succeeded?
    boolean success = true;
    for (Plugin p: pkg.getPlugins()) {
      System.out.println(p.verifyCommand);
      for (String collection: collections) {
        System.out.println("Executing " + p.verifyCommand + " for collection:" + collection);
        Map<String, String> collectionParameterOverrides = getPackageParams(pkg.name, collection);

        Command cmd = p.verifyCommand;

        Map<String, String> systemParams = new HashMap<String,String>();
        systemParams.put("collection", collection);
        systemParams.put("package-name", pkg.name);
        systemParams.put("package-version", pkg.version);
        String url = solrBaseUrl + resolve(cmd.path, pkg.parameterDefaults, collectionParameterOverrides, systemParams);

        if ("GET".equalsIgnoreCase(cmd.method)) {
          String response = PackageUtils.getJson(solrClient.getHttpClient(), url);
          System.out.println(response);
          String actualValue = JsonPath.parse(response, PackageUtils.jsonPathConfiguration())
              .read(resolve(cmd.condition, pkg.parameterDefaults, collectionParameterOverrides, systemParams));
          String expectedValue = resolve(cmd.expected, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
          System.out.println("Actual: "+actualValue+", expected: "+expectedValue);
          if (!expectedValue.equals(actualValue)) {
            System.out.println("Failed to deploy plugin: "+p.id);
            success = false;
          }
        } // nocommit POST?
      }
    }
    return success;
  }

  // nocommit: javadocs should mention that version==null or "latest" will return latest version installed
  public SolrPackageInstance getPackageInstance(String packageName, String version) {
    fetchPackages();
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
    if (version == null || version.equalsIgnoreCase("latest")) {
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
}
