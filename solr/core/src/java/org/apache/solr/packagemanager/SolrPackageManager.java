package org.apache.solr.packagemanager;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.packagemanager.SolrPackage.Command;
import org.apache.solr.packagemanager.SolrPackage.Metadata;
import org.apache.solr.packagemanager.SolrPackage.Plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

public class SolrPackageManager implements Closeable {

  final String solrBaseUrl;
  
  final SolrZkClient zkClient;
  public SolrPackageManager(String solrBaseUrl, String zkHost) {
    this.solrBaseUrl = solrBaseUrl;
    this.zkClient = new SolrZkClient(zkHost, 30000);
    System.out.println("Done initializing a zkClient instance...");
  }

  Map<String, List<SolrPackageInstance>> packages = null;

  Metadata fetchMetadata(String manifestFilePath) throws MalformedURLException, IOException {
    String metadataJson = PackageUtils.getStringFromStream(solrBaseUrl + "/api/node/files"+manifestFilePath);
    System.out.println("Fetched metadata blob: "+metadataJson);
    Metadata metadata = new ObjectMapper().readValue(metadataJson, Metadata.class);
    System.out.println("Now metadata: "+metadata);
    return metadata;
  }

  public List<SolrPackageInstance> getPackages() throws SolrException {
    System.out.println("Getting packages from clusterprops...");
    List<SolrPackageInstance> ret = new ArrayList<SolrPackageInstance>();
    packages = new HashMap<String, List<SolrPackageInstance>>();
    try {
      String clusterPropsJson = null;
      Map packagesJson = null;
      
      if (zkClient.exists("/packages.json", true) == true) {
        clusterPropsJson = new String(zkClient.getData("/packages.json", null, null, true), "UTF-8");
        System.out.println("clusterprops are: "+clusterPropsJson);
        packagesJson = (Map)new ObjectMapper().
            configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).readValue(clusterPropsJson, Map.class).get("packages");
      }

      if (packagesJson != null) {
        for (Object packageName: packagesJson.keySet()) {
          List pkg = (List)packagesJson.get(packageName);
          for (Map pkgVersion: (List<Map>)pkg) {
            Metadata metadata = fetchMetadata(pkgVersion.get("manifest").toString());
            List<Plugin> solrplugins = metadata.plugins;
            SolrPackageInstance pkgInstance = new SolrPackageInstance(packageName.toString(), null, 
                pkgVersion.get("version").toString(), solrplugins, metadata.parameterDefaults);
            List<SolrPackageInstance> list = packages.containsKey(packageName)? packages.get(packageName): new ArrayList<SolrPackageInstance>();
            list.add(pkgInstance);
            packages.put(packageName.toString(), list);
            ret.add(pkgInstance);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (packages == null) packages = Collections.emptyMap(); // nocommit can't happen
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
    System.out.println("Got packages: "+ret);
    return ret;
  }

  Map<String, String> getPackageParams(String packageName, String collection) {
    try {
      return (Map<String, String>)((Map)((Map)((Map)new ObjectMapper().readValue
          (PackageUtils.get(solrBaseUrl + "/api/collections/" + collection + "/config/params/packages"), Map.class).get("response")).get("params")).get("packages")).get(packageName);
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
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
        boolean packageParamsExist = ((Map)((Map)new ObjectMapper().readValue(
              PackageUtils.get(solrBaseUrl + "/api/collections/abc/config/params/packages"), Map.class)
            ).get("response")).containsKey("params");
        PackageUtils.postJson(solrBaseUrl + "/api/collections/" + collection + "/config/params",
            new ObjectMapper().writeValueAsString(
                Map.of(packageParamsExist? "update": "set", 
                    Map.of("packages", Map.of(packageInstance.name, collectionParameterOverrides)))));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String paramsJson = PackageUtils.get(solrBaseUrl+"/api/collections/"+collection+"/config/params?omitHeader=true");
      System.out.println("Before Posting param: "+paramsJson);

      // Set the package version in the collection's parameters
      PackageUtils.postJson(solrBaseUrl+"/api/collections/"+collection+"/config/params", "{set:{PKG_VERSIONS:{"+packageInstance.name+" : '"+(pegToLatest? "$LATEST": packageInstance.version)+"'}}}");

      paramsJson = PackageUtils.get(solrBaseUrl+"/api/collections/"+collection+"/config/params?omitHeader=true");
      System.out.println("Posted param: "+paramsJson);
      
      // If updating, refresh the package version for this to take effect
      if (isUpdate || pegToLatest) {
        PackageUtils.postJson(solrBaseUrl+"/api/cluster/package", "{\"refresh\" : \""+packageInstance.name+"\"}");
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
          System.out.println("Executing " + cmd + " for collection:" + collection);
          PackageUtils.postJson(solrBaseUrl + "/solr/"+collection+"/config", cmd);
        }
      }

      // Set the package version in the collection's parameters
      PackageUtils.postJson(solrBaseUrl+"/api/collections/"+collection+"/config/params", "{update:{PKG_VERSIONS:{'"+packageInstance.name+"' : '"+(pegToLatest? "$LATEST": packageInstance.version)+"'}}}");
      paramsJson = PackageUtils.get(solrBaseUrl+"/api/collections/"+collection+"/config/params?omitHeader=true");
      System.out.println("Posted param: "+paramsJson);

    }

    // Verify that package was successfully deployed
    boolean success = verify(packageInstance, collections);
    if (success) {
      System.out.println("Deployed and verified package: "+packageInstance.name+", version: "+packageInstance.version);
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
          String response = PackageUtils.get(url);
          System.out.println(response);
          String actualValue = JsonPath.parse(response).read(resolve(cmd.condition, pkg.parameterDefaults, collectionParameterOverrides, systemParams));
          String expectedValue = resolve(cmd.expected, pkg.parameterDefaults, collectionParameterOverrides, systemParams);
          System.out.println("Actual: "+actualValue+", expected: "+expectedValue);
          if (!expectedValue.equals(actualValue)) {
            System.out.println("Failed to deploy plugin: "+p.id);
            success = false;
          }
        } // commit POST?
      }
    }
    return success;
  }

  // nocommit: javadocs should mention that version==null or "latest" will return latest version installed
  public SolrPackageInstance getPackageInstance(String pluginId, String version) {
    getPackages();
    List<SolrPackageInstance> versions = packages.get(pluginId);
    String latestVersion = "0.0.0";
    SolrPackageInstance latest = null;
    if (versions != null) {
      for (SolrPackageInstance pkg: versions) {
        if (pkg.version.equals(version)) {
          return pkg;
        }
        if (PackageUtils.compareVersions(latestVersion, pkg.version) <= 0) {
          latestVersion = pkg.version;
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
  }
}
