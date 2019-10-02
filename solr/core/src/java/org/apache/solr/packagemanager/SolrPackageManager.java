package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.packagemanager.SolrPackage.Command;
import org.apache.solr.packagemanager.SolrPackage.Metadata;
import org.apache.solr.packagemanager.SolrPackage.Plugin;
import org.apache.solr.packagemanager.pf4j.DefaultVersionManager;
import org.apache.solr.packagemanager.pf4j.VersionManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

public class SolrPackageManager {

  final VersionManager versionManager;

  public SolrPackageManager(File repo) {
    versionManager = new DefaultVersionManager();
  }

  Map<String, SolrPackageInstance> packages = null;

  Metadata fetchMetadata(String blobSha256) throws MalformedURLException, IOException {
    String metadataJson = 
        IOUtils.toString(new URL("http://localhost:8983/api/node/blob"+"/"+blobSha256).openStream(), "UTF-8");
    System.out.println("Fetched metadata blob: "+metadataJson);
    Metadata metadata = new ObjectMapper().readValue(metadataJson, Metadata.class);
    System.out.println("Now metadata: "+metadata);
    return metadata;
  }

  public List<SolrPackageInstance> getPlugins() {
    System.out.println("Getting packages from clusterprops...");
    List<SolrPackageInstance> ret = new ArrayList<SolrPackageInstance>();
    packages = new HashMap<String, SolrPackageInstance>();
    try {
      String clusterPropsZnode = IOUtils.toString(new URL("http://localhost:8983/solr/admin/zookeeper?detail=true&path=/clusterprops.json&wt=json").openStream(), "UTF-8");
      String clusterPropsJson = ((Map)new ObjectMapper().readValue(clusterPropsZnode, Map.class).get("znode")).get("data").toString();
      Map packagesJson = (Map)new ObjectMapper().readValue(clusterPropsJson, Map.class).get("packages");

      System.out.println("clusterprops are: "+clusterPropsJson);
      for (Object packageName: packagesJson.keySet()) {
        Map pkg = (Map)packagesJson.get(packageName);
        List<Plugin> solrplugins = fetchMetadata(pkg.get("metadata").toString()).plugins;
        SolrPackageInstance pkgInstance = new SolrPackageInstance(pkg.get("name").toString(), null, 
            pkg.get("version").toString(), solrplugins);
        packages.put(packageName.toString(), pkgInstance);
        ret.add(pkgInstance);
      }
    } catch (IOException e) {
      e.printStackTrace();
      if (packages == null) packages = Collections.emptyMap(); // nocommit can't happen
    }
    return ret;
  }

  String solrBaseUrl = "http://localhost:8983";

  public boolean deployInstallPackage(String pluginId, List<String> collections) {
    SolrPackageInstance pkg = getPackage(pluginId);

    for (Plugin p: pkg.getPlugins()) {
      System.out.println(p.setupCommand);
      for (String collection: collections) {
        System.out.println("Executing " + p.setupCommand + " for collection:" + collection);
        postJson("http://localhost:8983/solr/"+collection+"/config", p.setupCommand);
      }
    }
    
    boolean success = verify(pkg, collections);
    if (success) {
      System.out.println("Deployed and verified package: "+pkg.id+", version: "+pkg.version);
    }
    return success;
  }
  
  //nocommit should this be private?
  public boolean verify(SolrPackageInstance pkg, List<String> collections) {
    // verify deployment succeeded?
    boolean success = true;
    for (Plugin p: pkg.getPlugins()) {
      System.out.println(p.verifyCommand);
      for (String collection: collections) {
        System.out.println("Executing " + p.verifyCommand + " for collection:" + collection);
        //postJson("http://localhost:8983/solr/"+collection+"/config", p.setupCommand);
        Command cmd = p.verifyCommand;
        String url = solrBaseUrl + resolve(cmd.path, collection, pkg.version, pkg.id);

        if ("GET".equalsIgnoreCase(cmd.method)) {
          String response = get(url);
          System.out.println(response);
          String actualValue = JsonPath.parse(response).read(cmd.condition);
          String expectedValue = resolve(cmd.expected, collection, pkg.version, pkg.id);
          System.out.println("Actual: "+actualValue+", expected: "+expectedValue);
          if (!expectedValue.equals(actualValue)) {
            System.out.println("Failed to deploy plugin: "+p.id);
            success = false;
          }
        }
      }
    }
    return success;
  }
  
  private String resolve(String str, String collection, String packageVersion, String packageName) {
    return str.replaceAll("\\{collection\\}", collection)
        .replaceAll("\\{package-version\\}", packageVersion)
        .replaceAll("\\{package-name\\}", packageName);
  }

  public boolean deployUpdatePackage(String pluginId, List<String> collections) {
    SolrPackageInstance pkg = getPackage(pluginId);
    for (Plugin p: pkg.getPlugins()) {

      System.out.println(p.updateCommand);
      for (String collection: collections) {
        System.out.println("Executing " + p.updateCommand + " for collection:" + collection);
        postJson("http://localhost:8983/solr/"+collection+"/config", p.updateCommand);
      }
    }
    boolean success = verify(pkg, collections);
    if (success) {
      System.out.println("Deployed and verified package: "+pkg.id+", version: "+pkg.version);
    }
    return true;
  }


  private String get(String url) {
    try (CloseableHttpClient client = HttpClients.createDefault();) {
      HttpGet httpGet = new HttpGet(url);
      httpGet.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = client.execute(httpGet);

      try {
        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
          InputStream is = rspEntity.getContent();
          StringWriter writer = new StringWriter();
          IOUtils.copy(is, writer, "UTF-8");
          String results = writer.toString();

          return(results);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    return null;
  }

  private void postJson(String url, String postBody) {
    try (CloseableHttpClient client = HttpClients.createDefault();) {
      HttpPost httpPost = new HttpPost(url);
      StringEntity entity = new StringEntity(postBody);
      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = client.execute(httpPost);

      try {
        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
          InputStream is = rspEntity.getContent();
          StringWriter writer = new StringWriter();
          IOUtils.copy(is, writer, "UTF-8");
          String results = writer.toString();
          System.out.println(results);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  public SolrPackageInstance getPackage(String pluginId) {
    getPlugins();
    return packages.get(pluginId);
  }
}
