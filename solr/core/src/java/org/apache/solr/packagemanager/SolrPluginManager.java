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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.packagemanager.SolrPluginInfo.Metadata;
import org.apache.solr.packagemanager.SolrPluginInfo.Plugin;
import org.apache.solr.packagemanager.pf4j.DefaultVersionManager;
import org.apache.solr.packagemanager.pf4j.VersionManager;

import com.google.gson.Gson;

public 	class SolrPluginManager {

  final VersionManager versionManager;

  public SolrPluginManager(File repo) {
    versionManager = new DefaultVersionManager();
  }

  Map<String, SolrPluginWrapper> packages = null;

  Metadata fetchMetadata(String blobSha256) throws MalformedURLException, IOException {
    String metadataJson = 
        IOUtils.toString(new URL("http://localhost:8983/api/node/blob"+"/"+blobSha256).openStream(), "UTF-8");
    System.out.println("Fetched metadata blob: "+metadataJson);
    Metadata metadata = new Gson().fromJson(metadataJson, Metadata.class);
    System.out.println("Now metadata: "+metadata);
    return metadata;
  }

  public List<SolrPluginWrapper> getPlugins() {
    System.out.println("Getting packages from clusterprops...");
    List<SolrPluginWrapper> ret = new ArrayList<SolrPluginWrapper>();
    packages = new HashMap<String, SolrPluginWrapper>();
    try {
      String clusterPropsZnode = IOUtils.toString(new URL("http://localhost:8983/solr/admin/zookeeper?detail=true&path=/clusterprops.json&wt=json").openStream(), "UTF-8");
      String clusterPropsJson = ((Map)new Gson().fromJson(clusterPropsZnode, Map.class).get("znode")).get("data").toString();
      Map packagesJson = (Map)new Gson().fromJson(clusterPropsJson, Map.class).get("packages");

      System.out.println("clusterprops are: "+clusterPropsJson);
      for (Object packageName: packagesJson.keySet()) {
        Map pkg = (Map)packagesJson.get(packageName);
        List<Plugin> solrplugins = fetchMetadata(pkg.get("metadata").toString()).plugins;
        SolrPluginDescriptor descriptor = new SolrPluginDescriptor(pkg.get("name").toString(), null, 
            pkg.get("version").toString(), solrplugins);
        ClassLoader abc;
        SolrPluginWrapper wrapper = new SolrPluginWrapper(this, descriptor, null, null);
        packages.put(packageName.toString(), wrapper);
        ret.add(wrapper);
      }
    } catch (IOException e) {
      e.printStackTrace();
      if (packages == null) packages = Collections.emptyMap(); // nocommit can't happen
    }
    return ret;
  }

  public boolean deployInstallPlugin(String pluginId, List<String> collections) {
    SolrPluginWrapper plugin = getPlugin(pluginId);

    System.out.println("1: "+plugin);
    System.out.println("2: "+plugin.getDescriptor());
    System.out.println("3: "+((SolrPluginDescriptor)plugin.getDescriptor()).getPlugins());
    for (Plugin p: ((SolrPluginDescriptor)plugin.getDescriptor()).getPlugins()) {
      System.out.println(p.setupCommand);
      for (String collection: collections) {
        System.out.println("Executing " + p.setupCommand + " for collection:" + collection);
        postJson("http://localhost:8983/solr/"+collection+"/config", p.setupCommand);
      }
    }
    return true;
  }

  public boolean deployUpdatePlugin(String pluginId, List<String> collections) {
    SolrPluginWrapper plugin = getPlugin(pluginId);
    for (Plugin p: ((SolrPluginDescriptor)plugin.getDescriptor()).getPlugins()) {

      System.out.println(p.updateCommand);
      for (String collection: collections) {
        System.out.println("Executing " + p.updateCommand + " for collection:" + collection);
        postJson("http://localhost:8983/solr/"+collection+"/config", p.updateCommand);
      }
    }
    return true;
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

  public SolrPluginWrapper getPlugin(String pluginId) {
    getPlugins();
    return packages.get(pluginId);
  }
}
