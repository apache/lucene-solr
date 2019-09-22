package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.packagemanager.SolrPluginInfo.Metadata;
import org.apache.solr.packagemanager.SolrPluginInfo.SolrPluginRelease;
import org.pf4j.PluginException;
import org.pf4j.PluginManager;
import org.pf4j.PluginWrapper;
import org.pf4j.update.PluginInfo;
import org.pf4j.update.UpdateManager;
import org.pf4j.update.UpdateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class SolrUpdateManager extends UpdateManager {

  final private PluginManager pluginManager;
  final private String repositoriesJsonStr;

  private static final Logger log = LoggerFactory.getLogger(SolrUpdateManager.class);

  public SolrUpdateManager(PluginManager pluginManager, String repositoriesJsonStr) {
    super(pluginManager, (Path)Paths.get("."));
    this.pluginManager = pluginManager;
    this.repositoriesJsonStr = repositoriesJsonStr;
  }

  @Override
  protected synchronized void initRepositoriesFromJson() {
    UpdateRepository items[] = new Gson().fromJson(this.repositoriesJsonStr, SolrUpdateRepository[].class);
    this.repositories = Arrays.asList(items);
  }

  @Override
  public synchronized void refresh() {
    initRepositoriesFromJson();
    for (UpdateRepository updateRepository : repositories) {
      updateRepository.refresh();
    }
  }


  @Override
  public synchronized boolean installPlugin(String id, String version) throws PluginException {
    return updateOrInstallPackage(Operation.INSTALL, id, version);
  }

  @Override
  public synchronized boolean updatePlugin(String id, String version) throws PluginException {
    return updateOrInstallPackage(Operation.UPDATE, id, version);
  }
  
  
  private boolean updateOrInstallPackage(Operation op, String id, String version) throws PluginException {
    //System.out.println("ENTERS HERE");
    // Download to temporary location
    Path downloaded = downloadPlugin(id, version);
    //System.out.println("Downloaded in "+downloaded);
    Path pluginsRoot = pluginManager.getPluginsRoot();

    PluginWrapper existingPlugin = pluginManager.getPlugin(id);
    if (existingPlugin != null && version.equals(existingPlugin.getDescriptor().getVersion())) {
      throw new PluginException("Plugin already installed.");
    }

    SolrPluginRelease release = null;
    String repository = null;
    for (PluginInfo info: getPlugins()) {
      if (id.equals(info.id)) {
        for (SolrPluginRelease r: ((SolrPluginInfo)info).versions) {
          if (version.equals(r.version) ) {
            release = r;
            repository = info.getRepositoryId();
            break;
          }
        }
      }
    }

    if (release == null) {
      throw new PluginException("Couldn't find the release..");
    }

    String sha256 = uploadToBlobHandler(downloaded);

    addOrUpdatePackage(op, id, version, sha256, repository, release.metadata, "some-signature");
    return true;
  }

  public static enum Operation {
    INSTALL, UPDATE;
  }
  
  private boolean addOrUpdatePackage(Operation op, String id, String version, String sha256, String repository, Metadata packageMetadata, String sig) {

    String json;
    
    if (op.equals(Operation.INSTALL)) {
      json = "{add: ";
    } else {
      json = "{update: ";
    }
    json = json
        + "{name: '"+id+"', "
        + "version: '"+version+"', "
        + "repository: '"+repository+"', "
        + "sha256: '"+sha256+"', "
        + "setup-commands: "+new Gson().toJson(setupCommands)+","
        + "update-commands: "+new Gson().toJson(updateCommands)
        + "}}";

    try (CloseableHttpClient client = HttpClients.createDefault();) {
      HttpPost httpPost = new HttpPost("http://localhost:8983/api/cluster/package");
      StringEntity entity = new StringEntity(json);
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


    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

  private String uploadToBlobHandler(Path downloaded) {
    String url = "http://localhost:8983/api/cluster/blob";
    File file = downloaded.toFile();
    try (CloseableHttpClient client = HttpClients.createDefault();) {
      HttpPost post = new HttpPost(url);

      MultipartEntity entity = new MultipartEntity();
      entity.addPart("file", new FileBody(file));
      post.setEntity(entity);

      try {
        HttpResponse response = client.execute(post);

        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
          InputStream is = rspEntity.getContent();
          StringWriter writer = new StringWriter();
          IOUtils.copy(is, writer, "UTF-8");
          String results = writer.toString();
          System.out.println(results);
          String sha = new Gson().fromJson(results, Map.class).get("sha256").toString();
          //System.out.println("SHA: "+sha);
          return sha;
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return null;
  }

}
