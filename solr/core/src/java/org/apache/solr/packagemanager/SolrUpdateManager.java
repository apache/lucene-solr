package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
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
import org.apache.solr.packagemanager.SolrPackage.Metadata;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.packagemanager.pf4j.CompoundVerifier;
import org.apache.solr.packagemanager.pf4j.DefaultVersionManager;
import org.apache.solr.packagemanager.pf4j.FileDownloader;
import org.apache.solr.packagemanager.pf4j.FileVerifier;
import org.apache.solr.packagemanager.pf4j.PluginException;
import org.apache.solr.packagemanager.pf4j.SimpleFileDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrUpdateManager {

  final private SolrPackageManager packageManager;
  final private String repositoriesJsonStr;
  protected List<SolrPackageRepository> repositories;
  
  private DefaultVersionManager versionManager;
  private String systemVersion;
  private Map<String, SolrPackageRelease> lastPluginRelease = new HashMap<>();


  private static final Logger log = LoggerFactory.getLogger(SolrUpdateManager.class);

  public SolrUpdateManager(SolrPackageManager pluginManager, String repositoriesJsonStr) {
    this.packageManager = pluginManager;
    this.repositoriesJsonStr = repositoriesJsonStr;
    versionManager = new DefaultVersionManager();
    systemVersion = "0.0.0";
  }

  protected synchronized void initRepositoriesFromJson() {
    SolrPackageRepository items[];
    try {
      items = new ObjectMapper().readValue(this.repositoriesJsonStr, SolrPackageRepository[].class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.repositories = Arrays.asList(items);
  }

  public synchronized void refresh() {
    initRepositoriesFromJson();
    for (SolrPackageRepository updateRepository : repositories) {
      updateRepository.refresh();
    }
  }


  public synchronized boolean installPackage(String id, String version) throws PluginException {
    return updateOrInstallPackage(Operation.INSTALL, id, version);
  }

  public synchronized boolean updatePackage(String id, String version) throws PluginException {
    return updateOrInstallPackage(Operation.UPDATE, id, version);
  }
  
  
  public List<SolrPackage> getPackages() {
    List<SolrPackage> list = new ArrayList<>(getPackagesMap().values());
    Collections.sort(list);

    return list;
  }
  
  public Map<String, SolrPackage> getPackagesMap() {
    Map<String, SolrPackage> packagesMap = new HashMap<>();
    for (SolrPackageRepository repository : getRepositories()) {
      packagesMap.putAll(repository.getPackages());
    }

    return packagesMap;
  }
  
  public List<SolrPackageRepository> getRepositories() {
    refresh();
    return repositories;
  }

  private boolean updateOrInstallPackage(Operation op, String id, String version) throws PluginException {
    Path downloaded = downloadPackage(id, version);

    SolrPackageInstance existingPlugin = packageManager.getPackage(id);
    if (existingPlugin != null && version.equals(existingPlugin.getVersion())) {
      throw new PluginException("Plugin already installed.");
    }

    SolrPackageRelease release = null;
    String repository = null;
    for (SolrPackage pkg: getPackages()) {
      if (id.equals(pkg.id)) {
        for (SolrPackageRelease r: pkg.versions) {
          if (version.equals(r.version) ) {
            release = r;
            repository = pkg.getRepositoryId();
            break;
          }
        }
      }
    }

    if (release == null) {
      throw new PluginException("Couldn't find the release..");
    }

    String sha256 = uploadToBlobHandler(downloaded);
    String metadataSha256;
    try {
      metadataSha256 = uploadToBlobHandler(new ObjectMapper().writeValueAsString(release.metadata));
    } catch (IOException e) {
      throw new PluginException(e);
    }

    addOrUpdatePackage(op, id, version, sha256, repository, release.sig, metadataSha256, release.metadata);
    
    return true;
  }

  public static enum Operation {
    INSTALL, UPDATE;
  }
  
  private boolean addOrUpdatePackage(Operation op, String id, String version, String sha256, String repository, String sig,
      String metadataSha256, Metadata packageMetadata) {
    
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
        + "blob: {sha256: '"+sha256+"', sig: '"+sig+"'}, "
        + "metadata: '"+metadataSha256+"'"
        + "}}";

    System.out.println("Posting package: "+json);
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
          String sha = new ObjectMapper().readValue(results, Map.class).get("sha256").toString();
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
  
  private String uploadToBlobHandler(String json) throws IOException {
    
    System.out.println("Trying to upload the blob: "+json);
    FileUtils.writeStringToFile(new File("tmp-metadata"), json);
    return uploadToBlobHandler(new File("tmp-metadata").toPath());
  }

  /**
   * Downloads a plugin with given coordinates, runs all {@link FileVerifier}s
   * and returns a path to the downloaded file.
   *
   * @param id of plugin
   * @param version of plugin or null to download latest
   * @return Path to file which will reside in a temporary folder in the system default temp area
   * @throws PluginException if download failed
   */
  protected Path downloadPackage(String id, String version) throws PluginException {
      try {
          SolrPackageRelease release = findReleaseForPlugin(id, version);
          Path downloaded = getFileDownloader(id).downloadFile(new URL(release.url));
          //getFileVerifier(id).verify(new FileVerifier.Context(id, release), downloaded);
          //nocommit verify this download
          return downloaded;
      } catch (IOException e) {
          throw new PluginException(e, "Error during download of plugin {}", id);
      }
  }

  /**
   * Finds the {@link FileDownloader} to use for this repository.
   *
   * @param pluginId the plugin we wish to download
   * @return FileDownloader instance
   */
  protected FileDownloader getFileDownloader(String pluginId) {
      for (SolrPackageRepository ur : repositories) {
          if (ur.getPlugin(pluginId) != null && ur.getFileDownloader() != null) {
              return ur.getFileDownloader();
          }
      }

      return new SimpleFileDownloader();
  }

  /**
   * Gets a file verifier to use for this plugin. First tries to use custom verifier
   * configured for the repository, then fallback to the default CompoundVerifier
   *
   * @param pluginId the plugin we wish to download
   * @return FileVerifier instance
   */
  protected FileVerifier getFileVerifier(String pluginId) {
      for (SolrPackageRepository ur : repositories) {
          if (ur.getPlugin(pluginId) != null && ur.getFileVerfier() != null) {
              return ur.getFileVerfier();
          }
      }

      return new CompoundVerifier();
  }
  
  /**
   * Resolves Release from id and version.
   *
   * @param id of plugin
   * @param version of plugin or null to locate latest version
   * @return PluginRelease for downloading
   * @throws PluginException if id or version does not exist
   */
  protected SolrPackageRelease findReleaseForPlugin(String id, String version) throws PluginException {
      SolrPackage pluginInfo = getPackagesMap().get(id);
      if (pluginInfo == null) {
          log.info("Plugin with id {} does not exist in any repository", id);
          throw new PluginException("Plugin with id {} not found in any repository", id);
      }

      if (version == null) {
          return getLastPackageRelease(id);
      }

      for (SolrPackageRelease release : pluginInfo.versions) {
          if (versionManager.compareVersions(version, release.version) == 0 && release.url != null) {
              return release;
          }
      }

      throw new PluginException("Plugin {} with version @{} does not exist in the repository", id, version);
  }
  
  /**
   * Returns the last release version of this plugin for given system version, regardless of release date.
   *
   * @return PluginRelease which has the highest version number
   */
  public SolrPackageRelease getLastPackageRelease(String id) {
      SolrPackage pluginInfo = getPackagesMap().get(id);
      if (pluginInfo == null) {
          return null;
      }

      if (!lastPluginRelease.containsKey(id)) {
          for (SolrPackageRelease release : pluginInfo.versions) {
              if (systemVersion.equals("0.0.0") || versionManager.checkVersionConstraint(systemVersion, release.requires)) {
                  if (lastPluginRelease.get(id) == null) {
                      lastPluginRelease.put(id, release);
                  } else if (versionManager.compareVersions(release.version, lastPluginRelease.get(id).version) > 0) {
                      lastPluginRelease.put(id, release);
                  }
              }
          }
      }

      return lastPluginRelease.get(id);
  }
  
  /**
   * Finds whether the newer version of the plugin.
   *
   * @return true if there is a newer version available which is compatible with system
   */
  public boolean hasPluginUpdate(String id) {
      SolrPackage pluginInfo = getPackagesMap().get(id);
      if (pluginInfo == null) {
          return false;
      }

      String installedVersion = packageManager.getPackage(id).getVersion();
      SolrPackageRelease last = getLastPackageRelease(id);

      return last != null && versionManager.compareVersions(last.version, installedVersion) > 0;
  }

  
  /**
   * Return a list of plugins that are newer versions of already installed plugins.
   *
   * @return list of plugins that have updates
   */
  public List<SolrPackage> getUpdates() {
      List<SolrPackage> updates = new ArrayList<>();
      for (SolrPackageInstance installed : packageManager.getPlugins()) {
          String pluginId = installed.getPluginId();
          if (hasPluginUpdate(pluginId)) {
              updates.add(getPackagesMap().get(pluginId));
          }
      }

      return updates;
  }

  /**
   * Checks if Update Repositories has newer versions of some of the installed plugins.
   *
   * @return true if updates exist
   */
  public boolean hasUpdates() {
      return getUpdates().size() > 0;
  }


}
