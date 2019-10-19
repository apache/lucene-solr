package org.apache.solr.packagemanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.ByteBuffer;
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.packagemanager.pf4j.CompoundVerifier;
import org.apache.solr.packagemanager.pf4j.DefaultVersionManager;
import org.apache.solr.packagemanager.pf4j.FileDownloader;
import org.apache.solr.packagemanager.pf4j.FileVerifier;
import org.apache.solr.packagemanager.pf4j.PackageManagerException;
import org.apache.solr.packagemanager.pf4j.SimpleFileDownloader;
import org.apache.solr.pkg.PackageAPI;
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

  final String solrBaseUrl;

  private static final Logger log = LoggerFactory.getLogger(SolrUpdateManager.class);

  public SolrUpdateManager(SolrPackageManager pluginManager, String repositoriesJsonStr, String solrBaseUrl) {
    this.packageManager = pluginManager;
    this.repositoriesJsonStr = repositoriesJsonStr;
    versionManager = new DefaultVersionManager();
    systemVersion = "0.0.0";
    this.solrBaseUrl = solrBaseUrl;
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


  public synchronized boolean installPackage(String id, String version) throws PackageManagerException {
    return updateOrInstallPackage(Operation.INSTALL, id, version);
  }

  public synchronized boolean updatePackage(String id, String version) throws PackageManagerException {
    return updateOrInstallPackage(Operation.UPDATE, id, version);
  }
  
  // nocommit do we need this, when we have a map version of this?
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

  private boolean updateOrInstallPackage(Operation op, String id, String version) throws PackageManagerException {
    /*Path downloaded = downloadPackage(id, version);

    SolrPackageInstance existingPlugin = packageManager.getPackage(id);
    if (existingPlugin != null && version.equals(existingPlugin.getVersion())) {
      throw new PackageManagerException("Plugin already installed.");
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
      throw new PackageManagerException("Couldn't find the release..");
    }

    String sha256 = uploadToBlobHandler(downloaded);
    String metadataSha256;
    try {
      metadataSha256 = uploadToBlobHandler(new ObjectMapper().writeValueAsString(release.metadata));
    } catch (IOException e) {
      throw new PackageManagerException(e);
    }

    addOrUpdatePackage(op, id, version, sha256, repository, release.sig, metadataSha256, release.metadata);
    
    return true;*/
    
    //postFile(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"),
    //    "/package/mypkg/v1.0/runtimelibs.jar",
    //    "j+Rflxi64tXdqosIhbusqi6GTwZq8znunC/dzwcWW0/dHlFGKDurOaE1Nz9FSPJuXbHkVLj638yZ0Lp1ssnoYA=="
    //);

    SolrPackageInstance existingPlugin = packageManager.getPackage(id, version);
    if (existingPlugin != null && version.equals(existingPlugin.getVersion())) {
      throw new PackageManagerException("Plugin already installed.");
    }

    SolrPackage pkg = getPackagesMap().get(id);
    SolrPackageRelease release = findReleaseForPlugin(id, version);
    Path downloaded = downloadPackage(id, version);
    System.out.println("Yahaan file hai: "+downloaded);
    System.out.println("Signature: "+release.sig);
    System.out.println("Filename: "+downloaded.getFileName().toString());
    
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder(solrBaseUrl).build()) {
      // post the metadata
      System.out.println("Posting metadata");
      postFile(solrClient, ByteBuffer.wrap(new ObjectMapper().writeValueAsString(release.metadata).getBytes()),
          "/package/"+id+"/"+version+"/solr-manifest.json",
          null);

      // post the artifacts
      System.out.println("Posting artifacts");
      postFile(solrClient, getFileContent(downloaded.toFile()),
          "/package/"+id+"/"+version+"/"+downloaded.getFileName().toString(),
          release.sig
      );
      
      addOrUpdatePackage(op, solrClient, id, version, new String[] {"/package/"+id+"/"+version+"/"+downloaded.getFileName().toString()}, 
          pkg.getRepositoryId(), release.sig, "/package/"+id+"/"+version+"/solr-manifest.json", null);
    } catch (SolrServerException | IOException e) {
      throw new PackageManagerException(e);
    }
    return false;
  }
  
  public static ByteBuffer getFileContent(File file) throws IOException {
    ByteBuffer jar;
    try (FileInputStream fis = new FileInputStream(file)) {
      byte[] buf = new byte[fis.available()];
      fis.read(buf);
      jar = ByteBuffer.wrap(buf);
    }
    return jar;
  }

  public static void postFile(SolrClient client, ByteBuffer buffer, String name, String sig)
      throws SolrServerException, IOException {
    String resource = "/api/cluster/files" + name;
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (sig != null) {
      params.add("sig", sig);
    }
    V2Response rsp = new V2Request.Builder(resource)
        .withMethod(SolrRequest.METHOD.PUT)
        .withPayload(buffer)
        .forceV2(true)
        .withMimeType("application/octet-stream")
        .withParams(params)
        .build()
        .process(client);
    if (!name.equals(rsp.getResponse().get(CommonParams.FILE))) {
      throw new PackageManagerException("Mismatch in file uploaded. Uploaded: " +
          rsp.getResponse().get(CommonParams.FILE)+", Original: "+name);
    }
  }

  public static enum Operation {
    INSTALL, UPDATE;
  }
  
  private boolean addOrUpdatePackage(Operation op, SolrClient solrClient, String id, String version, String files[], String repository, String sig,
      String manifest, String manifestSHA512) {
    
    /*String json;
    
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
    try (CloseableHttpClient client = createTrustAllHttpClientBuilder()) {
      HttpPost httpPost = new HttpPost(solrBaseUrl + "/api/cluster/package");
      StringEntity entity = new StringEntity(json);
      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");

      try (CloseableHttpResponse response = client.execute(httpPost)) {
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
    } catch (Exception e) {
      e.printStackTrace();
    }*/
    
    PackageAPI.AddVersion add = new PackageAPI.AddVersion();
    add.version = version;
    add.pkg = id;
    add.files = Arrays.asList(files);
    add.manifest = manifest;
    add.manifestSHA512 = "MY_MANIFEST_SHA512";

    V2Request req = new V2Request.Builder("/api/cluster/package")
        .forceV2(true)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(Collections.singletonMap("add", add))
        .build();

    try {
      V2Response resp = req.process(solrClient);
      System.out.println("Response: "+resp.jsonStr());
    } catch (SolrServerException | IOException e) {
      throw new PackageManagerException(e);
    }

    return true;
  }

  public static CloseableHttpClient createTrustAllHttpClientBuilder() throws Exception {
    SSLContextBuilder builder = new SSLContextBuilder();
    builder.loadTrustMaterial(null, (chain, authType) -> true);           
    SSLConnectionSocketFactory sslsf = new 
    SSLConnectionSocketFactory(builder.build(), NoopHostnameVerifier.INSTANCE);
    return HttpClients.custom().setSSLSocketFactory(sslsf).build();
  }
  
  private String uploadToBlobHandler(Path downloaded) throws PackageManagerException {
    String url = solrBaseUrl + "/api/cluster/blob";
    File file = downloaded.toFile();
    try (CloseableHttpClient client = createTrustAllHttpClientBuilder()) { //HttpClients.createDefault();) {
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
        throw e;
      }
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      throw new PackageManagerException(e1);
    }
    return null;
  }
  
  private String uploadToBlobHandler(String json) throws IOException, PackageManagerException {
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
   * @throws PackageManagerException if download failed
   */
  protected Path downloadPackage(String id, String version) throws PackageManagerException {
      try {
          SolrPackageRelease release = findReleaseForPlugin(id, version);
          Path downloaded = getFileDownloader(id).downloadFile(new URL(release.url));
          //getFileVerifier(id).verify(new FileVerifier.Context(id, release), downloaded);
          //nocommit verify this download
          return downloaded;
      } catch (IOException e) {
          throw new PackageManagerException(e, "Error during download of plugin {}", id);
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
   * @throws PackageManagerException if id or version does not exist
   */
  public SolrPackageRelease findReleaseForPlugin(String id, String version) throws PackageManagerException {
      SolrPackage pluginInfo = getPackagesMap().get(id);
      if (pluginInfo == null) {
          log.info("Plugin with id {} does not exist in any repository", id);
          throw new PackageManagerException("Plugin with id {} not found in any repository", id);
      }

      if (version == null) {
          return getLastPackageRelease(id);
      }

      for (SolrPackageRelease release : pluginInfo.versions) {
          if (versionManager.compareVersions(version, release.version) == 0 && release.url != null) {
              return release;
          }
      }

      throw new PackageManagerException("Plugin {} with version @{} does not exist in the repository", id, version);
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

      String installedVersion = packageManager.getPackage(id, null).getVersion();
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
      for (SolrPackageInstance installed : packageManager.getPackages()) {
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
