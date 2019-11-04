package org.apache.solr.packagemanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
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
    SolrPackageInstance existingPlugin = packageManager.getPackage(id, version);
    if (existingPlugin != null && version.equals(existingPlugin.getVersion())) {
      throw new PackageManagerException("Plugin already installed.");
    }

    SolrPackage pkg = getPackagesMap().get(id);
    SolrPackageRelease release = findReleaseForPackage(id, version);
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

    Package.AddVersion add = new Package.AddVersion();
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

  protected Path downloadPackage(String id, String version) throws PackageManagerException {
    try {
      SolrPackageRelease release = findReleaseForPackage(id, version);
      Path downloaded = getFileDownloader(id).downloadFile(new URL(release.url));
      //getFileVerifier(id).verify(new FileVerifier.Context(id, release), downloaded);
      //nocommit verify this download
      return downloaded;
    } catch (IOException e) {
      throw new PackageManagerException("Error during download of plugin " + id, e);
    }
  }

  protected SimpleFileDownloader getFileDownloader(String pluginId) {
    for (SolrPackageRepository ur : repositories) {
      if (ur.getPlugin(pluginId) != null && ur.getFileDownloader() != null) {
        return ur.getFileDownloader();
      }
    }

    return new SimpleFileDownloader();
  }

  public SolrPackageRelease findReleaseForPackage(String id, String version) throws PackageManagerException {
    SolrPackage pkg = getPackagesMap().get(id);
    if (pkg == null) {
      log.info("Plugin with id {} does not exist in any repository", id);
      throw new PackageManagerException("Plugin with id "+id+" not found in any repository");
    }

    if (version == null) {
      return getLastPackageRelease(id);
    }

    for (SolrPackageRelease release : pkg.versions) {
      if (versionManager.compareVersions(version, release.version) == 0 && release.url != null) {
        return release;
      }
    }

    throw new PackageManagerException("Plugin "+id+" with version @"+version+" does not exist in the repository");
  }

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

  public boolean hasPackageUpdate(String id) {
    SolrPackage pkg = getPackagesMap().get(id);
    if (pkg == null) {
      return false;
    }

    String installedVersion = packageManager.getPackage(id, null).getVersion();
    SolrPackageRelease last = getLastPackageRelease(id);

    return last != null && versionManager.compareVersions(last.version, installedVersion) > 0;
  }


  public List<SolrPackage> getUpdates() {
    List<SolrPackage> updates = new ArrayList<>();
    for (SolrPackageInstance installed : packageManager.getPackages()) {
      String pluginId = installed.getPluginId();
      if (hasPackageUpdate(pluginId)) {
        updates.add(getPackagesMap().get(pluginId));
      }
    }

    return updates;
  }

  public boolean hasUpdates() {
    return getUpdates().size() > 0;
  }
}
