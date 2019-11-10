package org.apache.solr.packagemanager;

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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrUpdateManager {

  final private SolrPackageManager packageManager;
  final private String repositoriesJsonStr;
  protected List<SolrPackageRepository> repositories;

  public static final String systemVersion = Version.LATEST.toString();

  final HttpSolrClient solrClient;

  private static final Logger log = LoggerFactory.getLogger(SolrUpdateManager.class);

  public SolrUpdateManager(HttpSolrClient solrClient, SolrPackageManager packageManager, String repositoriesJsonStr, String solrBaseUrl) {
    this.packageManager = packageManager;
    this.repositoriesJsonStr = repositoriesJsonStr;
    this.solrClient = solrClient;
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
    for (SolrPackageRepository updateRepository: repositories) {
      updateRepository.refresh();
    }
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

  public boolean updateOrInstallPackage(String packageName, String version) throws SolrException {
    // nocommit: handle version being null
    SolrPackageInstance existingPlugin = packageManager.getPackageInstance(packageName, version);
    if (existingPlugin != null && existingPlugin.getVersion().equals(version)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Plugin already installed.");
    }

    SolrPackageRelease release = findReleaseForPackage(packageName, version);
    Path downloaded = downloadPackage(packageName, version);
    // nocommit handle a failure in downloading

    try {
      // post the metadata
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Posting metadata");
      
      if (release.manifest == null) {
        String manifestJson = PackageUtils.getFileFromJar(downloaded, "manifest.json");
        if (manifestJson == null) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "No manifest found for package: " + packageName + ", version: " + version);
        }
        release.manifest = new ObjectMapper().readValue(manifestJson, SolrPackage.Manifest.class);
      }
      PackageUtils.postFile(solrClient, ByteBuffer.wrap(new ObjectMapper().writeValueAsString(release.manifest).getBytes()),
            "/package/"+packageName+"/"+version+"/solr-manifest.json", null);
      // nocommit calculate SHA512 after posting?

      // post the artifacts
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Posting artifacts");
      PackageUtils.postFile(solrClient, ByteBuffer.wrap(FileUtils.readFileToByteArray(downloaded.toFile())),
          "/package/"+packageName+"/"+version+"/"+downloaded.getFileName().toString(),
          release.sig
          );

      // Call Package Manager API to add this version of the package
      Package.AddVersion add = new Package.AddVersion();
      add.version = version;
      add.pkg = packageName;
      add.files = Arrays.asList(new String[] {"/package/"+packageName+"/"+version+"/"+downloaded.getFileName().toString()});
      add.manifest = "/package/" + packageName + "/" + version + "/solr-manifest.json";
      add.manifestSHA512 = "MY_MANIFEST_SHA512"; // nocommit: hardcoded sha512 of manifest must go away

      V2Request req = new V2Request.Builder("/api/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("add", add))
          .build();

      try {
        V2Response resp = req.process(solrClient);
        PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Response: "+resp.jsonStr());
      } catch (SolrServerException | IOException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }

    } catch (SolrServerException | IOException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
    return false;
  }

  protected Path downloadPackage(String id, String version) throws SolrException {
    try {
      SolrPackageRelease release = findReleaseForPackage(id, version);

      for (SolrPackageRepository repo: repositories) {
        if (repo.hasPackage(id)) {
          return repo.download(new URL(release.url));
        }
      }
    } catch (IOException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Error during download of package " + id, e);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST, "Package not found in any repository.");
  }

  public SolrPackageRelease findReleaseForPackage(String id, String version) throws SolrException {
    SolrPackage pkg = getPackagesMap().get(id);
    if (pkg == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Package "+id+" not found in any repository");
    }

    if (version == null || "latest".equals(version)) {
      return getLastPackageRelease(pkg);
    }

    for (SolrPackageRelease release : pkg.versions) {
      if (PackageUtils.compareVersions(version, release.version) == 0 && release.url != null) {
        return release;
      }
    }

    throw new SolrException(ErrorCode.BAD_REQUEST, "Package "+id+" with version @"+version+" does not exist in the repository");
  }

  public SolrPackageRelease getLastPackageRelease(String id) {
    SolrPackage pkg = getPackagesMap().get(id);
    if (pkg == null) {
      return null;
    }
    return getLastPackageRelease(pkg);
  }

  public SolrPackageRelease getLastPackageRelease(SolrPackage pkg) {
    SolrPackageRelease latest = null;
    for (SolrPackageRelease release: pkg.versions) {
      if (latest == null) {
        latest = release;
      } else {
        if (PackageUtils.compareVersions(latest.version, release.version) < 0) {
          latest = release;
        }
      }
    }
    return latest;
  }

  public boolean hasPackageUpdate(String id) {
    SolrPackage pkg = getPackagesMap().get(id);
    if (pkg == null) {
      return false;
    }
    String installedVersion = packageManager.getPackageInstance(id, null).getVersion();
    SolrPackageRelease last = getLastPackageRelease(id);
    return last != null && PackageUtils.compareVersions(last.version, installedVersion) > 0;
  }

  public List<SolrPackage> getUpdates() {
    List<SolrPackage> updates = new ArrayList<>();
    for (SolrPackageInstance installed : packageManager.fetchPackages()) {
      String packageName = installed.getPackageName();
      if (hasPackageUpdate(packageName)) {
        updates.add(getPackagesMap().get(packageName));
      }
    }
    return updates;
  }

  public boolean hasUpdates() {
    return getUpdates().size() > 0;
  }
}
