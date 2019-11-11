package org.apache.solr.packagemanager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import org.apache.solr.core.BlobRepository;
import org.apache.solr.packagemanager.SolrPackage.Artifact;
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
    List<Path> downloaded = downloadPackage(packageName, version);
    // nocommit handle a failure in downloading

    try {
      // post the metadata
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Posting metadata");
      
      if (release.manifest == null) {
        String manifestJson = PackageUtils.getFileFromArtifacts(downloaded, "manifest.json");
        if (manifestJson == null) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "No manifest found for package: " + packageName + ", version: " + version);
        }
        release.manifest = new ObjectMapper().readValue(manifestJson, SolrPackage.Manifest.class);
      }
      String manifestJson = new ObjectMapper().writeValueAsString(release.manifest);
      String manifestSHA512 = BlobRepository.sha512Digest(ByteBuffer.wrap(manifestJson.getBytes()));
      PackageUtils.postFile(solrClient, ByteBuffer.wrap(manifestJson.getBytes()),
            "/package/"+packageName+"/"+version+"/manifest.json", null);

      // post the artifacts
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Posting artifacts");
      for (int i=0; i<release.artifacts.size(); i++) {
        PackageUtils.postFile(solrClient, ByteBuffer.wrap(FileUtils.readFileToByteArray(downloaded.get(i).toFile())),
            "/package/" + packageName + "/"+version + "/" + downloaded.get(i).getFileName().toString(),
            release.artifacts.get(i).sig
            );
      }

      // Call Package Manager API to add this version of the package
      Package.AddVersion add = new Package.AddVersion();
      add.version = version;
      add.pkg = packageName;
      add.files = downloaded.stream().map(file -> "/package/" + packageName + "/" + version + "/" + file.getFileName().toString()).collect(Collectors.toList());  
      add.manifest = "/package/" + packageName + "/" + version + "/manifest.json";
      add.manifestSHA512 = manifestSHA512;

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

  protected List<Path> downloadPackage(String packageName, String version) throws SolrException {
    try {
      SolrPackageRelease release = findReleaseForPackage(packageName, version);
      List<Path> downloadedPaths = new ArrayList<Path>(release.artifacts.size());

      for (SolrPackageRepository repo: repositories) {
        if (repo.hasPackage(packageName)) {
          for (Artifact art: release.artifacts) {
            downloadedPaths.add(repo.download(art.url));
          }
          return downloadedPaths;
        }
      }
    } catch (IOException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Error during download of package " + packageName, e);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST, "Package not found in any repository.");
  }

  public SolrPackageRelease findReleaseForPackage(String packageName, String version) throws SolrException {
    SolrPackage pkg = getPackagesMap().get(packageName);
    if (pkg == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Package "+packageName+" not found in any repository");
    }

    if (version == null || "latest".equals(version)) {
      return getLastPackageRelease(pkg);
    }

    for (SolrPackageRelease release : pkg.versions) {
      if (PackageUtils.compareVersions(version, release.version) == 0) {
        return release;
      }
    }

    throw new SolrException(ErrorCode.BAD_REQUEST, "Package " + packageName + ":" + version + " does not exist in any repository.");
  }

  public SolrPackageRelease getLastPackageRelease(String packageName) {
    SolrPackage pkg = getPackagesMap().get(packageName);
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

  public boolean hasPackageUpdate(String packageName) {
    SolrPackage pkg = getPackagesMap().get(packageName);
    if (pkg == null) {
      return false;
    }
    String installedVersion = packageManager.getPackageInstance(packageName, null).getVersion();
    SolrPackageRelease last = getLastPackageRelease(packageName);
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
  
  public void listAvailable(List args) throws SolrException {
    PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Available packages:\n-----");
    for (SolrPackage pkg: getPackages()) {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, pkg.name + " \t\t"+pkg.description);
      for (SolrPackageRelease version: pkg.versions) {
        PackageUtils.postMessage(PackageUtils.GREEN, log, false, "\tVersion: "+version.version);
      }
    }
  }
  
  
  public void install(List args) throws SolrException {
    String pkg = args.get(0).toString().split(":")[0];
    String version = args.get(0).toString().contains(":")? args.get(0).toString().split(":")[1]: null;
    updateOrInstallPackage(pkg, version);
    PackageUtils.postMessage(PackageUtils.GREEN, log, false, args.get(0).toString() + " installed.");
  }

  public void updatePackage(String zkHost, String packageName, List args) throws SolrException {
    if (hasUpdates()) {
      String latestVersion = getLastPackageRelease(packageName).version;

      Map<String, String> collectionsDeployedIn = packageManager.getDeployedCollections(zkHost, packageName);
      List<String> peggedToLatest = collectionsDeployedIn.keySet().stream().
          filter(collection -> collectionsDeployedIn.get(collection).equals("$LATEST")).collect(Collectors.toList());
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Already deployed on collections: "+collectionsDeployedIn);
      updateOrInstallPackage(packageName, latestVersion);

      SolrPackageInstance updatedPackage = packageManager.getPackageInstance(packageName, "latest");
      boolean res = packageManager.verify(updatedPackage, peggedToLatest);
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Verifying version "+updatedPackage.getVersion()+" on "+peggedToLatest
          +", result: "+res);
      if (!res) throw new SolrException(ErrorCode.BAD_REQUEST, "Failed verification after deployment");
    } else {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Package "+packageName+" is already up to date.");
    }
  }

  public void listUpdates() throws SolrException {
    if (hasUpdates()) {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Available updates:\n-----");

      for (SolrPackage i: getUpdates()) {
        SolrPackage plugin = (SolrPackage)i;
        PackageUtils.postMessage(PackageUtils.GREEN, log, false, plugin.name + " \t\t"+plugin.description);
        for (SolrPackageRelease version: plugin.versions) {
          PackageUtils.postMessage(PackageUtils.GREEN, log, false, "\tVersion: "+version.version);
        }
      }
    } else {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "No updates found. System is up to date.");
    }
  }

}
