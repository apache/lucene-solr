package org.apache.solr.packagemanager;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
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
import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.BlobRepository;
import org.apache.solr.packagemanager.SolrPackage.Artifact;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.pkg.PackageAPI;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles most of the management of repositories and packages present in external repositories.
 */
public class RepositoryManager {

  final private PackageManager packageManager;

  public static final String systemVersion = Version.LATEST.toString();

  final HttpSolrClient solrClient;

  private static final Logger log = LoggerFactory.getLogger(RepositoryManager.class);

  public RepositoryManager(HttpSolrClient solrClient, PackageManager packageManager) {
    this.packageManager = packageManager;
    this.solrClient = solrClient;
  }

  public List<SolrPackage> getPackages() {
    List<SolrPackage> list = new ArrayList<>(getPackagesMap().values());
    Collections.sort(list);
    return list;
  }

  /**
   * Get a map of package name to {@link SolrPackage} objects
   */
  public Map<String, SolrPackage> getPackagesMap() {
    Map<String, SolrPackage> packagesMap = new HashMap<>();
    for (PackageRepository repository: getRepositories()) {
      packagesMap.putAll(repository.getPackages());
    }

    return packagesMap;
  }

  /**
   * List of added repositories
   */
  public List<PackageRepository> getRepositories() {
    // TODO: Instead of fetching again and again, we should look for caching this
    PackageRepository items[];
    try {
      items = new ObjectMapper().readValue(getRepositoriesJson(packageManager.zkClient), DefaultPackageRepository[].class);
    } catch (IOException | KeeperException | InterruptedException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    List<PackageRepository> repositories = Arrays.asList(items);

    for (PackageRepository updateRepository: repositories) {
      updateRepository.refresh();
    }

    return repositories;
  }

  /**
   * Add a repository to Solr
   */
  public void addRepository(String name, String uri) throws KeeperException, InterruptedException, MalformedURLException, IOException {
    String existingRepositoriesJson = getRepositoriesJson(packageManager.zkClient);
    log.info(existingRepositoriesJson);

    List repos = new ObjectMapper().readValue(existingRepositoriesJson, List.class);
    repos.add(new DefaultPackageRepository(name, uri));
    if (packageManager.zkClient.exists("/repositories.json", true) == false) {
      packageManager.zkClient.create("/repositories.json", new ObjectMapper().writeValueAsString(repos).getBytes(), CreateMode.PERSISTENT, true);
    } else {
      packageManager.zkClient.setData("/repositories.json", new ObjectMapper().writeValueAsString(repos).getBytes(), true);
    }

    if (packageManager.zkClient.exists("/keys", true)==false) packageManager.zkClient.create("/keys", new byte[0], CreateMode.PERSISTENT, true);
    if (packageManager.zkClient.exists("/keys/exe", true)==false) packageManager.zkClient.create("/keys/exe", new byte[0], CreateMode.PERSISTENT, true);
    if (packageManager.zkClient.exists("/keys/exe/"+name+".der", true)==false) {
      packageManager.zkClient.create("/keys/exe/"+name+".der", new byte[0], CreateMode.PERSISTENT, true);
    }
    packageManager.zkClient.setData("/keys/exe/"+name+".der", IOUtils.toByteArray(new URL(uri+"/publickey.der").openStream()), true);

    PackageUtils.printGreen("Added repository: "+name);
    PackageUtils.printGreen(getRepositoriesJson(packageManager.zkClient));
  }

  private String getRepositoriesJson(SolrZkClient zkClient) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    if (zkClient.exists("/repositories.json", true)) {
      return new String(zkClient.getData("/repositories.json", null, null, true), "UTF-8");
    }
    return "[]";
  }

  /**
   * Install a given package and version from the available repositories to Solr.
   * The various steps for doing so are, briefly, a) find upload a manifest to package store,
   * b) download the artifacts and upload to package store, c) call {@link PackageAPI} to register
   * the package.
   */
  private boolean installPackage(String packageName, String version) throws SolrException {
    SolrPackageInstance existingPlugin = packageManager.getPackageInstance(packageName, version);
    if (existingPlugin != null && existingPlugin.getVersion().equals(version)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Plugin already installed.");
    }

    SolrPackageRelease release = getPackageRelease(packageName, version);
    List<Path> downloaded = downloadPackageArtifacts(packageName, version);
    // TODO: Should we introduce a checksum to validate the downloading?
    // Currently, not a big problem since signature based checking happens anyway

    try {
      // post the manifest
      PackageUtils.printGreen("Posting manifest");

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
          "/package/" + packageName + "/" + version + "/manifest.json", null);

      // post the artifacts
      PackageUtils.printGreen("Posting artifacts");
      for (int i=0; i<release.artifacts.size(); i++) {
        PackageUtils.postFile(solrClient, ByteBuffer.wrap(FileUtils.readFileToByteArray(downloaded.get(i).toFile())),
            "/package/" + packageName + "/"+version + "/" + downloaded.get(i).getFileName().toString(),
            release.artifacts.get(i).sig
            );
      }

      // Call Package API to add this version of the package
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
        PackageUtils.printGreen("Response: "+resp.jsonStr());
      } catch (SolrServerException | IOException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }

    } catch (SolrServerException | IOException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
    return false;
  }

  private List<Path> downloadPackageArtifacts(String packageName, String version) throws SolrException {
    try {
      SolrPackageRelease release = getPackageRelease(packageName, version);
      List<Path> downloadedPaths = new ArrayList<Path>(release.artifacts.size());

      for (PackageRepository repo: getRepositories()) {
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

  /**
   * Given a package name and version, find the release/version object as found in the repository
   */
  private SolrPackageRelease getPackageRelease(String packageName, String version) throws SolrException {
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

  private SolrPackageRelease getLastPackageRelease(SolrPackage pkg) {
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

  /**
   * Is there a version of the package available in the repositories that is more
   * latest than our latest installed version of the package?
   */
  public boolean hasPackageUpdate(String packageName) {
    SolrPackage pkg = getPackagesMap().get(packageName);
    if (pkg == null) {
      return false;
    }
    String installedVersion = packageManager.getPackageInstance(packageName, null).getVersion();
    SolrPackageRelease last = getLastPackageRelease(packageName);
    return last != null && PackageUtils.compareVersions(last.version, installedVersion) > 0;
  }

  /**
   * Print list of available packages
   */
  public void listAvailable() throws SolrException {
    PackageUtils.printGreen("Available packages:\n-----");
    for (SolrPackage pkg: getPackages()) {
      PackageUtils.printGreen(pkg.name + " \t\t"+pkg.description);
      for (SolrPackageRelease version: pkg.versions) {
        PackageUtils.printGreen("\tVersion: "+version.version);
      }
    }
  }

  /**
   * Install a version of the package. Also, run verify commands in case some
   * collection was using "$LATEST" version of this package and got auto-updated.
   */
  public void install(String packageName, String version) throws SolrException {
    String latestVersion = getLastPackageRelease(packageName).version;

    Map<String, String> collectionsDeployedIn = packageManager.getDeployedCollections(packageName);
    List<String> peggedToLatest = collectionsDeployedIn.keySet().stream().
        filter(collection -> collectionsDeployedIn.get(collection).equals("$LATEST")).collect(Collectors.toList());
    if (!peggedToLatest.isEmpty()) {
      PackageUtils.printGreen("Collections that will be affected (since they are configured to use $LATEST): "+peggedToLatest);
    }

    if (version == null || version.equals("latest")) {
      installPackage(packageName, latestVersion);
    } else {
      installPackage(packageName, version);
    }

    SolrPackageInstance updatedPackage = packageManager.getPackageInstance(packageName, "latest");
    boolean res = packageManager.verify(updatedPackage, peggedToLatest);
    PackageUtils.printGreen("Verifying version "+updatedPackage.getVersion()+" on " + peggedToLatest +", result: "+res);
    if (!res) throw new SolrException(ErrorCode.BAD_REQUEST, "Failed verification after deployment");
  }
}
