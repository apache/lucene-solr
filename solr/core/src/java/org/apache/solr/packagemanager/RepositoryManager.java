/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.packagemanager;

import static org.apache.solr.packagemanager.PackageUtils.getMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.packagemanager.SolrPackage.Artifact;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.pkg.PackageAPI;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.util.SolrCLI;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles most of the management of repositories and packages present in external repositories.
 */
public class RepositoryManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final private PackageManager packageManager;

  public static final String systemVersion = Version.LATEST.toString();

  final HttpSolrClient solrClient;

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
      items = getMapper().readValue(getRepositoriesJson(packageManager.zkClient), DefaultPackageRepository[].class);
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
  public void addRepository(String repoName, String uri) throws Exception {
    String existingRepositoriesJson = getRepositoriesJson(packageManager.zkClient);
    log.info(existingRepositoriesJson);

    @SuppressWarnings({"unchecked"})
    List<PackageRepository> repos = getMapper().readValue(existingRepositoriesJson, List.class);
    repos.add(new DefaultPackageRepository(repoName, uri));
    if (packageManager.zkClient.exists(PackageUtils.REPOSITORIES_ZK_PATH, true) == false) {
      packageManager.zkClient.create(PackageUtils.REPOSITORIES_ZK_PATH, getMapper().writeValueAsString(repos).getBytes("UTF-8"), CreateMode.PERSISTENT, true);
    } else {
      packageManager.zkClient.setData(PackageUtils.REPOSITORIES_ZK_PATH, getMapper().writeValueAsString(repos).getBytes("UTF-8"), true);
    }

    addKey(IOUtils.toByteArray(new URL(uri + "/publickey.der").openStream()), repoName + ".der");
  }

  public void addKey(byte[] key, String destinationKeyFilename) throws Exception {
    // get solr_home directory from info servlet
    String systemInfoUrl = solrClient.getBaseURL() + "/solr/admin/info/system";
    Map<String,Object> systemInfo = SolrCLI.getJson(solrClient.getHttpClient(), systemInfoUrl, 2, true);
    String solrHome = (String) systemInfo.get("solr_home");
    
    // put the public key into package store's trusted key store and request a sync.
    String path = PackageStoreAPI.KEYS_DIR + "/" + destinationKeyFilename;
    PackageUtils.uploadKey(key, path, Paths.get(solrHome), solrClient);
    PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), solrClient.getBaseURL() + "/api/node/files" + path + "?sync=true");
  }

  private String getRepositoriesJson(SolrZkClient zkClient) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    if (zkClient.exists(PackageUtils.REPOSITORIES_ZK_PATH, true)) {
      return new String(zkClient.getData(PackageUtils.REPOSITORIES_ZK_PATH, null, null, true), "UTF-8");
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
    if (existingPlugin != null && existingPlugin.version.equals(version)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Plugin already installed.");
    }

    SolrPackageRelease release = getPackageRelease(packageName, version);
    List<Path> downloaded = downloadPackageArtifacts(packageName, version);
    // TODO: Should we introduce a checksum to validate the downloading?
    // Currently, not a big problem since signature based checking happens anyway

    try {
      // post the manifest
      PackageUtils.printGreen("Posting manifest...");

      if (release.manifest == null) {
        String manifestJson = PackageUtils.getFileFromJarsAsString(downloaded, "manifest.json");
        if (manifestJson == null) {
          throw new SolrException(ErrorCode.NOT_FOUND, "No manifest found for package: " + packageName + ", version: " + version);
        }
        release.manifest = getMapper().readValue(manifestJson, SolrPackage.Manifest.class);
      }
      String manifestJson = getMapper().writeValueAsString(release.manifest);
      String manifestSHA512 = BlobRepository.sha512Digest(ByteBuffer.wrap(manifestJson.getBytes("UTF-8")));
      PackageUtils.postFile(solrClient, ByteBuffer.wrap(manifestJson.getBytes("UTF-8")),
          String.format(Locale.ROOT, "/package/%s/%s/%s", packageName, version, "manifest.json"), null);

      // post the artifacts
      PackageUtils.printGreen("Posting artifacts...");
      for (int i=0; i<release.artifacts.size(); i++) {
        PackageUtils.postFile(solrClient, ByteBuffer.wrap(FileUtils.readFileToByteArray(downloaded.get(i).toFile())),
            String.format(Locale.ROOT, "/package/%s/%s/%s", packageName, version, downloaded.get(i).getFileName().toString()),
            release.artifacts.get(i).sig
            );
      }

      // Call Package API to add this version of the package
      PackageUtils.printGreen("Executing Package API to register this package...");
      Package.AddVersion add = new Package.AddVersion();
      add.version = version;
      add.pkg = packageName;
      add.files = downloaded.stream().map(
          file -> String.format(Locale.ROOT, "/package/%s/%s/%s", packageName, version, file.getFileName().toString())).collect(Collectors.toList());  
      add.manifest = "/package/" + packageName + "/" + version + "/manifest.json";
      add.manifestSHA512 = manifestSHA512;

      V2Request req = new V2Request.Builder(PackageUtils.PACKAGE_PATH)
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
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error during download of package " + packageName, e);
    }
    throw new SolrException(ErrorCode.NOT_FOUND, "Package not found in any repository.");
  }

  /**
   * Given a package name and version, find the release/version object as found in the repository
   */
  private SolrPackageRelease getPackageRelease(String packageName, String version) throws SolrException {
    SolrPackage pkg = getPackagesMap().get(packageName);
    if (pkg == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Package "+packageName+" not found in any repository");
    }
    if (version == null || PackageUtils.LATEST.equals(version)) {
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
    String installedVersion = packageManager.getPackageInstance(packageName, null).version;
    SolrPackageRelease last = getLastPackageRelease(packageName);
    return last != null && PackageUtils.compareVersions(last.version, installedVersion) > 0;
  }

  /**
   * Install a version of the package. Also, run verify commands in case some
   * collection was using {@link PackageLoader#LATEST} version of this package and got auto-updated.
   */
  public boolean install(String packageName, String version) throws SolrException {
    SolrPackageRelease pkg = getLastPackageRelease(packageName);
    if (pkg == null) {
      PackageUtils.printRed("Package " + packageName + " not found in any repository. Check list of available packages via \"solr package list-available\".");
      return false;
    }
    String latestVersion = pkg.version;

    Map<String, String> collectionsDeployedIn = packageManager.getDeployedCollections(packageName);
    List<String> collectionsPeggedToLatest = collectionsDeployedIn.keySet().stream().
        filter(collection -> collectionsDeployedIn.get(collection).equals(PackageLoader.LATEST)).collect(Collectors.toList());
    if (!collectionsPeggedToLatest.isEmpty()) {
      PackageUtils.printGreen("Collections that will be affected (since they are configured to use $LATEST): "+collectionsPeggedToLatest);
    }

    if (version == null || version.equals(PackageUtils.LATEST)) {
      installPackage(packageName, latestVersion);
    } else {
      installPackage(packageName, version);
    }

    if (collectionsPeggedToLatest.isEmpty() == false) {
      SolrPackageInstance updatedPackage = packageManager.getPackageInstance(packageName, PackageUtils.LATEST);
      boolean res = packageManager.verify(updatedPackage, collectionsPeggedToLatest, false, new String[] {}); // Cluster level plugins don't work with peggedToLatest functionality
      PackageUtils.printGreen("Verifying version " + updatedPackage.version + 
          " on " + collectionsPeggedToLatest + ", result: " + res);
      if (!res) {
        PackageUtils.printRed("Failed verification after deployment");
        return false;
      }
    }
    return true;
  }
}
