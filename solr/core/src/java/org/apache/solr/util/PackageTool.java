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
package org.apache.solr.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.packagemanager.PackageUtils;
import org.apache.solr.packagemanager.SolrPackage;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.packagemanager.SolrPackageInstance;
import org.apache.solr.packagemanager.SolrPackageManager;
import org.apache.solr.packagemanager.SolrPackageRepository;
import org.apache.solr.packagemanager.SolrUpdateManager;
import org.apache.solr.util.SolrCLI.StatusTool;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;


public class PackageTool extends SolrCLI.ToolBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public String getName() {
    return "package";
  }

  public static String solrUrl = null;
  public static String solrBaseUrl = null;
  public static HttpSolrClient solrClient;
  public SolrPackageManager packageManager;
  public SolrUpdateManager updateManager;

  @Override
  protected void runImpl(CommandLine cli) throws Exception {
    try {
      // Need a logging free, clean output going through to the user.
      Configurator.setRootLevel(Level.OFF);

      solrUrl = cli.getOptionValues("solrUrl")[cli.getOptionValues("solrUrl").length-1];
      solrBaseUrl = solrUrl.replaceAll("\\/solr$", ""); // strip out ending "/solr"
      log.info("Solr url: "+solrUrl+", solr base url: "+solrBaseUrl);
      solrClient = new HttpSolrClient.Builder(solrBaseUrl).build(); // nocommit close this
      String zkHost = getZkHost(cli);

      log.info("ZK: "+zkHost);
      String cmd = cli.getArgs()[0];

      try (SolrZkClient zkclient = new SolrZkClient(zkHost, 30000)) {
        if (cmd != null) {
          packageManager = new SolrPackageManager(solrClient, solrBaseUrl, zkHost); 
          try {
            updateManager = new SolrUpdateManager(solrClient, packageManager,
                getRepositoriesJson(zkclient), solrBaseUrl);

            switch (cmd) {
              case "add-repo":
                addRepo(zkHost, cli.getArgs()[1], cli.getArgs()[2]);
                break;
              case "list-installed":
                listInstalled(cli.getArgList().subList(1, cli.getArgList().size()));
                break;
              case "list-available":
                try {
                  listAvailable(cli.getArgList().subList(1, cli.getArgList().size()));
                } catch (SolrException ex) {
                  ex.printStackTrace();
                }
                break;
              case "install":
                install(cli.getArgList().subList(1, cli.getArgList().size()));
                break;
              case "deploy":
                String packageName = cli.getArgList().get(1).toString().split(":")[0];
                String version = cli.getArgList().get(1).toString().contains(":")? // nocommit, fix
                    cli.getArgList().get(1).toString().split(":")[1]: null;
                    deploy(packageName, version, cli.hasOption("update"), cli.getOptionValues("collections"), cli.getOptionValues("param"));
                    break;
              case "update":
                if (cli.getArgList().size() == 1) {
                  update();
                } else {
                  updatePackage(zkHost, cli.getArgs()[1], cli.getArgList().subList(2, cli.getArgList().size()));
                }
                break;
              default:
                throw new RuntimeException("Unrecognized command: "+cmd);
            };
          } finally {
            packageManager.close();
          }
        }
      } finally {
        solrClient.close();
      }
      log.info("Finished: "+cmd); // nocommit

    } catch (Exception ex) {
      ex.printStackTrace();
      throw ex;
    }
  }

  protected void addRepo(String zkHost, String name, String uri) throws KeeperException, InterruptedException, MalformedURLException, IOException {
    try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) { // nocommit why not a central zk client?
      String existingRepositoriesJson = getRepositoriesJson(zkClient);
      log.info(existingRepositoriesJson);

      List repos = new ObjectMapper().readValue(existingRepositoriesJson, List.class);
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "SPR is: "+solrClient);
      repos.add(new SolrPackageRepository(name, uri));
      if (zkClient.exists("/repositories.json", true) == false) {
        zkClient.create("/repositories.json", new ObjectMapper().writeValueAsString(repos).getBytes(), CreateMode.PERSISTENT, true);
      } else {
        zkClient.setData("/repositories.json", new ObjectMapper().writeValueAsString(repos).getBytes(), true);
      }

      if (zkClient.exists("/keys", true)==false) zkClient.create("/keys", new byte[0], CreateMode.PERSISTENT, true);
      if (zkClient.exists("/keys/exe", true)==false) zkClient.create("/keys/exe", new byte[0], CreateMode.PERSISTENT, true);
      if (zkClient.exists("/keys/exe/"+"pub_key.der", true)==false) zkClient.create("/keys/exe/"+"pub_key.der", new byte[0], CreateMode.PERSISTENT, true);
      zkClient.setData("/keys/exe/"+"pub_key.der", IOUtils.toByteArray(new URL(uri+"/publickey.der").openStream()), true);

      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Added repository: "+name);
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, getRepositoriesJson(zkClient));
    }
  }

  protected String getRepositoriesJson(SolrZkClient zkClient) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    if (zkClient.exists("/repositories.json", true)) {
      return new String(zkClient.getData("/repositories.json", null, null, true), "UTF-8");
    }
    return "[]";
  }

  protected void listInstalled(List args) {
    for (SolrPackageInstance pkg: packageManager.fetchPackages()) {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, pkg.getPackageName()+" ("+pkg.getVersion()+")");
    }
  }
  protected void listAvailable(List args) throws SolrException {
    PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Available packages:\n-----");
    for (SolrPackage pkg: updateManager.getPackages()) {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, pkg.name + " \t\t"+pkg.description);
      for (SolrPackageRelease version: pkg.versions) {
        PackageUtils.postMessage(PackageUtils.GREEN, log, false, "\tVersion: "+version.version);
      }
    }
  }
  protected void install(List args) throws SolrException {
    String pkg = args.get(0).toString().split(":")[0];
    String version = args.get(0).toString().contains(":")? args.get(0).toString().split(":")[1]: null;
    updateManager.updateOrInstallPackage(pkg, version);
    PackageUtils.postMessage(PackageUtils.GREEN, log, false, args.get(0).toString() + " installed.");
  }
  protected void deploy(String packageName, String version, boolean isUpdate,
      String collections[], String parameters[]) throws SolrException {

    boolean pegToLatest = "latest".equals(version); // User wants to peg this package's version to the latest installed (for auto-update, i.e. no explicit deploy step)
    SolrPackageInstance packageInstance = packageManager.getPackageInstance(packageName, version);
    // nocommit if not found, exception here
    if (version == null) version = packageInstance.getVersion();

    Manifest manifest = packageInstance.manifest;
    if (PackageUtils.checkVersionConstraint(SolrUpdateManager.systemVersion, manifest.minSolrVersion, manifest.maxSolrVersion) == false) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Version incompatible! Solr version: "
          + SolrUpdateManager.systemVersion+", package minSolrVersion: " + manifest.minSolrVersion
          + ", maxSolrVersion: "+manifest.maxSolrVersion);
    }


    PackageUtils.postMessage(PackageUtils.GREEN, log, false, packageManager.deployPackage(packageInstance, pegToLatest, isUpdate,
        Arrays.asList(collections), parameters));
  }

  protected void update() throws SolrException {
    if (updateManager.hasUpdates()) {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Available updates:\n-----");

      for (SolrPackage i: updateManager.getUpdates()) {
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

  protected void updatePackage(String zkHost, String packageName, List args) throws SolrException {
    if (updateManager.hasUpdates()) {
      String latestVersion = updateManager.getLastPackageRelease(packageName).version;
      SolrPackageInstance installedPackage = packageManager.getPackageInstance(packageName, "latest");
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Updating ["+packageName+"] from " + installedPackage.getVersion() + " to version "+latestVersion);

      List<String> collectionsDeployedIn = getDeployedCollections(zkHost, packageManager, installedPackage, true);
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Already deployed on collections: "+collectionsDeployedIn);
      updateManager.updateOrInstallPackage(packageName, latestVersion);

      SolrPackageInstance updatedPackage = packageManager.getPackageInstance(packageName, "latest");
      boolean res = packageManager.verify(updatedPackage, collectionsDeployedIn);
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Verifying version "+updatedPackage.getVersion()+" on "+collectionsDeployedIn
          +", result: "+res);
      if (!res) throw new SolrException(ErrorCode.BAD_REQUEST, "Failed verification after deployment");
    } else {
      PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Package "+packageName+" is already up to date.");
    }
  }

  private List<String> getDeployedCollections(String zkHost, SolrPackageManager packageManager, SolrPackageInstance pkg, boolean onlyLatest) {

    List<String> allCollections;
    try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
      allCollections = zkClient.getChildren("/collections", null, true);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    PackageUtils.postMessage(PackageUtils.GREEN, log, false, "Need to verify if these collections have the plugin installed? "+ allCollections);
    List<String> deployed = new ArrayList<String>();
    for (String collection: allCollections) {
      // Check package version installed
      String paramsJson = PackageUtils.getJson(solrClient.getHttpClient(), solrBaseUrl+"/api/collections/"+collection+"/config/params/PKG_VERSIONS?omitHeader=true");
      String version = null;
      try {
        version = JsonPath.parse(paramsJson, PackageUtils.jsonPathConfiguration())
            .read("$['response'].['params'].['PKG_VERSIONS'].['"+pkg.name+"'])");
      } catch (PathNotFoundException ex) {
        // Don't worry if PKG_VERSION wasn't found. It just means this collection was never touched by the package manager.
      }
      if (onlyLatest) {
        if ("$LATEST".equals(version) && packageManager.verify(pkg, Collections.singletonList(collection))) {
          deployed.add(collection);
        } else {
          log.info("Skipping collection: "+collection+", version: "+version);
        }
      } else {
        if (packageManager.verify(pkg, Collections.singletonList(collection))) {
          deployed.add(collection);
        }
      }
    }
    return deployed;
  }

  // nocommit fix the descriptions of all the options
  @SuppressWarnings("static-access")
  public Option[] getOptions() {
    return new Option[] {
        OptionBuilder
        .withArgName("URL")
        .hasArg()
        .isRequired(true)
        .withDescription("Address of the Solr Web application, defaults to: "+SolrCLI.DEFAULT_SOLR_URL)
        .create("solrUrl"),

        OptionBuilder
        .withArgName("COLLECTIONS")
        .hasArgs()
        .isRequired(false)
        .withDescription("Solr URL scheme: http or https, defaults to http if not specified")
        .create("collections"),

        OptionBuilder
        .withArgName("PARAMS")
        .hasArgs()
        .isRequired(false)
        .withDescription("Solr URL scheme: http or https, defaults to http if not specified")
        .withLongOpt("param")
        .create("p"),

        OptionBuilder
        .isRequired(false)
        .withDescription("Solr URL scheme: http or https, defaults to http if not specified")
        .withLongOpt("update")
        .create("u"),

        OptionBuilder
        .isRequired(false)
        .withDescription("Solr URL scheme: http or https, defaults to http if not specified")
        .withLongOpt("auto-update")
        .create(),

    };
  }

  private String getZkHost(CommandLine cli) throws Exception {
    String zkHost = cli.getOptionValue("zkHost");
    if (zkHost != null)
      return zkHost;

    // find it using the localPort

    String systemInfoUrl = solrUrl+"/admin/info/system";
    CloseableHttpClient httpClient = SolrCLI.getHttpClient();
    try {
      // hit Solr to get system info
      Map<String,Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String,Object> status = statusTool.reportStatus(solrUrl+"/", systemInfo, httpClient);
      Map<String,Object> cloud = (Map<String, Object>)status.get("cloud");
      if (cloud != null) {
        String zookeeper = (String) cloud.get("ZooKeeper");
        if (zookeeper.endsWith("(embedded)")) {
          zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
        }
        zkHost = zookeeper;
      }
    } finally {
      HttpClientUtil.close(httpClient);
    }

    return zkHost;
  }
}