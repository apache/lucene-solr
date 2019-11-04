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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.packagemanager.PackageManagerException;
import org.apache.solr.packagemanager.SolrPackage;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.packagemanager.SolrPackageInstance;
import org.apache.solr.packagemanager.SolrPackageManager;
import org.apache.solr.packagemanager.SolrPackageRepository;
import org.apache.solr.packagemanager.SolrUpdateManager;
import org.apache.solr.util.SolrCLI.StatusTool;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;


public class PackageTool extends SolrCLI.ToolBase {
  @Override
  public String getName() {
    return "package";
  }

  public static String solrUrl = null;
  public static String solrBaseUrl = null;

  public SolrPackageManager packageManager;
  public SolrUpdateManager updateManager;

  @Override
  protected void runImpl(CommandLine cli) throws Exception {
    // Need a logging free, clean output going through to the user.
    Configurator.setRootLevel(Level.INFO);

    solrUrl = cli.getOptionValues("solrUrl")[cli.getOptionValues("solrUrl").length-1];
    solrBaseUrl = solrUrl.replaceAll("\\/solr$", ""); // strip out ending "/solr"
    System.out.println("solr url: "+solrUrl+", solr base url: "+solrBaseUrl);

    String zkHost = getZkHost(cli);

    System.out.println("ZK: "+zkHost);
    String cmd = cli.getArgs()[0];

    try (SolrZkClient zkclient = new SolrZkClient(zkHost, 30000)) {
      if (cmd != null) {
        packageManager = new SolrPackageManager(new File("./plugins"), solrBaseUrl, zkHost); 
        try {
          updateManager = new SolrUpdateManager(packageManager,
              getRepositoriesJson(zkclient), solrBaseUrl);

          switch (cmd) {
            case "add-repo":
              addRepo(zkHost, cli.getArgs()[1], cli.getArgs()[2]);
              break;
            case "list":
              list(cli.getArgList().subList(1, cli.getArgList().size()));
              break;
            case "list-available":
              try {
                available(cli.getArgList().subList(1, cli.getArgList().size()));
              } catch (PackageManagerException ex) {
                ex.printStackTrace();
              }
              break;
            case "install":
              install(cli.getArgList().subList(1, cli.getArgList().size()));
              break;
            case "deploy":
              String colls[] = cli.getOptionValues("collections");
              String params[] = cli.getOptionValues("param");
              System.out.println("coll: "+Arrays.toString(colls)+", params: "+Arrays.toString(params));
              String packageName = cli.getArgList().get(1).toString().split(":")[0];
              String version = cli.getArgList().get(1).toString().contains(":")? 
                  cli.getArgList().get(1).toString().split(":")[1]: null;
                  deploy(packageName, version, cli.hasOption("update"), colls, params);
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
    }
    System.out.println("khatam: "+cmd); // nocommit
  }

  protected void addRepo(String zkHost, String name, String uri) throws KeeperException, InterruptedException, MalformedURLException, IOException {
    try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
      String existingRepositoriesJson = getRepositoriesJson(zkClient);
      System.out.println(existingRepositoriesJson);

      List repos = new ObjectMapper().readValue(existingRepositoriesJson, List.class);
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

      System.out.println("Added repository: "+name);
      System.out.println(getRepositoriesJson(zkClient));
    }
  }

  protected String getRepositoriesJson(SolrZkClient zkClient) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    if (zkClient.exists("/repositories.json", true)) {
      return new String(zkClient.getData("/repositories.json", null, null, true), "UTF-8");
    }
    return "[]";
  }

  protected void list(List args) {
    for (SolrPackageInstance pkg: packageManager.getPackages()) {
      System.out.println(pkg.getPluginId()+" ("+pkg.getVersion()+")");
    }
  }
  protected void available(List args) throws PackageManagerException {
    System.out.println("Available packages:\n-----");
    for (SolrPackage i: updateManager.getPackages()) {
      SolrPackage plugin = (SolrPackage)i;
      System.out.println(plugin.id + " \t\t"+plugin.description);
      for (SolrPackageRelease version: plugin.versions) {
        System.out.println("\tVersion: "+version.version);
      }
    }
  }
  protected void install(List args) throws PackageManagerException {
    updateManager.installPackage(args.get(0).toString(), args.get(1).toString());
    System.out.println(args.get(0).toString() + " installed.");
  }
  protected void deploy(String packageName, String version, boolean isUpdate,
      String collections[], String parameters[]) throws PackageManagerException {

    System.out.println(packageManager.deployInstallPackage(packageName, version, isUpdate,
        Arrays.asList(collections), parameters));
  }

  protected void update() throws PackageManagerException {
    if (updateManager.hasUpdates()) {
      System.out.println("Available updates:\n-----");

      for (SolrPackage i: updateManager.getUpdates()) {
        SolrPackage plugin = (SolrPackage)i;
        System.out.println(plugin.id + " \t\t"+plugin.description);
        for (SolrPackageRelease version: plugin.versions) {
          System.out.println("\tVersion: "+version.version);
        }
      }
    } else {
      System.out.println("No updates found. System is up to date.");
    }
  }

  protected void updatePackage(String zkHost, String packageName, List args) throws PackageManagerException {
    if (updateManager.hasUpdates()) {
      String latestVersion = updateManager.getLastPackageRelease(packageName).version;
      SolrPackageInstance installedPackage = packageManager.getPackage(packageName, "latest");
      System.out.println("Updating ["+packageName+"] from " + installedPackage.getVersion() + " to version "+latestVersion);

      List<String> collectionsDeployedIn = getDeployedCollections(zkHost, packageManager, installedPackage, true);
      System.out.println("Already deployed on collections: "+collectionsDeployedIn);
      updateManager.updatePackage(packageName, latestVersion);

      SolrPackageInstance updatedPackage = packageManager.getPackage(packageName, "latest");
      boolean res = packageManager.verify(updatedPackage, collectionsDeployedIn);
      System.out.println("Verifying version "+updatedPackage.getVersion()+" on "+collectionsDeployedIn
          +", result: "+res);
      if (!res) throw new PackageManagerException("Failed verification after deployment");
    } else {
      System.out.println("Package "+packageName+" is already up to date.");
    }
  }

  private List<String> getDeployedCollections(String zkHost, SolrPackageManager packageManager, SolrPackageInstance pkg, boolean onlyLatest) {

    List<String> allCollections;
    try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
      allCollections = zkClient.getChildren("/collections", null, true);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Need to verify if these collections have the plugin installed? "+ allCollections);
    List<String> deployed = new ArrayList<String>();
    for (String collection: allCollections) {
      // Check package version installed
      // http://localhost:8983/api/collections/abc/config/params/PKG_VERSIONS?omitHeader=true
      String paramsJson = SolrPackageManager.get(solrBaseUrl+"/api/collections/"+collection+"/config/params/PKG_VERSIONS?omitHeader=true");
      String version = null;
      try {
        version = JsonPath.parse(paramsJson).read("$['response'].['params'].['PKG_VERSIONS'].['"+pkg.id+"'])");
      } catch (PathNotFoundException ex) {
        // Don't worry if PKG_VERSION wasn't found. It just means this collection was never touched by the package manager.
      }
      if (onlyLatest) {
        if ("$LATEST".equals(version) && packageManager.verify(pkg, Collections.singletonList(collection))) {
          deployed.add(collection);
        } else {
          System.out.println("Skipping collection: "+collection+", version: "+version);
        }
      } else {
        if (packageManager.verify(pkg, Collections.singletonList(collection))) {
          deployed.add(collection);
        }
      }
    }
    return deployed;
  }

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