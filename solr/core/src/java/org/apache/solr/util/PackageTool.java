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
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Pair;
import org.apache.solr.packagemanager.PackageManager;
import org.apache.solr.packagemanager.PackageUtils;
import org.apache.solr.packagemanager.RepositoryManager;
import org.apache.solr.packagemanager.SolrPackage;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.apache.solr.packagemanager.SolrPackageInstance;
import org.apache.solr.util.SolrCLI.StatusTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.packagemanager.PackageUtils.print;
import static org.apache.solr.packagemanager.PackageUtils.printGreen;

public class PackageTool extends SolrCLI.ToolBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @SuppressForbidden(reason = "Need to turn off logging, and SLF4J doesn't seem to provide for a way.")
  public PackageTool() {
    // Need a logging free, clean output going through to the user.
    Configurator.setRootLevel(Level.OFF);
  }

  @Override
  public String getName() {
    return "package";
  }

  public static String solrUrl = null;
  public static String solrBaseUrl = null;
  public PackageManager packageManager;
  public RepositoryManager repositoryManager;

  @Override
  @SuppressForbidden(reason = "We really need to print the stacktrace here, otherwise "
      + "there shall be little else information to debug problems. Other SolrCLI tools "
      + "don't print stack traces, hence special treatment is needed here.")
  protected void runImpl(CommandLine cli) throws Exception {
    try {
      solrUrl = cli.getOptionValues("solrUrl")[cli.getOptionValues("solrUrl").length-1];
      solrBaseUrl = solrUrl.replaceAll("\\/solr$", ""); // strip out ending "/solr"
      log.info("Solr url:{}, solr base url: {}", solrUrl, solrBaseUrl);
      String zkHost = getZkHost(cli);

      log.info("ZK: {}", zkHost);
      String cmd = cli.getArgList().size() == 0? "help": cli.getArgs()[0];

      try (HttpSolrClient solrClient = new HttpSolrClient.Builder(solrBaseUrl).build()) {
        if (cmd != null) {
          packageManager = new PackageManager(solrClient, solrBaseUrl, zkHost);
          try {
            repositoryManager = new RepositoryManager(solrClient, packageManager);

            switch (cmd) {
              case "add-repo":
                String repoName = cli.getArgs()[1];
                String repoUrl = cli.getArgs()[2];
                repositoryManager.addRepository(repoName, repoUrl);
                PackageUtils.printGreen("Added repository: " + repoName);
                break;
              case "add-key":
                String keyFilename = cli.getArgs()[1];
                repositoryManager.addKey(FileUtils.readFileToByteArray(new File(keyFilename)), Paths.get(keyFilename).getFileName().toString());
                break;
              case "list-installed":
                PackageUtils.printGreen("Installed packages:\n-----");
                for (SolrPackageInstance pkg: packageManager.fetchInstalledPackageInstances()) {
                  PackageUtils.printGreen(pkg);
                }
                break;
              case "list-available":
                PackageUtils.printGreen("Available packages:\n-----");
                for (SolrPackage pkg: repositoryManager.getPackages()) {
                  PackageUtils.printGreen(pkg.name + " \t\t"+pkg.description);
                  for (SolrPackageRelease version: pkg.versions) {
                    PackageUtils.printGreen("\tVersion: "+version.version);
                  }
                }
                break;
              case "list-deployed":
                if (cli.hasOption('c')) {
                  String collection = cli.getArgs()[1];
                  Map<String, SolrPackageInstance> packages = packageManager.getPackagesDeployed(collection);
                  PackageUtils.printGreen("Packages deployed on " + collection + ":");
                  for (String packageName: packages.keySet()) {
                    PackageUtils.printGreen("\t" + packages.get(packageName));
                  }
                } else {
                  String packageName = cli.getArgs()[1];
                  Map<String, String> deployedCollections = packageManager.getDeployedCollections(packageName);
                  if (deployedCollections.isEmpty() == false) {
                    PackageUtils.printGreen("Collections on which package " + packageName + " was deployed:");
                    for (String collection: deployedCollections.keySet()) {
                      PackageUtils.printGreen("\t" + collection + "("+packageName+":"+deployedCollections.get(collection)+")");
                    }
                  } else {
                    PackageUtils.printGreen("Package "+packageName+" not deployed on any collection.");
                  }
                }
                break;
              case "install":
              {
                Pair<String, String> parsedVersion = parsePackageVersion(cli.getArgList().get(1).toString());
                String packageName = parsedVersion.first();
                String version = parsedVersion.second();
                boolean success = repositoryManager.install(packageName, version);
                if (success) {
                  PackageUtils.printGreen(packageName + " installed.");
                } else {
                  PackageUtils.printRed(packageName + " installation failed.");
                }
                break;
              }
              case "deploy":
              {
                if (cli.hasOption("cluster") || cli.hasOption("collections")) {
                  Pair<String, String> parsedVersion = parsePackageVersion(cli.getArgList().get(1).toString());
                  String packageName = parsedVersion.first();
                  String version = parsedVersion.second();
                  boolean noprompt = cli.hasOption('y');
                  boolean isUpdate = cli.hasOption("update") || cli.hasOption('u');
                  String collections[] = cli.hasOption("collections")? PackageUtils.validateCollections(cli.getOptionValue("collections").split(",")): new String[] {};
                  packageManager.deploy(packageName, version, collections, cli.hasOption("cluster"), cli.getOptionValues("param"), isUpdate, noprompt);
                } else {
                  PackageUtils.printRed("Either specify -cluster to deploy cluster level plugins or -collections <list-of-collections> to deploy collection level plugins");
                }
                break;
              }
              case "undeploy":
              {
                if (cli.hasOption("cluster") || cli.hasOption("collections")) {
                  Pair<String, String> parsedVersion = parsePackageVersion(cli.getArgList().get(1).toString());
                  if (parsedVersion.second() != null) {
                    throw new SolrException(ErrorCode.BAD_REQUEST, "Only package name expected, without a version. Actual: " + cli.getArgList().get(1));
                  }
                  String packageName = parsedVersion.first();
                  String collections[] = cli.hasOption("collections")? PackageUtils.validateCollections(cli.getOptionValue("collections").split(",")): new String[] {};
                  packageManager.undeploy(packageName, collections, cli.hasOption("cluster"));
                } else {
                  PackageUtils.printRed("Either specify -cluster to undeploy cluster level plugins or -collections <list-of-collections> to undeploy collection level plugins");
                }
                break;
              }
              case "uninstall": {
                Pair<String, String> parsedVersion = parsePackageVersion(cli.getArgList().get(1).toString());
                if (parsedVersion.second() == null) {
                  throw new SolrException(ErrorCode.BAD_REQUEST, "Package name and version are both required. Actual: " + cli.getArgList().get(1));
                }
                String packageName = parsedVersion.first();
                String version = parsedVersion.second();
                packageManager.uninstall(packageName, version);
                break;
              }
              case "help":
              case "usage":
                print("Package Manager\n---------------");
                printGreen("./solr package add-repo <repository-name> <repository-url>");
                print("Add a repository to Solr.");
                print("");
                printGreen("./solr package add-key <file-containing-trusted-key>");
                print("Add a trusted key to Solr.");
                print("");
                printGreen("./solr package install <package-name>[:<version>] ");
                print("Install a package into Solr. This copies over the artifacts from the repository into Solr's internal package store and sets up classloader for this package to be used.");
                print("");
                printGreen("./solr package deploy <package-name>[:<version>] [-y] [--update] -collections <comma-separated-collections> [-p <param1>=<val1> -p <param2>=<val2> ...] ");
                print("Bootstraps a previously installed package into the specified collections. It the package accepts parameters for its setup commands, they can be specified (as per package documentation).");
                print("");
                printGreen("./solr package list-installed");
                print("Print a list of packages installed in Solr.");
                print("");
                printGreen("./solr package list-available");
                print("Print a list of packages available in the repositories.");
                print("");
                printGreen("./solr package list-deployed -c <collection>");
                print("Print a list of packages deployed on a given collection.");
                print("");
                printGreen("./solr package list-deployed <package-name>");
                print("Print a list of collections on which a given package has been deployed.");
                print("");
                printGreen("./solr package undeploy <package-name> -collections <comma-separated-collections>");
                print("Undeploys a package from specified collection(s)");
                print("");
                printGreen("./solr package uninstall <package-name>:<version>");
                print("Uninstall an unused package with specified version from Solr. Both package name and version are required.");
                print("\n");
                print("Note: (a) Please add '-solrUrl http://host:port' parameter if needed (usually on Windows).");
                print("      (b) Please make sure that all Solr nodes are started with '-Denable.packages=true' parameter.");
                print("\n");
                break;
              default:
                throw new RuntimeException("Unrecognized command: "+cmd);
            };
          } finally {
            packageManager.close();
          }
        }
      }
      log.info("Finished: {}", cmd);

    } catch (Exception ex) {
      ex.printStackTrace(); // We need to print this since SolrCLI drops the stack trace in favour of brevity. Package tool should surely print full stacktraces!
      throw ex;
    }
  }

  /**
   * Parses package name and version in the format "name:version" or "name"
   * @return A pair of package name (first) and version (second)
   */
  private Pair<String, String> parsePackageVersion(String arg) {
    String[] splits = arg.split(":");
    if (splits.length > 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid package name: " + arg +
          ". Didn't match the pattern: <packagename>:<version> or <packagename>");
    }

    String packageName = splits[0];
    String version = splits.length == 2? splits[1]: null;
    return new Pair<>(packageName, version);
  }

  public Option[] getOptions() {
    return new Option[] {
        Option.builder("solrUrl")
        .argName("URL")
        .hasArg()
        .required(true)
        .desc("Address of the Solr Web application, defaults to: " + SolrCLI.DEFAULT_SOLR_URL + '.')
        .build(),

        Option.builder("collections")
        .argName("COLLECTIONS")
        .hasArg()
        .required(false)
        .desc("Specifies that this action should affect plugins for the given collections only, excluding cluster level plugins.")
        .build(),

        Option.builder("cluster")
        .required(false)
        .desc("Specifies that this action should affect cluster-level plugins only.")
        .build(),

        Option.builder("p")
        .argName("PARAMS")
        .hasArgs()
        .required(false)
        .desc("List of parameters to be used with deploy command.")
        .longOpt("param")
        .build(),

        Option.builder("u")
        .required(false)
        .desc("If a deployment is an update over a previous deployment.")
        .longOpt("update")
        .build(),

        Option.builder("c")
        .required(false)
        .desc("The collection to apply the package to, not required.")
        .longOpt("collection")
        .build(),

        Option.builder("y")
        .required(false)
        .desc("Don't prompt for input; accept all default choices, defaults to false.")
        .longOpt("noprompt")
        .build()
    };
  }

  private String getZkHost(CommandLine cli) throws Exception {
    String zkHost = cli.getOptionValue("zkHost");
    if (zkHost != null)
      return zkHost;

    String systemInfoUrl = solrUrl+"/admin/info/system";
    CloseableHttpClient httpClient = SolrCLI.getHttpClient();
    try {
      // hit Solr to get system info
      Map<String,Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String,Object> status = statusTool.reportStatus(solrUrl+"/", systemInfo, httpClient);
      @SuppressWarnings({"unchecked"})
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
