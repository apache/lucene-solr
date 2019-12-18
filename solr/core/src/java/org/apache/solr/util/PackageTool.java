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

import static org.apache.solr.packagemanager.PackageUtils.printGreen;
import static org.apache.solr.packagemanager.PackageUtils.print;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
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
      log.info("Solr url: "+solrUrl+", solr base url: "+solrBaseUrl);
      String zkHost = getZkHost(cli);

      log.info("ZK: "+zkHost);
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
                repositoryManager.install(packageName, version);
                PackageUtils.printGreen(packageName + " installed.");
                break;
              }
              case "deploy":
              {
                Pair<String, String> parsedVersion = parsePackageVersion(cli.getArgList().get(1).toString());
                String packageName = parsedVersion.first();
                String version = parsedVersion.second();
                boolean noprompt = cli.hasOption('y');
                boolean isUpdate = cli.hasOption("update") || cli.hasOption('u');
                packageManager.deploy(packageName, version, PackageUtils.validateCollections(cli.getOptionValue("collections").split(",")), cli.getOptionValues("param"), isUpdate, noprompt);
                break;
              }
              case "undeploy":
              {
                Pair<String, String> parsedVersion = parsePackageVersion(cli.getArgList().get(1).toString());
                if (parsedVersion.second() != null) {
                  throw new SolrException(ErrorCode.BAD_REQUEST, "Only package name expected, without a version. Actual: " + cli.getArgList().get(1));
                }
                String packageName = parsedVersion.first();
                packageManager.undeploy(packageName, cli.getOptionValue("collections").split(","));
                break;
              }
              case "help":
              case "usage":
                print("Package Manager\n---------------");
                printGreen("./solr package add-repo <repository-name> <repository-url>");
                print("Add a repository to Solr.");
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
      log.info("Finished: "+cmd);

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
    String splits[] = arg.split(":");
    if (splits.length > 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid package name: " + arg +
          ". Didn't match the pattern: <packagename>:<version> or <packagename>");
    }

    String packageName = splits[0];
    String version = splits.length == 2? splits[1]: null;
    return new Pair(packageName, version);
  }

  @SuppressWarnings("static-access")
  public Option[] getOptions() {
    return new Option[] {
        OptionBuilder
        .withArgName("URL")
        .hasArg()
        .isRequired(true)
        .withDescription("Address of the Solr Web application, defaults to: " + SolrCLI.DEFAULT_SOLR_URL)
        .create("solrUrl"),

        OptionBuilder
        .withArgName("COLLECTIONS")
        .hasArg()
        .isRequired(false)
        .withDescription("List of collections. Run './solr package help' for more details.")
        .create("collections"),

        OptionBuilder
        .withArgName("PARAMS")
        .hasArgs()
        .isRequired(false)
        .withDescription("List of parameters to be used with deploy command. Run './solr package help' for more details.")
        .withLongOpt("param")
        .create("p"),

        OptionBuilder
        .isRequired(false)
        .withDescription("If a deployment is an update over a previous deployment. Run './solr package help' for more details.")
        .withLongOpt("update")
        .create("u"),

        OptionBuilder
        .isRequired(false)
        .withDescription("Run './solr package help' for more details.")
        .withLongOpt("collection")
        .create("c"),

        OptionBuilder
        .isRequired(false)
        .withDescription("Run './solr package help' for more details.")
        .withLongOpt("noprompt")
        .create("y")
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