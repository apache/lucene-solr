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
import org.apache.solr.packagemanager.PackageManager;
import org.apache.solr.packagemanager.PackageUtils;
import org.apache.solr.packagemanager.RepositoryManager;
import org.apache.solr.packagemanager.SolrPackageInstance;
import org.apache.solr.util.SolrCLI.StatusTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressForbidden(reason = "Need to use System.out.println() instead of log4j/slf4j for cleaner output")
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
                repositoryManager.addRepository(cli.getArgs()[1], cli.getArgs()[2]);
                break;
              case "list-installed":
                packageManager.listInstalled();
                break;
              case "list-available":
                repositoryManager.listAvailable();
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
                  PackageUtils.printGreen("Collections on which package " + packageName + " was deployed:");
                  for (String collection: deployedCollections.keySet()) {
                    PackageUtils.printGreen("\t" + collection + "("+packageName+":"+deployedCollections.get(collection)+")");
                  }
                }
                break;
              case "install":
              {
                String packageName = parsePackageVersion(cli.getArgList().get(1).toString())[0];
                String version = parsePackageVersion(cli.getArgList().get(1).toString())[1];
                repositoryManager.install(packageName, version);
                PackageUtils.printGreen(repositoryManager.toString() + " installed.");
                break;
              }
              case "deploy":
              {
                String packageName = parsePackageVersion(cli.getArgList().get(1).toString())[0];
                String version = parsePackageVersion(cli.getArgList().get(1).toString())[1];
                boolean noprompt = cli.hasOption('y');
                boolean isUpdate = cli.hasOption("update") || cli.hasOption('u');
                packageManager.deploy(packageName, version, cli.getOptionValues("collections"), cli.getOptionValues("param"), isUpdate, noprompt);
                break;
              }
              case "undeploy":
              {
                String packageName = parsePackageVersion(cli.getArgList().get(1).toString())[0];
                packageManager.undeploy(packageName, cli.getOptionValues("collections"));
                break;
              }
              case "help":
              case "usage":
                System.out.println("./solr package add-repo <repository-name> <repository-url>");
                System.out.println("Add a repository to Solr.");
                System.out.println("");
                System.out.println("./solr package install <package-name>[:<version>] ");
                System.out.println("Install a package into Solr. This copies over the artifacts from the repository into Solr's internal package store and sets up classloader for this package to be used.");
                System.out.println("");
                System.out.println("./solr package deploy <package-name>[:<version>] [-y] [--update] -collections <comma-separated-collections> [-p <param1>=<val1> -p <param2>=<val2> ...] ");
                System.out.println("Bootstraps a previously installed package into the specified collections. It the package accepts parameters for its setup commands, they can be specified (as per package documentation).");
                System.out.println("");
                System.out.println("./solr package list-installed");
                System.out.println("Print a list of packages installed in Solr.");
                System.out.println("");
                System.out.println("./solr package list-available");
                System.out.println("Print a list of packages available in the repositories.");
                System.out.println("");
                System.out.println("./solr package list-deployed -c <collection>");
                System.out.println("Print a list of packages deployed on a given collection.");
                System.out.println("");
                System.out.println("./solr package list-deployed <package-name>");
                System.out.println("Print a list of collections on which a given package has been deployed.");
                System.out.println("");
                System.out.println("./solr package undeploy <package-name> -collections <comma-separated-collections>");
                System.out.println("Undeploys a package from specified collection(s)");
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
   * @param arg User supplied argument
   * @return Array of two elements, first the package name, second version (or null if not present)
   */
  private String[] parsePackageVersion(String arg) {
    String packageName = arg.split(":")[0];
    String version = arg.contains(":")? arg.split(":")[1]: null;
    return new String[] {packageName, version};
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
        .hasArgs()
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