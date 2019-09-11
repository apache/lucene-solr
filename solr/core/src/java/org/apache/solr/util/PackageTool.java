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
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.packagemanager.SolrPluginInfo;
import org.apache.solr.packagemanager.SolrPluginInfo.SolrPluginRelease;
import org.apache.solr.packagemanager.SolrPluginManager;
import org.apache.solr.packagemanager.SolrUpdateManager;
import org.apache.solr.util.SolrCLI.StatusTool;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.pf4j.PluginException;
import org.pf4j.PluginWrapper;
import org.pf4j.update.PluginInfo;

import com.google.gson.Gson;


public class PackageTool extends SolrCLI.ToolBase {
  @Override
  public String getName() {
    return "package";
  }


  @Override
  protected void runImpl(CommandLine cli) throws Exception {
    String zkHost = getZkHost(cli);

    String cmd = cli.getArgs()[0];

    if (cmd != null) {
      SolrPluginManager pluginManager = new SolrPluginManager(new File("./plugins"));
      SolrUpdateManager updateManager = new SolrUpdateManager(pluginManager,
          getRepositoriesJson(new SolrZkClient(zkHost, 30000)));


      switch (cmd) {
        case "add-repo":
          addRepo(pluginManager, updateManager, zkHost, cli.getArgs()[1], cli.getArgs()[2]);
          break;
        case "list":
          list(pluginManager, updateManager, cli.getArgList().subList(1, cli.getArgList().size()));
          break;
        case "list-available":
          available(pluginManager, updateManager, cli.getArgList().subList(1, cli.getArgList().size()));
          break;
        case "install":
          install(pluginManager, updateManager, cli.getArgList().subList(1, cli.getArgList().size()));
          break;
        case "deploy":
          deploy(pluginManager, updateManager, cli.getArgList().subList(1, cli.getArgList().size()));
          break;
        default:
          throw new RuntimeException("Unrecognized command: "+cmd);
      };
    }
  }

  protected void addRepo(SolrPluginManager pluginManager, SolrUpdateManager updateManager, String zkHost, String name, String uri) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    SolrZkClient zkClient = new SolrZkClient(zkHost, 30000);

    String existingRepositoriesJson = getRepositoriesJson(zkClient);
    System.out.println(existingRepositoriesJson);

    List repos = new Gson().fromJson(existingRepositoriesJson, List.class);
    repos.add(new Repository(name, uri));
    if (zkClient.exists("/repositories.json", true) == false) {
      zkClient.create("/repositories.json", new Gson().toJson(repos).getBytes(), CreateMode.PERSISTENT, true);
    } else {
      zkClient.setData("/repositories.json", new Gson().toJson(repos).getBytes(), true);
    }
    System.out.println("Added repository: "+name);
    System.out.println(getRepositoriesJson(zkClient));
  }

  class Repository {
    String pluginsJsonFileName = "manifest.json";
    final String id;
    final String url;
    public Repository(String id, String url) {
      this.id = id;
      this.url = url;
    }
  }

  protected String getRepositoriesJson(SolrZkClient zkClient) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    if (zkClient.exists("/repositories.json", true)) {
      return new String(zkClient.getData("/repositories.json", null, null, true), "UTF-8");
    }
    return "[]";
  }

  protected void list(SolrPluginManager pluginManager, SolrUpdateManager updateManager, List args) {
    for (PluginWrapper plugin: pluginManager.getPlugins()) {
      System.out.println(plugin.getPluginId()+" ("+plugin.getDescriptor().getVersion()+")");
    }
  }
  protected void available(SolrPluginManager pluginManager, SolrUpdateManager updateManager, List args) throws PluginException {
    //System.out.println(updateManager.getAvailablePlugins());
    for (PluginInfo i: updateManager.getAvailablePlugins()) {
      SolrPluginInfo plugin = (SolrPluginInfo)i;
      System.out.println(plugin.id + " \t\t"+plugin.description);
      for (SolrPluginRelease version: plugin.versions) {
        System.out.println("\tVersion: "+version.version);
      }
    }

  }
  protected void install(SolrPluginManager pluginManager, SolrUpdateManager updateManager, List args) throws PluginException {
    pluginManager.loadPlugins();
    updateManager.installPlugin(args.get(0).toString(), args.get(1).toString());
    System.out.println(args.get(0).toString() + " installed.");
  }
  protected void deploy(SolrPluginManager pluginManager, SolrUpdateManager updateManager, List args) throws PluginException {
    System.out.println(pluginManager.deployPlugin(args.get(0).toString(), args.subList(1, args.size())));
  }

  @SuppressWarnings("static-access")
  public Option[] getOptions() {
    return new Option[] {
        OptionBuilder
        .withArgName("URL")
        .hasArg()
        .isRequired(false)
        .withDescription("Address of the Solr Web application, defaults to: "+SolrCLI.DEFAULT_SOLR_URL)
        .create("solrUrl"),
    };
  }


  private String getZkHost(CommandLine cli) throws Exception {
    String zkHost = cli.getOptionValue("zkHost");
    if (zkHost != null)
      return zkHost;

    // find it using the localPort
    String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.DEFAULT_SOLR_URL);

    if (!solrUrl.endsWith("/"))
      solrUrl += "/";

    String systemInfoUrl = solrUrl+"admin/info/system";
    CloseableHttpClient httpClient = SolrCLI.getHttpClient();
    try {
      // hit Solr to get system info
      Map<String,Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String,Object> status = statusTool.reportStatus(solrUrl, systemInfo, httpClient);
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