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

package org.apache.solr.cloud.synchronizeddisruption;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SynchronizedDisruptionConfig;
import org.apache.solr.handler.admin.SynchronizedDisruptionHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.SynchronizedDisruptionHandler.ACTION_PARAM;
import static org.apache.solr.handler.admin.SynchronizedDisruptionHandler.ADD_CLASS_PARAM;
import static org.apache.solr.handler.admin.SynchronizedDisruptionHandler.ADD_CRON_PARAM;
import static org.apache.solr.handler.admin.SynchronizedDisruptionHandler.ADD_PARAM;
import static org.apache.solr.handler.admin.SynchronizedDisruptionHandler.ADD_REMOVE_NAME_PARAM;

public class SynchronizedDisruptionManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String THREAD_FACTORY_PREFIX = "SynchronizedDisruption";
  private static final int DEFAULT_SD_THREAD_POOL = 10;
  static final String CRON_PLUGIN_INFO_ATTRIBUTE = "cron";
  static final String CLASSNAME_PLUGIN_INFO_ATTRIBUTE = "className";
  static final String DISRUPTION_NAME = "name";


  public static final String SYNCHRONIZED_DISRUPTION_ZNODE = "/sychronizeddisruption";

  private final ScheduledExecutorService disruptionExecutor;
  private final ConcurrentMap<String, SynchronizedDisruption> disruptions = new ConcurrentHashMap<>();

  private CoreContainer coreContainer;

  public SynchronizedDisruptionManager(CoreContainer coreContainer) {
    this(coreContainer, new SynchronizedDisruptionConfig.SynchronizedDisruptionConfigBuilder().build());
  }

  public SynchronizedDisruptionManager(CoreContainer coreContainer, SynchronizedDisruptionConfig sdConfig) {
    this.coreContainer = coreContainer;
    PluginInfo[] pluginInfos = sdConfig.getSynchronizedDisruptions();

    int poolSize = Math.min(pluginInfos.length, DEFAULT_SD_THREAD_POOL);
    disruptionExecutor = Executors.newScheduledThreadPool(poolSize, new DefaultSolrThreadFactory(THREAD_FACTORY_PREFIX));

    //Load Zookeeper disruptions first, as they have precedence
    try {
      List<Map<String, String>> existingZKDisruptions = getExistingZKDisruptions();
      existingZKDisruptions.forEach(node -> {
        addSynchronizedDisruption(this.coreContainer.getResourceLoader(),node.get(DISRUPTION_NAME),
            node.get(CLASSNAME_PLUGIN_INFO_ATTRIBUTE),node.get(CRON_PLUGIN_INFO_ATTRIBUTE), false);
      });
    } catch (KeeperException e) {
      log.error("KeeperException", e);
    } catch (InterruptedException e) {
      log.error("InterruptedException", e);
    }

    if (pluginInfos.length != 0) {
      SynchronizedDisruptionHandler synchronizedDisruptionHandler = new SynchronizedDisruptionHandler(this, this.coreContainer);
      Arrays.stream(pluginInfos)
          .forEach(pi -> {
            String name = pi.name;

            //Disruptions In Zookeeper override disruptions defined as a plugin
            if (disruptions.containsKey(name))
              return;

            String cron = pi.attributes.get(CRON_PLUGIN_INFO_ATTRIBUTE);
            String className = pi.className;
            if (cron.isEmpty()) {
              log.error(String.format(Locale.ROOT,"Cron value is empty; %s failed to load", name));
              return;
            }
            if (className.isEmpty()) {
              log.error(String.format(Locale.ROOT,"Cron value is empty; %s failed to load", name));
              return;
            }
            try {
              ModifiableSolrParams params = new ModifiableSolrParams();
              params.set(ACTION_PARAM, ADD_PARAM);
              params.set(ADD_CLASS_PARAM, className);
              params.set(ADD_CRON_PARAM, cron);
              params.set(ADD_REMOVE_NAME_PARAM, name);
              log.info(String.format(Locale.ROOT,"Loading and preparing %s for Synchronized Disruption", name));
              SolrQueryResponse response = new SolrQueryResponse();
              synchronizedDisruptionHandler.handleRequest(params, response::add);
              String responseString = response.getValues().size() != 0 ? response.getValues().getVal(0).toString() : response.toString();
              log.info(String.format(Locale.ROOT,"Added disruption %s with result %s", name, responseString));
            } catch (Exception e) {
              log.error(String.format(Locale.ROOT,"Failed to load %s due to Exception", name), e);
            }
          });
    }
  }

  private SynchronizedDisruption loadSynchronizedDisruption(SolrResourceLoader loader, ScheduledExecutorService disruptionExecutor,
                                                           String className, String cronValue) {
    return loader.newInstance(className, SynchronizedDisruption.class, new String[0],
        new Class[]{ScheduledExecutorService.class, String.class}, new Object[]{disruptionExecutor, cronValue});
  }

  /**
   * Adds a Synchronized disruption
   * @param loader is a SolrResourceLoader
   * @param name is the name of the disruption that will appear in Zookeeper; this must be unique
   * @param className is the full class name of the disruption
   * @param cronValue is a string cron expression based off of {@link org.apache.logging.log4j.core.util.CronExpression}
   * @param addToZookeeper determines if the disruption will be added to zookeeper
   * @return the success value if the disruption was added
   */
  public boolean addSynchronizedDisruption(SolrResourceLoader loader, String name, String className, String cronValue, boolean addToZookeeper) {
    return addSynchronizedDisruption(loader, this.disruptionExecutor, name, className, cronValue, addToZookeeper);
  }

  private boolean addSynchronizedDisruption(SolrResourceLoader loader, ScheduledExecutorService disruptionExecutor,
                                           String name, String className, String cronValue, boolean addToZookeeper) {
    try{
      SynchronizedDisruption synchronizedDisruption = loadSynchronizedDisruption(loader, disruptionExecutor, className, cronValue);
      if (addToZookeeper)
          updateZookeeper(cronValue, name, className);
      disruptions.put(name, synchronizedDisruption);
      synchronizedDisruption.prepare();
    } catch (Exception e) {
      log.error(String.format(Locale.ROOT,"Exception creating disruption %s with class %s", name, className), e);
      return false;
    }
    return true;
  }

  /**
   *
   * @param name is the name of the disruption to be closed
   * @return the success value if the disruption was removed
   */
  public boolean closeSynchronizedDisruption(String name) {
    return closeSynchronizedDisruption(name, false);
  }

  /**
   *
   * @param name is the name of the disruption to be closed
   * @param deleteFromZookeeper is the disruption should be remvoed from Zookeeper
   * @return the success value if the disruption was removed
   */
  public boolean closeSynchronizedDisruption(String name, boolean deleteFromZookeeper) {
    if (!disruptions.containsKey(name))
      return false;
    boolean deletedFromZK = deleteZookeeper(name);
    if (!deletedFromZK) log.warn(String.format(Locale.ROOT,"Synchronized Disruption %s was not deleted", name));
    SynchronizedDisruption disruption = disruptions.get(name);
    disruption.close();
    disruptions.remove(name);
    return true;
  }

  /** Closes all Diruptions handled by the Manager, but does not remove them from Zookeeper */
  public void closeAll() {
    disruptions.keySet().forEach(this::closeSynchronizedDisruption);
  }

  /**
   *
   * @return a List of each disruption with the name, class and cron values
   */
  public List<String> listDisruptions() {
    List< String> disruptionList = disruptions.entrySet()
        .stream()
        .map(kv -> "Name: " + kv.getKey() + ", Class: " + kv.getValue().getClass() + ", Cron: " + kv.getValue().getCronSchedule())
        .collect(Collectors.toList());

    return disruptionList != null ? disruptionList : Collections.emptyList();
  }

  private boolean updateZookeeper(String cronExpression, String name, String className) throws KeeperException, InterruptedException {
    CoreContainer coreContainer = this.coreContainer;
    ZkController zkController = coreContainer.getZkController();
    try(SolrZkClient solrZkClient = new SolrZkClient(zkController.getZkServerAddress(), zkController.getClientTimeout())) {
      String zkNodeForDisruption = SYNCHRONIZED_DISRUPTION_ZNODE + "/" + name;
      if (!solrZkClient.exists(zkNodeForDisruption, true)) {
        Map<String, String> zkMap = new HashMap<>();
        zkMap.put(CRON_PLUGIN_INFO_ATTRIBUTE, cronExpression);
        zkMap.put(CLASSNAME_PLUGIN_INFO_ATTRIBUTE, className);
        if (!solrZkClient.exists(SYNCHRONIZED_DISRUPTION_ZNODE, true))
          solrZkClient.create(SYNCHRONIZED_DISRUPTION_ZNODE, new byte[0], CreateMode.PERSISTENT, true);
        String output = solrZkClient.create(zkNodeForDisruption, Utils.toJSON(zkMap), CreateMode.PERSISTENT, true);
        return output.equals(zkNodeForDisruption);
      }
    }
    return false;
  }

  private boolean deleteZookeeper(String name) {
    CoreContainer coreContainer = this.coreContainer;
    ZkController zkController = coreContainer.getZkController();
    try (SolrZkClient solrZkClient = new SolrZkClient(zkController.getZkServerAddress(), zkController.getClientTimeout())) {
      String zkNodeForDisruption = SYNCHRONIZED_DISRUPTION_ZNODE + "/" + name;
      try {
        if (solrZkClient.exists(zkNodeForDisruption, true)) {
          solrZkClient.clean(zkNodeForDisruption);
        }
      } catch (InterruptedException e) {
        log.error("InterruptedException", e);
        return false;
      } catch (KeeperException e) {
        log.error("KeeperException", e);
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @return A list of Disruptions from Zookeeper containing the name, cron value, and class of the disruption
   * @throws KeeperException contains the details of an issue interacting with Zookeeper
   * @throws InterruptedException contains the details of an interruption
   */
  List<Map<String, String>> getExistingZKDisruptions() throws KeeperException, InterruptedException {
    CoreContainer coreContainer = this.coreContainer;
    ZkController zkController = coreContainer.getZkController();
    try (SolrZkClient solrZkClient = new SolrZkClient(zkController.getZkServerAddress(), zkController.getClientTimeout())) {
      if (!solrZkClient.exists(SYNCHRONIZED_DISRUPTION_ZNODE, true))
        return new ArrayList<>();
      List<String> children = solrZkClient.getChildren(SYNCHRONIZED_DISRUPTION_ZNODE, null, true);
      List<Map<String, String>> zkDisruptions = new ArrayList<>();
      children.forEach(child -> {
        try {
          String zkChildPath = SYNCHRONIZED_DISRUPTION_ZNODE + "/" + child;
          Map<String, String> disruption = (Map) Utils.fromJSON(solrZkClient.getData(zkChildPath, null, null, true));
          disruption.put(DISRUPTION_NAME, child);
          zkDisruptions.add(disruption);
        } catch (KeeperException e) {
          log.error("KeeperException for ZNode" + SYNCHRONIZED_DISRUPTION_ZNODE + " child " + child, e);
        } catch (InterruptedException e) {
          log.error("InterruptedException for ZNode" + SYNCHRONIZED_DISRUPTION_ZNODE + " child " + child, e);
        }
      });
      return zkDisruptions;
    }
  }
}
