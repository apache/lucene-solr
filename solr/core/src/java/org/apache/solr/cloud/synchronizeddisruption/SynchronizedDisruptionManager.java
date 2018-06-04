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
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SynchronizedDisruptionConfig;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronizedDisruptionManager {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String THREAD_FACTORY_PREFIX = "SynchronizedDisruption";
  private static final int DEFAULT_SD_THREAD_POOL = 10;

  private final ScheduledExecutorService disruptionExecutor;
  private final ConcurrentMap<String, SynchronizedDisruption> disruptions = new ConcurrentHashMap<>();

  public SynchronizedDisruptionManager(SolrResourceLoader loader, SynchronizedDisruptionConfig sdConfig) {
    PluginInfo[] pluginInfos = sdConfig.getSynchronizedDisruptions();

    int poolSize = Math.min(pluginInfos.length, DEFAULT_SD_THREAD_POOL);
    disruptionExecutor = Executors.newScheduledThreadPool(poolSize, new DefaultSolrThreadFactory(THREAD_FACTORY_PREFIX));

    Arrays.stream(pluginInfos)
      .forEach(pi -> {
        String name = pi.name;
        logger.info("Loading and preparing " + name + " for Synchronized Disruption");
        SynchronizedDisruption synchronizedDisruption = loadSynchronizedDisruption(loader, pi);
        disruptions.put(name, synchronizedDisruption);
        synchronizedDisruption.prepare();
      });
  }

  public SynchronizedDisruption loadSynchronizedDisruption(SolrResourceLoader loader, PluginInfo info) {
    return loader.newInstance(info.className, SynchronizedDisruption.class, new String[0],
        new Class[]{ScheduledExecutorService.class, String.class}, new Object[]{disruptionExecutor, info.attributes.get("cron")});
  }

  public boolean closeSynchronizedDisruption(String name) {
    SynchronizedDisruption disruption = disruptions.get(name);
    disruption.close();
    disruptions.remove(name);
    return true;
  }

  public void closeAll() {
    disruptions.keySet().forEach(this::closeSynchronizedDisruption);
  }

}
