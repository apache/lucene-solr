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

package org.apache.solr.util.modules;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.update.DefaultUpdateRepository;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;

/**
 * UpdateManager for Solr that adds Apache dist repo as default
 */
public class ModuleUpdateManager extends UpdateManager {
  private static final Logger log = LoggerFactory.getLogger(ModuleUpdateManager.class);

  public ModuleUpdateManager(PluginManager pluginManager) {
    super(pluginManager, (Path) null); // Don't want repositories.json
    repositories = new ArrayList<>();
//    repositories.add(new ApacheMirrorsUpdateRepository("solr-contribs", "lucene/solr/" + pluginManager.getSystemVersion()));
//    repositories.add(new DefaultUpdateRepository("local", "file:///Users/janhoy/solr-repo/"));
  }

  /**
   * Plugin manager that initializes new repositories that are added to the Apache one
   * @param pluginManager the Plugin manager
   * @param repos List of UpdateRepository
   */
  public ModuleUpdateManager(PluginManager pluginManager, List<UpdateRepository> repos) {
    this(pluginManager);
    repositories.addAll(repos);
  }

  public boolean installModule(String id, String version) {
    return installPlugin(id, version);
  }
}
