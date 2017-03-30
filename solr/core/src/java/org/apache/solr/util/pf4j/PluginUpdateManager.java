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

package org.apache.solr.util.pf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.PluginState;
import ro.fortsoft.pf4j.update.FileDownloader;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;
import ro.fortsoft.pf4j.util.FileUtils;
import ro.fortsoft.pf4j.util.Unzip;

/**
 * UpdateManager for Solr that adds Apache dist repo as default
 */
public class PluginUpdateManager extends UpdateManager {
  private static final Logger log = LoggerFactory.getLogger(PluginUpdateManager.class);

  public PluginUpdateManager(PluginManager pluginManager) {
    super(pluginManager);
    repositories = new ArrayList<>();
    repositories.add(new UpdateRepository("apache", resolveApacheDistUrl(Version.LATEST)));
  }

  /**
   * Plugin manager that initializes new repositories that are added to the Apache one
   * @param pluginManager the Plugin manager
   * @param repos List of UpdateRepository
   */
  public PluginUpdateManager(PluginManager pluginManager, List<UpdateRepository> repos) {
    this(pluginManager);
    repositories.addAll(repos);
  }

  /*
  Find closest mirror that has the requested Solr version, or fall back to archive
   */
  private String resolveApacheDistUrl(Version latest) {
    // TODO: Hack
    return "http://people.apache.org/~janhoy/dist/plugins/";
  }

  public void addRepository(String id, String url) {
    if (repositories.stream().map(UpdateRepository::getId).filter(id::equals).count() > 0) {
      log.warn("Repository with id " + id + " already exists, doing nothing");
    } else {
      repositories.add(new UpdateRepository(id, url));
    }

  }

  @Override
  public synchronized boolean installPlugin(String url) {
    File pluginArchiveFile;
    try {
      pluginArchiveFile = new FileDownloader().downloadFile(url);

      File pluginDirectory = expandPluginZip(pluginArchiveFile);
      String pluginId = pluginManager.loadPlugin(pluginDirectory.toPath());
      PluginState state = pluginManager.startPlugin(pluginId);

      return PluginState.STARTED.equals(state);
    } catch (IOException e) {
      log.error("Installation failed. " + e.getMessage(), e);
      return false;
    }
  }

  // TODO: Hack copy paste for now
  private File expandPluginZip(File pluginZip) throws IOException {
    String fileName = pluginZip.getName();
    long pluginZipDate = pluginZip.lastModified();
    String pluginName = fileName.substring(0, fileName.length() - 4);
    File pluginDirectory = Paths.get("plugins").resolve(pluginName).toFile();
    // check if exists root or the '.zip' file is "newer" than root
    if (!pluginDirectory.exists() || (pluginZipDate > pluginDirectory.lastModified())) {
      log.debug("Expand plugin zip '{}' in '{}'", pluginZip, pluginDirectory);

      // do not overwrite an old version, remove it
      if (pluginDirectory.exists()) {
        FileUtils.delete(pluginDirectory);
      }

      // create root for plugin
      pluginDirectory.mkdirs();

      // expand '.zip' file
      Unzip unzip = new Unzip();
      unzip.setSource(pluginZip);
      unzip.setDestination(pluginDirectory);
      unzip.extract();
    }

    return pluginDirectory;
  }
}
