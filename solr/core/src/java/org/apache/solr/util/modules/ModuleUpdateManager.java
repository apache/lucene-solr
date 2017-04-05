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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.PluginState;
import ro.fortsoft.pf4j.update.DefaultUpdateRepository;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;
import ro.fortsoft.pf4j.util.FileUtils;
import ro.fortsoft.pf4j.util.Unzip;
import sun.net.www.protocol.file.FileURLConnection;

/**
 * UpdateManager for Solr that adds Apache dist repo as default
 */
public class ModuleUpdateManager extends UpdateManager {
  private static final Logger log = LoggerFactory.getLogger(ModuleUpdateManager.class);

  public ModuleUpdateManager(PluginManager pluginManager) {
    super(pluginManager);
    repositories = new ArrayList<>();
//    repositories.add(new UpdateRepository("apache", resolveApacheDistUrl(Version.LATEST)));
    repositories.add(new DefaultUpdateRepository("local", "file:///Users/janhoy/solr-repo/"));
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

  public void addRepository(String id, String url) {
    if (repositories.stream().map(UpdateRepository::getId).filter(id::equals).count() > 0) {
      log.warn("Repository with id " + id + " already exists, doing nothing");
    } else {
      repositories.add(new DefaultUpdateRepository(id, url));
    }
  }

  public void addRepository(UpdateRepository newRepo) {
    newRepo.refresh();
    repositories.add(newRepo);
  }

  public void removeRepository(String id) {
    for (UpdateRepository repo : getRepositories()) {
      if (id == repo.getId()) {
        repositories.remove(repo);
        break;
      }
    }
  }

  public boolean installModule(String url) {
    return installPlugin(url);
  }

  @Override
  public synchronized boolean installPlugin(String url) {
    File pluginArchiveFile;
    try {
      pluginArchiveFile = downloadFile(url);

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
    File pluginDirectory = Paths.get(pluginZip.getParent()).resolve(pluginName).toFile();
    // check if exists root or the '.zip' file is "newer" than root
    if (!pluginDirectory.exists() || (pluginZipDate > pluginDirectory.lastModified())) {
      log.debug("Expand plugin zip '{}' in '{}'", pluginZip, pluginDirectory);

      // do not overwrite an old version, remove it
      if (pluginDirectory.exists()) {
        FileUtils.delete(pluginDirectory.toPath());
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

  public File downloadFile(String fileUrl) throws IOException {
    Path pluginsDir = ((SolrPluginManager) pluginManager).getPluginsRoot();
    Files.createDirectories(pluginsDir);

    Path tmpFile = Files.createTempFile(pluginsDir, null, ".tmp");
    Files.deleteIfExists(tmpFile);

      log.debug("Download '{}' to '{}'", fileUrl, tmpFile);

      // create the url
      URL url = new URL(fileUrl);

      // set up the URL connection
      URLConnection connection = url.openConnection();

      // connect to the remote site (may takes some time)
      connection.connect();

      // check for http authorization
    long lastModified;
    if (connection instanceof HttpURLConnection) {
      HttpURLConnection httpConnection = (HttpURLConnection) connection;
      if (httpConnection.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
        throw new ConnectException("HTTP Authorization failure");
      }

      // try to get the server-specified last-modified date of this artifact
      lastModified = httpConnection.getHeaderFieldDate("Last-Modified", System.currentTimeMillis());
    } else {
      lastModified = ((FileURLConnection)connection).getLastModified();
    }
      // try to get the input stream (three times)
      InputStream is = null;
      for (int i = 0; i < 3; i++) {
          try {
              is = connection.getInputStream();
              break;
          } catch (IOException e) {
              log.error(e.getMessage(), e);
          }
      }
      if (is == null) {
          throw new ConnectException("Can't get '" + url + " to '" + tmpFile + "'");
      }

      // reade from remote resource and write to the local file
      FileOutputStream fos = new FileOutputStream(tmpFile.toFile());
      byte[] buffer = new byte[1024];
      int length;
      while ((length = is.read(buffer)) >= 0) {
          fos.write(buffer, 0, length);
      }
      fos.close();
      is.close();

    // rename tmp file to resource file
    String path = url.getPath();
    String fileName = path.substring(path.lastIndexOf('/') + 1);
    File file = new File(pluginsDir.toFile(), fileName);
    if (file.exists()) {
        log.debug("Delete old '{}' resource file", file);
        file.delete();
    }

      // rename tmp file to resource file
      log.debug("Rename '{}' to {}", tmpFile, file);
      Files.move(tmpFile, file.toPath());


      log.debug("Set last modified of '{}' to '{}'", file, lastModified);
      file.setLastModified(lastModified);

      return file;
  }

  public boolean updatePlugin(String id, String url) {
    if (!pluginManager.deletePlugin(id)) {
        return false;
    }
      File pluginArchiveFile;
      File pluginDirectory;
      try {
          pluginArchiveFile = downloadFile(url);
        pluginDirectory = expandPluginZip(pluginArchiveFile);
      } catch (IOException e) {
          log.error(e.getMessage(), e);
          return false;
      }

      String newPluginId = pluginManager.loadPlugin(pluginDirectory.toPath());
      PluginState state = pluginManager.startPlugin(newPluginId);

      return PluginState.STARTED.equals(state);
  }
}
