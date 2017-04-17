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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.update.FileDownloader;
import ro.fortsoft.pf4j.update.PluginInfo;
import ro.fortsoft.pf4j.update.SimpleFileDownloader;
import ro.fortsoft.pf4j.update.UpdateRepository;

/**
 * Update Repository that resolves Apache Mirros
 */
public class ApacheMirrorsUpdateRepository extends SolrUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String APACHE_DIST_URL = "https://www.apache.org/dist/";
  private static final String APACHE_ARCHIVE_URL = "https://archive.apache.org/dist/";
  private static final String CLOSER_URL = "https://www.apache.org/dyn/closer.lua?action=download&filename=";
  private String modulesPath;

  public ApacheMirrorsUpdateRepository(String id, String modulesPath) {
    super(id);
    this.modulesPath = modulesPath;
  }

  @Override
  protected String resolveModulesUrl() {
    String mirrorUrl = CLOSER_URL + modulesPath;
    try {
      mirrorUrl = getFinalURL(mirrorUrl);
      return mirrorUrl;
    } catch (IOException e) {
      log.debug("Url {} not found in mirrors, response={}",
          mirrorUrl, e.getMessage());
      try {
        mirrorUrl = getFinalURL(APACHE_DIST_URL + modulesPath);
        log.debug("Resolved URL: {}", mirrorUrl);
        return mirrorUrl;
      } catch (IOException e1) {
        log.debug("Url {} not found in main repo, response={}",
            mirrorUrl, e1.getMessage());
        try {
          mirrorUrl = getFinalURL(APACHE_ARCHIVE_URL + modulesPath);
          log.debug("Resolved URL: {}", mirrorUrl);
          return mirrorUrl;
        } catch (IOException e2) {
          log.debug("Url {} not found in archive repo, response={}",
              mirrorUrl, e2.getMessage());
          return null;
        }
      }
    }
  }

  @Override
  public Map<String, PluginInfo> getPlugins() {
    if (plugins == null) {
      initPlugins();
    }
    return plugins;
  }

  @Override
  public PluginInfo getPlugin(String id) {
    return getPlugins().get(id);
  }

  @Override
  public void refresh() {
    plugins = null;
  }

  @Override
  public FileDownloader getFileDownloader() {
    return new ApacheChecksumVerifyingDownloader();
  }
  
  public static String getFinalURL(String url) throws IOException {
      HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
      con.setInstanceFollowRedirects(false);
      con.setRequestMethod("GET");
      con.connect();
      con.getInputStream();

      if (con.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM || con.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP) {
          String redirectUrl = con.getHeaderField("Location");
          return getFinalURL(redirectUrl);
      }
      return url;
  }

  private class ApacheChecksumVerifyingDownloader extends SimpleFileDownloader {
    @Override
    public Path downloadFile(URL url) throws PluginException, IOException {
      String md5FileUrl = getMD5FileUrl(url.toString());
      String md5 = getAndParseMd5File(md5FileUrl);
      if (md5 == null) {
        throw new PluginException("Failed to fetch md5 of " + url + ", aborting");
      }
      Path downloadFile = super.downloadFile(url);
      if (!DigestUtils.md5Hex(Files.newInputStream(downloadFile)).equalsIgnoreCase(md5)) {
        throw new PluginException("MD5 checksum of file " + url + " does not match the one from " + md5FileUrl + ", aborting");
      }
      return downloadFile;
    }

    private String getMD5FileUrl(String url) {
      return APACHE_ARCHIVE_URL + url.substring(url.indexOf(modulesPath)) + ".md5";
    }

    private String getAndParseMd5File(String url) {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            new URL(url).openStream()));
        return reader.readLine().split(" ")[0];
      } catch (IOException e) {
        log.warn("Failed to find md5 sun file " + url);
        return null;
      }
    }
  }
}
