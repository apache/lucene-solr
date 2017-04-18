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

package org.apache.solr.util.plugin.bundle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.update.FileDownloader;
import ro.fortsoft.pf4j.update.SimpleFileDownloader;

/**
 * Update Repository that resolves Apache Mirros
 */
public class ApacheMirrorsUpdateRepository extends PluginUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String APACHE_DIST_URL = "https://www.apache.org/dist/";
  private static final String APACHE_ARCHIVE_URL = "https://archive.apache.org/dist/";
  private static final String CLOSER_URL = "https://www.apache.org/dyn/closer.lua?action=download&filename=";
  private String path;
  private FileDownloader downloader;

  public ApacheMirrorsUpdateRepository(String id, String path) {
    super(id);
    this.path = path;
  }

  @Override
  protected String resolveUrl() {
    String mirrorUrl = CLOSER_URL + path;
    try {
      mirrorUrl = getFinalURL(mirrorUrl);
      return mirrorUrl;
    } catch (IOException e) {
      log.debug("Url {} not found in mirrors, response={}",
          mirrorUrl, e.getMessage());
      try {
        mirrorUrl = getFinalURL(APACHE_DIST_URL + path);
        log.debug("Resolved URL: {}", mirrorUrl);
        return mirrorUrl;
      } catch (IOException e1) {
        log.debug("Url {} not found in main repo, response={}",
            mirrorUrl, e1.getMessage());
        try {
          mirrorUrl = getFinalURL(APACHE_ARCHIVE_URL + path);
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
  public FileDownloader getFileDownloader() {
    if (downloader == null) {
      downloader = new ApacheChecksumVerifyingDownloader();
    }
    return downloader;
  }

  /**
   * Static method that resolves final Apache mirrors URL, resolving redirects
   * @param url original URL
   * @return new URL which could be the same as the original or a new after redirects
   * @throws IOException if problems opening URL
   */
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

  /**
   * FileDownloader that fails if the MD5 sum of the downloaded file does not match the one downloaded
   * from Apache archives
   */
  private class ApacheChecksumVerifyingDownloader extends SimpleFileDownloader {
    /**
     * Succeeds if downloaded file exists and has same checksum as md5 file downloaded from Apache archive
     *
     * @param originalUrl    the source from which the file was downloaded
     * @param downloadedFile the path to the downloaded file
     * @throws PluginException if the validation failed
     */
    @Override
    protected void validateDownload(URL originalUrl, Path downloadedFile) throws PluginException {
      super.validateDownload(originalUrl, downloadedFile);
      try {
        String md5FileUrl = getMD5FileUrl(originalUrl.toString());
        String md5 = getAndParseMd5File(md5FileUrl);
        if (md5 == null) {
          throw new PluginException("Failed to fetch md5 of " + originalUrl + ", aborting");
        }
        if (!DigestUtils.md5Hex(Files.newInputStream(downloadedFile)).equalsIgnoreCase(md5)) {
          throw new PluginException("MD5 checksum of file " + originalUrl + " does not match the one from " + md5FileUrl + ", aborting");
        }
      } catch (IOException e) {
        throw new PluginException("Validation failed, could not read downloaded file " + downloadedFile, e);
      }
    }

    private String getMD5FileUrl(String url) {
      return APACHE_ARCHIVE_URL + url.substring(url.indexOf(path)) + ".md5";
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
