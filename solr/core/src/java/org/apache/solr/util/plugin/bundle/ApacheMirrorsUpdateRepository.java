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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.solr.common.SolrException;
import org.pf4j.update.FileVerifier;
import org.pf4j.update.VerifyException;
import org.pf4j.update.verifier.BasicVerifier;
import org.pf4j.update.verifier.CompoundVerifier;
import org.pf4j.update.verifier.Sha512SumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update Repository that resolves Apache Mirros
 */
public class ApacheMirrorsUpdateRepository extends PluginUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String APACHE_DIST_URL = "https://www.apache.org/dist/";
  private static final String APACHE_ARCHIVE_URL = "https://archive.apache.org/dist/";
  private static final String CLOSER_URL = "https://www.apache.org/dyn/closer.lua?action=download&filename=";
  private String path;
  private URL mirrorUrl;

  public ApacheMirrorsUpdateRepository(String id, String path) {
    super(id);
    this.path = path;
    try {
      this.mirrorUrl = new URL(CLOSER_URL + path);
    } catch (MalformedURLException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  protected URL resolveUrl() {
    try {
      mirrorUrl = getFinalURL(mirrorUrl);
      return mirrorUrl;
    } catch (IOException e) {
      log.debug("Url {} not found in mirrors, response={}",
          mirrorUrl, e.getMessage());
      try {
        mirrorUrl = getFinalURL(new URL(APACHE_DIST_URL + path));
        log.debug("Resolved URL: {}", mirrorUrl);
        return mirrorUrl;
      } catch (IOException e1) {
        log.debug("Url {} not found in main repo, response={}",
            mirrorUrl, e1.getMessage());
        try {
          mirrorUrl = getFinalURL(new URL(APACHE_ARCHIVE_URL + path));
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
  public FileVerifier getFileVerfier() {
    return new CompoundVerifier(Arrays.asList(
        new BasicVerifier(),
        new ApacheChecksumVerifier())
    );
  }

  /**
   * Method that resolves final Apache mirrors URL, resolving redirects
   * @param url original URL
   * @return new URL which could be the same as the original or a new after redirects
   * @throws IOException if problems opening URL
   */
  protected URL getFinalURL(URL url) throws IOException {
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setInstanceFollowRedirects(false);
      con.setRequestMethod("GET");
      con.connect();
      con.getInputStream();

      if (con.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM || con.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP) {
          URL redirectUrl = new URL(con.getHeaderField("Location"));
          return getFinalURL(redirectUrl);
      }
      return url;
  }

  public URL getMirrorUrl() {
    return mirrorUrl;
  }

  /**
   * FileVerifier that checks the SHA512 sum of Apache released downloads
   */
  private class ApacheChecksumVerifier extends Sha512SumVerifier {
    @Override
    public void verify(Context context, Path filePath) throws IOException, VerifyException {
      context.sha512sum = APACHE_ARCHIVE_URL + context.url.substring(context.url.indexOf(path)) + ".sha512";
      super.verify(context, filePath);
    }
  }
}
