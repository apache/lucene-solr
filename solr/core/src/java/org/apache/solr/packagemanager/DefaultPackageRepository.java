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

package org.apache.solr.packagemanager;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * This is a serializable bean (for the JSON that is stored in /repository.json) representing a repository of Solr packages.
 * Supports standard repositories based on a webservice.
 */
public class DefaultPackageRepository extends PackageRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DefaultPackageRepository() { // this is needed for deserialization from JSON
  
  }

  public DefaultPackageRepository(String repositoryName, String repositoryURL) {
    this.name = repositoryName;
    this.repositoryURL = repositoryURL;
  }

  @Override
  public void refresh() {
    packages = null;
  }

  @JsonIgnore
  private Map<String, SolrPackage> packages;

  @Override
  public Map<String, SolrPackage> getPackages() {
    if (packages == null) {
      initPackages();
    }

    return packages;
  }

  @Override
  public SolrPackage getPackage(String packageName) {
    return getPackages().get(packageName);
  }

  @Override
  public boolean hasPackage(String packageName) {
    return getPackages().containsKey(packageName);
  }

  @Override
  public Path download(String artifactName) throws SolrException, IOException {
    Path tmpDirectory = Files.createTempDirectory("solr-packages");
    tmpDirectory.toFile().deleteOnExit();
    URL url = new URL(new URL(repositoryURL.endsWith("/")? repositoryURL: repositoryURL+"/"), artifactName);
    String fileName = FilenameUtils.getName(url.getPath());
    Path destination = tmpDirectory.resolve(fileName);

    switch (url.getProtocol()) {
      case "http":
      case "https":
      case "ftp":
        FileUtils.copyURLToFile(url, destination.toFile());
        break;
      default:
        throw new SolrException(ErrorCode.BAD_REQUEST, "URL protocol " + url.getProtocol() + " not supported");
    }
    
    return destination;
  }

  private void initPackages() {
    try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
      SolrPackage[] items = PackageUtils.getJson(client, repositoryURL + "/repository.json", SolrPackage[].class);

      packages = new HashMap<>(items.length);
      for (SolrPackage pkg : items) {
        pkg.setRepository(name);
        packages.put(pkg.name, pkg);
      }
    } catch (IOException ex) {
      throw new SolrException(ErrorCode.INVALID_STATE, ex);
    }
    if (log.isDebugEnabled()) {
      log.debug("Found {} packages in repository '{}'", packages.size(), name);
    }
  }
}
