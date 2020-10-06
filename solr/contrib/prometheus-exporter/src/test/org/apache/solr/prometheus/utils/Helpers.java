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

package org.apache.solr.prometheus.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.prometheus.PrometheusExporterTestBase;
import org.apache.solr.prometheus.exporter.MetricsConfiguration;

public class Helpers {

  public static MetricsConfiguration loadConfiguration(String path) throws Exception {
    Path configPath = Paths.get(path);

    try (SolrResourceLoader loader = new SolrResourceLoader(configPath.getParent())) {
      XmlConfigFile config = new XmlConfigFile(loader, configPath.getFileName().toString());
      return MetricsConfiguration.from(config);
    }
  }

  public static void indexAllDocs(SolrClient client) throws IOException, SolrServerException {
    File exampleDocsDir = new File(SolrTestCaseJ4.getFile("exampledocs").getAbsolutePath());
    File[] xmlFiles = Objects.requireNonNull(exampleDocsDir.listFiles((dir, name) -> name.endsWith(".xml")));
    for (File xml : xmlFiles) {
      ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update");
      req.addFile(xml, "application/xml");
      client.request(req, PrometheusExporterTestBase.COLLECTION);
    }
    client.commit(PrometheusExporterTestBase.COLLECTION);
  }
}
