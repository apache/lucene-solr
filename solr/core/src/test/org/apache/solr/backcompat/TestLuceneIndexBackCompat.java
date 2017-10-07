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

package org.apache.solr.backcompat;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.TestBackwardsCompatibility;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.TestHarness;
import org.junit.Test;

/** Verify we can read/write previous versions' Lucene indexes. */
public class TestLuceneIndexBackCompat extends SolrTestCaseJ4 {
  private static final String[] oldNames = TestBackwardsCompatibility.getOldNames();
  private static final String[] oldSingleSegmentNames = TestBackwardsCompatibility.getOldSingleSegmentNames();

  @Test
  public void testOldIndexes() throws Exception {
    List<String> names = new ArrayList<>(oldNames.length + oldSingleSegmentNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldSingleSegmentNames));
    for (String name : names) {
      setupCore(name);

      assertQ(req("q", "*:*", "rows", "0"), "//result[@numFound='35']");

      assertU(adoc("id", "id_123456789"));
      assertU(commit());

      deleteCore();
    }
  }
  
  private void setupCore(String coreName) throws Exception {
    if (h != null) {
      h.close();
    }
    Path solrHome = createTempDir(coreName).toAbsolutePath();
    Files.createDirectories(solrHome);
    Path coreDir = solrHome.resolve(coreName);
    Path confDir = coreDir.resolve("conf");
    Files.createDirectories(confDir);
    Path dataDir = coreDir.resolve("data");
    Path indexDir = dataDir.resolve("index");
    Files.createDirectories(indexDir);

    Files.copy(getFile("solr/solr.xml").toPath(), solrHome.resolve("solr.xml"));
    FileUtils.copyDirectory(configset("backcompat").toFile(), confDir.toFile());

    try (Writer writer = new OutputStreamWriter(Files.newOutputStream(coreDir.resolve("core.properties")), StandardCharsets.UTF_8)) {
      Properties coreProps = new Properties();
      coreProps.put("name", coreName);
      coreProps.store(writer, null);
    }

    InputStream resource = TestBackwardsCompatibility.class.getResourceAsStream("index." + coreName + ".zip");
    assertNotNull("Index name " + coreName + " not found", resource);
    TestUtil.unzip(resource, indexDir);

    configString = "solrconfig.xml";
    schemaString = "schema.xml";
    testSolrHome = solrHome;
    System.setProperty("solr.solr.home", solrHome.toString());
    ignoreException("ignore_exception");
    solrConfig = TestHarness.createConfig(testSolrHome, coreName, getSolrConfigFile());
    h = new TestHarness(coreName, dataDir.toString(), solrConfig, getSchemaFile());
    lrf = h.getRequestFactory("",0,20, CommonParams.VERSION,"2.2");
  }
}
