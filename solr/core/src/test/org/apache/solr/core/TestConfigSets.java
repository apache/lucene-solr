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
package org.apache.solr.core;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.SolrTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestConfigSets extends SolrTestCaseJ4 {

  public String solrxml = "<solr><str name=\"configSetBaseDir\">${configsets:configsets}</str></solr>";

  @BeforeClass
  public static void beforeTestConfigSets() throws Exception {
    useFactory(null);
  }

  @AfterClass
  public static void afterTestConfigSets() throws Exception {

  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("configsets");
  }

  public CoreContainer setupContainer(String configSetsBaseDir) {
    Path testDirectory = SolrTestUtil.createTempDir();

    System.setProperty("configsets", configSetsBaseDir);

    CoreContainer container = new CoreContainer(new SolrXmlConfig().fromString(testDirectory, solrxml));
    container.load();

    return container;
  }

  @Test
  public void testDefaultConfigSetBasePathResolution() throws IOException {
    Path solrHome = Paths.get("/path/to/solr/home");

    NodeConfig config
        = new SolrXmlConfig().fromString(solrHome, "<solr><str name=\"configSetBaseDir\">configsets</str></solr>");
    assertThat(config.getConfigSetBaseDirectory().toAbsolutePath(),
                is(Paths.get("/path/to/solr/home/configsets").toAbsolutePath()));

    NodeConfig absConfig
        = new SolrXmlConfig().fromString(solrHome, "<solr><str name=\"configSetBaseDir\">/path/to/configsets</str></solr>");
    assertThat(absConfig.getConfigSetBaseDirectory().toAbsolutePath(), is(Paths.get("/path/to/configsets").toAbsolutePath()));

  }

  @Test
  public void testConfigSetServiceFindsConfigSets() {
    CoreContainer container = null;
    try {
      container = setupContainer(SolrTestUtil.TEST_PATH().resolve("configsets").toString());
      Path solrHome = Paths.get(container.getSolrHome());

      SolrCore core1 = container.create("core1", ImmutableMap.of("configSet", "configset-2"));
      assertThat(core1.getCoreDescriptor().getName(), is("core1"));
      assertThat(Paths.get(core1.getDataDir()).toString(), is(solrHome.resolve("core1").resolve("data").toString()));
      core1.close();
    }
    finally {
      if (container != null)
        container.shutdown();
    }
  }

  @Test
  public void testNonExistentConfigSetThrowsException() {
    final CoreContainer container = setupContainer(SolrTestUtil.getFile("solr/configsets").getAbsolutePath());
    try {
      Exception thrown = SolrTestCaseUtil.expectThrows(Exception.class, "Expected core creation to fail", () -> {
        container.create("core1", ImmutableMap.of("configSet", "nonexistent"));
      });
      Throwable wrappedException = getWrappedException(thrown);
      assertThat(wrappedException.getMessage(), containsString("nonexistent"));
    } finally {
      if (container != null)
        container.shutdown();
    }
  }

  @Test
  public void testConfigSetOnCoreReload() throws IOException {
    Path testDirectory = SolrTestUtil.createTempDir("core-reload");
    File configSetsDir = new File(testDirectory.toFile(), "configsets");

    FileUtils.copyDirectory(SolrTestUtil.getFile("solr/configsets"), configSetsDir);

    String csd = configSetsDir.getAbsolutePath();
    System.setProperty("configsets", csd);

    CoreContainer container = new CoreContainer(new SolrXmlConfig().fromString(testDirectory, solrxml));
    try {
      container.load();

      // We initially don't have a /dump handler defined
      SolrCore core = container.create("core1", ImmutableMap.of("configSet", "configset-2"));

      assertThat("No /dump handler should be defined in the initial configuration", core.getRequestHandler("/dump"), is(nullValue()));

      // Now copy in a config with a /dump handler and reload
      FileUtils.copyFile(SolrTestUtil.getFile("solr/collection1/conf/solrconfig-withgethandler.xml"), new File(new File(configSetsDir, "configset-2/conf"), "solrconfig.xml"));
      container.reload("core1");
      core.close();
      core = container.getCore("core1");
      assertThat("A /dump handler should be defined in the reloaded configuration", core.getRequestHandler("/dump"), is(notNullValue()));
      core.close();
    } finally {
      container.shutdown();
    }
  }

}
