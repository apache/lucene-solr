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
package org.apache.solr.handler.admin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CorePropertiesLocator;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreAdminCreateDiscoverTest extends SolrTestCaseJ4 {

  private static File solrHomeDirectory = null;

  private static CoreAdminHandler admin = null;

  private static String coreNormal = "normal";
  private static String coreSysProps = "sys_props";
  private static String coreDuplicate = "duplicate";

  @BeforeClass
  public static void beforeClass() throws Exception {
    useFactory(null); // I require FS-based indexes for this test.

    solrHomeDirectory = createTempDir().toFile();

    setupNoCoreTest(solrHomeDirectory.toPath(), null);

    admin = new CoreAdminHandler(h.getCoreContainer());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    admin = null; // Release it or the test harness complains.
    solrHomeDirectory = null;
  }

  private static void setupCore(String coreName, boolean blivet) throws IOException {
    File instDir = new File(solrHomeDirectory, coreName);
    File subHome = new File(instDir, "conf");
    assertTrue("Failed to make subdirectory ", subHome.mkdirs());

    // Be sure we pick up sysvars when we create this
    String srcDir = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(srcDir, "schema-tiny.xml"), new File(subHome, "schema_ren.xml"));
    FileUtils.copyFile(new File(srcDir, "solrconfig-minimal.xml"), new File(subHome, "solrconfig_ren.xml"));

    FileUtils.copyFile(new File(srcDir, "solrconfig.snippet.randomindexconfig.xml"),
        new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));
  }

  @Test
  public void testCreateSavesSysProps() throws Exception {

    setupCore(coreSysProps, true);

    // create a new core (using CoreAdminHandler) w/ properties
    // Just to be sure it's NOT written to the core.properties file
    File workDir = new File(solrHomeDirectory, coreSysProps);
    System.setProperty("INSTDIR_TEST", workDir.getAbsolutePath());
    System.setProperty("CONFIG_TEST", "solrconfig_ren.xml");
    System.setProperty("SCHEMA_TEST", "schema_ren.xml");

    File dataDir = new File(workDir.getAbsolutePath(), "data_diff");
    System.setProperty("DATA_TEST", "data_diff");

    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.NAME, coreSysProps,
            CoreAdminParams.INSTANCE_DIR, "${INSTDIR_TEST}",
            CoreAdminParams.CONFIG, "${CONFIG_TEST}",
            CoreAdminParams.SCHEMA, "${SCHEMA_TEST}",
            CoreAdminParams.DATA_DIR, "${DATA_TEST}"),
            resp);
    assertNull("Exception on create", resp.getException());

    // verify props are in persisted file

    Properties props = new Properties();
    File propFile = new File(solrHomeDirectory, coreSysProps + "/" + CorePropertiesLocator.PROPERTIES_FILENAME);
    FileInputStream is = new FileInputStream(propFile);
    try {
      props.load(new InputStreamReader(is, StandardCharsets.UTF_8));
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(is);
    }

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.NAME), coreSysProps);

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.CONFIG), "${CONFIG_TEST}");

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.SCHEMA), "${SCHEMA_TEST}");

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.DATA_DIR), "${DATA_TEST}");

    assertEquals(props.size(), 4);
    //checkOnlyKnown(propFile);

    // Now assert that certain values are properly dereferenced in the process of creating the core, see
    // SOLR-4982. Really, we should be able to just verify that the index files exist.

    // Should NOT be a datadir named ${DATA_TEST} (literal).
    File badDir = new File(workDir, "${DATA_TEST}");
    assertFalse("Should have substituted the sys var, found file " + badDir.getAbsolutePath(), badDir.exists());

    // For the other 3 vars, we couldn't get past creating the core if dereferencing didn't work correctly.

    // Should have segments in the directory pointed to by the ${DATA_TEST}.
    File test = new File(dataDir, "index");
    assertTrue("Should have found index dir at " + test.getAbsolutePath(), test.exists());
  }

  @Test
  public void testCannotCreateTwoCoresWithSameInstanceDir() throws Exception {

    setupCore(coreDuplicate, true);

    File workDir = new File(solrHomeDirectory, coreDuplicate);
    File data = new File(workDir, "data");

    // Create one core
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.NAME, coreDuplicate,
            CoreAdminParams.INSTANCE_DIR, workDir.getAbsolutePath(),
            CoreAdminParams.CONFIG, "solrconfig_ren.xml",
            CoreAdminParams.SCHEMA, "schema_ren.xml",
            CoreAdminParams.DATA_DIR, data.getAbsolutePath()),
            resp);
    assertNull("Exception on create", resp.getException());

    // Try to create another core with a different name, but the same instance dir
    SolrException e = expectThrows(SolrException.class, () -> {
      admin.handleRequestBody
          (req(CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.NAME, "different_name_core",
              CoreAdminParams.INSTANCE_DIR, workDir.getAbsolutePath(),
              CoreAdminParams.CONFIG, "solrconfig_ren.xml",
              CoreAdminParams.SCHEMA, "schema_ren.xml",
              CoreAdminParams.DATA_DIR, data.getAbsolutePath()),
              new SolrQueryResponse());
    });
    assertTrue(e.getMessage().contains("already defined there"));
  }

  @Test
  public void testInstanceDirAsPropertyParam() throws Exception {

    setupCore("testInstanceDirAsPropertyParam-XYZ", true);

    // make sure workDir is different even if core name is used as instanceDir
    File workDir = new File(solrHomeDirectory, "testInstanceDirAsPropertyParam-XYZ");
    File data = new File(workDir, "data");

    // Create one core
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.CREATE.toString(),
                CoreAdminParams.NAME, "testInstanceDirAsPropertyParam",
                "property.instanceDir", workDir.getAbsolutePath(),
                CoreAdminParams.CONFIG, "solrconfig_ren.xml",
                CoreAdminParams.SCHEMA, "schema_ren.xml",
                CoreAdminParams.DATA_DIR, data.getAbsolutePath()),
            resp);
    assertNull("Exception on create", resp.getException());

    resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.STATUS.toString(),
                CoreAdminParams.CORE, "testInstanceDirAsPropertyParam"),
            resp);
    NamedList status = (NamedList) resp.getValues().get("status");
    assertNotNull(status);
    NamedList coreProps = (NamedList) status.get("testInstanceDirAsPropertyParam");
    assertNotNull(status);
    String instanceDir = (String) coreProps.get("instanceDir");
    assertNotNull(instanceDir);
    assertEquals("Instance dir does not match param given in property.instanceDir syntax", workDir.getAbsolutePath(), new File(instanceDir).getAbsolutePath());
  }

  @Test
  public void testCreateSavesRegProps() throws Exception {

    setupCore(coreNormal, true);

    // create a new core (using CoreAdminHandler) w/ properties
    // Just to be sure it's NOT written to the core.properties file
    File workDir = new File(solrHomeDirectory, coreNormal);
    File data = new File(workDir, "data");

    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.NAME, coreNormal,
            CoreAdminParams.INSTANCE_DIR, workDir.getAbsolutePath(),
            CoreAdminParams.CONFIG, "solrconfig_ren.xml",
            CoreAdminParams.SCHEMA, "schema_ren.xml",
            CoreAdminParams.DATA_DIR, data.getAbsolutePath()),
            resp);
    assertNull("Exception on create", resp.getException());

    // verify props are in persisted file
    Properties props = new Properties();
    File propFile = new File(solrHomeDirectory, coreNormal + "/" + CorePropertiesLocator.PROPERTIES_FILENAME);
    FileInputStream is = new FileInputStream(propFile);
    try {
      props.load(new InputStreamReader(is, StandardCharsets.UTF_8));
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(is);
    }

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.NAME), coreNormal);

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.CONFIG), "solrconfig_ren.xml");

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.SCHEMA), "schema_ren.xml");

    assertEquals("Unexpected value preserved in properties file " + propFile.getAbsolutePath(),
        props.getProperty(CoreAdminParams.DATA_DIR), data.getAbsolutePath());

    assertEquals(props.size(), 4);

    //checkOnlyKnown(propFile);
    // For the other 3 vars, we couldn't get past creating the core if dereferencing didn't work correctly.

    // Should have segments in the directory pointed to by the ${DATA_TEST}.
    File test = new File(data, "index");
    assertTrue("Should have found index dir at " + test.getAbsolutePath(), test.exists());
  }

}
