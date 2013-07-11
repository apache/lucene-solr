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

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXMLCoresLocator;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.util.Map;

public class CoreAdminHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  public String getCoreName() { return this.getClass().getName() + "_sys_vars"; }

  @Test
  public void testCreateWithSysVars() throws Exception {
    useFactory(null); // I require FS-based indexes for this test.

    final File workDir = new File(TEMP_DIR, getCoreName());

    if (workDir.exists()) {
      FileUtils.deleteDirectory(workDir);
    }
    assertTrue("Failed to mkdirs workDir", workDir.mkdirs());
    String coreName = "with_sys_vars";
    File instDir = new File(workDir, coreName);
    File subHome = new File(instDir, "conf");
    assertTrue("Failed to make subdirectory ", subHome.mkdirs());

    // Be sure we pick up sysvars when we create this
    String srcDir = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(srcDir, "schema-tiny.xml"), new File(subHome, "schema_ren.xml"));
    FileUtils.copyFile(new File(srcDir, "solrconfig-minimal.xml"), new File(subHome, "solrconfig_ren.xml"));
    FileUtils.copyFile(new File(srcDir, "solrconfig.snippet.randomindexconfig.xml"),
        new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));

    final CoreContainer cores = h.getCoreContainer();
    SolrXMLCoresLocator.NonPersistingLocator locator
        = (SolrXMLCoresLocator.NonPersistingLocator) cores.getCoresLocator();

    final CoreAdminHandler admin = new CoreAdminHandler(cores);

    // create a new core (using CoreAdminHandler) w/ properties
    System.setProperty("INSTDIR_TEST", instDir.getAbsolutePath());
    System.setProperty("CONFIG_TEST", "solrconfig_ren.xml");
    System.setProperty("SCHEMA_TEST", "schema_ren.xml");

    File dataDir = new File(workDir.getAbsolutePath(), "data_diff");
    System.setProperty("DATA_TEST", dataDir.getAbsolutePath());

    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
        (req(CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.NAME, getCoreName(),
            CoreAdminParams.INSTANCE_DIR, "${INSTDIR_TEST}",
            CoreAdminParams.CONFIG, "${CONFIG_TEST}",
            CoreAdminParams.SCHEMA, "${SCHEMA_TEST}",
            CoreAdminParams.DATA_DIR, "${DATA_TEST}"),
            resp);
    assertNull("Exception on create", resp.getException());

    // First assert that these values are persisted.
    h.validateXPath
        (locator.xml
            ,"/solr/cores/core[@name='" + getCoreName() + "' and @instanceDir='${INSTDIR_TEST}']"
            ,"/solr/cores/core[@name='" + getCoreName() + "' and @dataDir='${DATA_TEST}']"
            ,"/solr/cores/core[@name='" + getCoreName() + "' and @schema='${SCHEMA_TEST}']"
            ,"/solr/cores/core[@name='" + getCoreName() + "' and @config='${CONFIG_TEST}']"
        );

    // Now assert that certain values are properly dereferenced in the process of creating the core, see
    // SOLR-4982.

    // Should NOT be a datadir named ${DATA_TEST} (literal). This is the bug after all
    File badDir = new File(instDir, "${DATA_TEST}");
    assertFalse("Should have substituted the sys var, found file " + badDir.getAbsolutePath(), badDir.exists());

    // For the other 3 vars, we couldn't get past creating the core fi dereferencing didn't work correctly.

    // Should have segments in the directory pointed to by the ${DATA_TEST}.
    File test = new File(dataDir, "index");
    assertTrue("Should have found index dir at " + test.getAbsolutePath(), test.exists());
    test = new File(test,"segments.gen");
    assertTrue("Should have found segments.gen at " + test.getAbsolutePath(), test.exists());

    // Cleanup
    FileUtils.deleteDirectory(workDir);

  }

  @Test
  public void testCoreAdminHandler() throws Exception {
    final File workDir = new File(TEMP_DIR, this.getClass().getName());

    if (workDir.exists()) {
      FileUtils.deleteDirectory(workDir);
    }
    assertTrue("Failed to mkdirs workDir", workDir.mkdirs());
    
    final CoreContainer cores = h.getCoreContainer();

    final CoreAdminHandler admin = new CoreAdminHandler(cores);

    String instDir = null;
    {
      SolrCore template = null;
      try {
        template = cores.getCore("collection1");
        instDir = template.getCoreDescriptor().getInstanceDir();
      } finally {
        if (null != template) template.close();
      }
    }
    
    final File instDirFile = new File(instDir);
    assertTrue("instDir doesn't exist: " + instDir, instDirFile.exists());
    final File instPropFile = new File(workDir, "instProp");
    FileUtils.copyDirectory(instDirFile, instPropFile);
    
    // create a new core (using CoreAdminHandler) w/ properties
    
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
      (req(CoreAdminParams.ACTION,
           CoreAdminParams.CoreAdminAction.CREATE.toString(),
           CoreAdminParams.INSTANCE_DIR, instPropFile.getAbsolutePath(),
           CoreAdminParams.NAME, "props",
           CoreAdminParams.PROPERTY_PREFIX + "hoss","man",
           CoreAdminParams.PROPERTY_PREFIX + "foo","baz"),
       resp);
    assertNull("Exception on create", resp.getException());

    CoreDescriptor cd = cores.getCoreDescriptor("props");
    assertNotNull("Core not added!", cd);
    assertEquals(cd.getCoreProperty("hoss", null), "man");
    assertEquals(cd.getCoreProperty("foo", null), "baz");

    // attempt to create a bogus core and confirm failure
    ignoreException("Could not load config");
    try {
      resp = new SolrQueryResponse();
      admin.handleRequestBody
        (req(CoreAdminParams.ACTION, 
             CoreAdminParams.CoreAdminAction.CREATE.toString(),
             CoreAdminParams.NAME, "bogus_dir_core",
             CoreAdminParams.INSTANCE_DIR, "dir_does_not_exist_127896"),
         resp);
      fail("bogus collection created ok");
    } catch (SolrException e) {
      // :NOOP:
      // :TODO: CoreAdminHandler's exception messages are terrible, otherwise we could assert something useful here
    }
    unIgnoreException("Could not load config");

    // check specifically for status of the failed core name
    resp = new SolrQueryResponse();
    admin.handleRequestBody
      (req(CoreAdminParams.ACTION, 
           CoreAdminParams.CoreAdminAction.STATUS.toString(),
           CoreAdminParams.CORE, "bogus_dir_core"),
         resp);
    Map<String,Exception> failures = 
      (Map<String,Exception>) resp.getValues().get("initFailures");
    assertNotNull("core failures is null", failures);

    NamedList<Object> status = 
      (NamedList<Object>)resp.getValues().get("status");
    assertNotNull("core status is null", status);

    assertEquals("wrong number of core failures", 1, failures.size());
    Exception fail = failures.get("bogus_dir_core");
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getCause().getMessage(),
               0 < fail.getCause().getMessage().indexOf("dir_does_not_exist"));

    assertEquals("bogus_dir_core status isn't empty",
                 0, ((NamedList)status.get("bogus_dir_core")).size());

               
    // :TODO: because of SOLR-3665 we can't ask for status from all cores

    // cleanup
    FileUtils.deleteDirectory(workDir);

  }
}
