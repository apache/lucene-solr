package org.apache.solr.schema;
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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;

public class TestManagedSchema extends AbstractBadConfigTestBase {

  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";
  
  @Before
  private void initManagedSchemaCore() throws Exception {
    createTempDir();
    final String tmpSolrHomePath 
        = TEMP_DIR + File.separator + TestManagedSchema.class.getSimpleName() + System.currentTimeMillis();
    tmpSolrHome = new File(tmpSolrHomePath).getAbsoluteFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-managed-schema.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-basic.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-minimal.xml"), tmpConfDir);
    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    initCore("solrconfig-managed-schema.xml", "schema-minimal.xml", tmpSolrHome.getPath());
  }

  @After
  private void deleteCoreAndTempSolrHomeDirectory() throws Exception {
    deleteCore();
    FileUtils.deleteDirectory(tmpSolrHome);
  }
  
  public void testUpgrade() throws Exception {
    File managedSchemaFile = new File(tmpConfDir, "managed-schema");
    assertTrue(managedSchemaFile.exists());
    String managedSchema = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    assertTrue(managedSchema.contains("DO NOT EDIT"));
    File upgradedOriginalSchemaFile = new File(tmpConfDir, "schema-minimal.xml.bak");
    assertTrue(upgradedOriginalSchemaFile.exists());
    assertSchemaResource(collection, "managed-schema");
  }
  
  public void testUpgradeThenRestart() throws Exception {
    assertSchemaResource(collection, "managed-schema");
    deleteCore();
    File nonManagedSchemaFile = new File(tmpConfDir, "schema-minimal.xml");
    assertFalse(nonManagedSchemaFile.exists());
    initCore("solrconfig-managed-schema.xml", "schema-minimal.xml", tmpSolrHome.getPath());
    File managedSchemaFile = new File(tmpConfDir, "managed-schema");
    assertTrue(managedSchemaFile.exists());
    String managedSchema = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    assertTrue(managedSchema.contains("DO NOT EDIT"));
    File upgradedOriginalSchemaFile = new File(tmpConfDir, "schema-minimal.xml.bak");
    assertTrue(upgradedOriginalSchemaFile.exists());
    assertSchemaResource(collection, "managed-schema");
  }

  public void testUpgradeThenRestartNonManaged() throws Exception {
    deleteCore();
    // After upgrade to managed schema, fail to restart when solrconfig doesn't contain
    // <schemaFactory class="ManagedIndexSchemaFactory">...</schemaFactory>
    assertConfigs("solrconfig-basic.xml", "schema-minimal.xml", tmpSolrHome.getPath(),
                  "Can't find resource 'schema-minimal.xml'");
  }

  public void testUpgradeThenRestartNonManagedAfterPuttingBackNonManagedSchema() throws Exception {
    assertSchemaResource(collection, "managed-schema");
    deleteCore();
    File nonManagedSchemaFile = new File(tmpConfDir, "schema-minimal.xml");
    assertFalse(nonManagedSchemaFile.exists());
    File upgradedOriginalSchemaFile = new File(tmpConfDir, "schema-minimal.xml.bak");
    assertTrue(upgradedOriginalSchemaFile.exists());
    
    // After upgrade to managed schema, downgrading to non-managed should work after putting back the non-managed schema.
    FileUtils.moveFile(upgradedOriginalSchemaFile, nonManagedSchemaFile);
    initCore("solrconfig-basic.xml", "schema-minimal.xml", tmpSolrHome.getPath());
    assertSchemaResource(collection, "schema-minimal.xml");
  }
  
  private void assertSchemaResource(String collection, String expectedSchemaResource) throws Exception {
    final CoreContainer cores = h.getCoreContainer();
    cores.setPersistent(false);
    final CoreAdminHandler admin = new CoreAdminHandler(cores);
    SolrQueryRequest request = req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.STATUS.toString());
    SolrQueryResponse response = new SolrQueryResponse();
    admin.handleRequestBody(request, response);
    assertNull("Exception on create", response.getException());
    NamedList responseValues = response.getValues();
    NamedList status = (NamedList)responseValues.get("status");
    NamedList collectionStatus = (NamedList)status.get(collection);
    String collectionSchema = (String)collectionStatus.get(CoreAdminParams.SCHEMA);
    assertEquals("Schema resource name differs from expected name", expectedSchemaResource, collectionSchema);
  }
}
