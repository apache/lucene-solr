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
package org.apache.solr.rest;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.rest.schema.analysis.ManagedWordSetResource;
import org.junit.Test;

/**
 * Tests {@link RestManager} functionality, including resource registration,
 * and REST API requests and responses.
 */
public class TestRestManager extends SolrRestletTestBase {

  /**
   * Tests {@link RestManager}'s responses to REST API requests on /config/managed
   * and /schema/managed.  Also tests {@link ManagedWordSetResource} functionality
   * through the REST API.
   */
  @Test
  public void testRestManagerEndpoints() throws Exception {
    // relies on these ManagedResources being activated in the schema-rest.xml used by this test
    assertJQ("/schema/managed",
             "/responseHeader/status==0");
    /*
     * TODO: can't assume these will be here unless schema-rest.xml includes these declarations
     * 
             "/managedResources/[0]/class=='org.apache.solr.rest.schema.analysis.ManagedWordSetResource'",
             "/managedResources/[0]/resourceId=='/schema/analysis/stopwords/english'",
             "/managedResources/[1]/class=='org.apache.solr.rest.schema.analysis.ManagedSynonymGraphFilterFactory$SynonymManager'",
             "/managedResources/[1]/resourceId=='/schema/analysis/synonyms/englishgraph'");
    */
    
    // no pre-existing managed config components
//    assertJQ("/config/managed", "/managedResources==[]");
        
    // add a ManagedWordSetResource for managing protected words (for stemming)
    String newEndpoint = "/schema/analysis/protwords/english";
    
    assertJPut(newEndpoint, json("{ 'class':'solr.ManagedWordSetResource' }"), "/responseHeader/status==0");
        
    assertJQ("/schema/managed"
        ,"/managedResources/[0]/class=='org.apache.solr.rest.schema.analysis.ManagedWordSetResource'"
        ,"/managedResources/[0]/resourceId=='/schema/analysis/protwords/english'");
    
    // query the resource we just created
    assertJQ(newEndpoint, "/wordSet/managedList==[]");
    
    // add some words to this new word list manager
    assertJPut(newEndpoint, Utils.toJSONString(Arrays.asList("this", "is", "a", "test")), "/responseHeader/status==0");

    assertJQ(newEndpoint
        ,"/wordSet/managedList==['a','is','test','this']"
        ,"/wordSet/initArgs=={'ignoreCase':false}"); // make sure the default is serialized even if not specified

    // Test for case-sensitivity - "Test" lookup should fail
    assertJQ(newEndpoint + "/Test", "/responseHeader/status==404");

    // Switch to case-insensitive
    assertJPut(newEndpoint, json("{ 'initArgs':{ 'ignoreCase':'true' } }"), "/responseHeader/status==0");

    // Test for case-insensitivity - "Test" lookup should succeed
    assertJQ(newEndpoint + "/Test", "/responseHeader/status==0");

    // Switch to case-sensitive - this request should fail: changing ignoreCase from true to false is not permitted
    assertJPut(newEndpoint, json("{ 'initArgs':{ 'ignoreCase':false } }"), "/responseHeader/status==400");

    // Test XML response format
    assertQ(newEndpoint + "?wt=xml"
        ,"/response/lst[@name='responseHeader']/int[@name='status']=0"
        ,"/response/lst[@name='wordSet']/arr[@name='managedList']/str[1]='a'"
        ,"/response/lst[@name='wordSet']/arr[@name='managedList']/str[2]='is'"
        ,"/response/lst[@name='wordSet']/arr[@name='managedList']/str[3]='test'"
        ,"/response/lst[@name='wordSet']/arr[@name='managedList']/str[4]='this'");

    // delete the one we created above
    assertJDelete(newEndpoint, "/responseHeader/status==0");

    // make sure it's really gone
//    assertJQ("/config/managed", "/managedResources==[]");
  }
  
  @Test
  public void testReloadFromPersistentStorage() throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader(Paths.get("./"));
    File unitTestStorageDir = createTempDir("testRestManager").toFile();
    assertTrue(unitTestStorageDir.getAbsolutePath()+" is not a directory!", 
        unitTestStorageDir.isDirectory());    
    assertTrue(unitTestStorageDir.canRead());
    assertTrue(unitTestStorageDir.canWrite());

    NamedList<String> ioInitArgs = new NamedList<>();
    ioInitArgs.add(ManagedResourceStorage.STORAGE_DIR_INIT_ARG, 
        unitTestStorageDir.getAbsolutePath());
    
    StorageIO storageIO = new ManagedResourceStorage.FileStorageIO();
    storageIO.configure(loader, ioInitArgs);
    
    NamedList<String> initArgs = new NamedList<>();
    RestManager restManager = new RestManager();
    restManager.init(loader, initArgs, storageIO);
    
    // verifies a RestManager can be reloaded from a previous RestManager's data
    RestManager restManager2 = new RestManager();
    restManager2.init(loader, initArgs, storageIO);    
  }

  @Test
  public void testResolveResourceId () throws Exception {
    String path = "http://solr.apache.org/schema/analysis/synonyms/de";
    String resourceId = RestManager.ManagedEndpoint.resolveResourceId(path);
    assertEquals(resourceId, "/schema/analysis/synonyms/de");
  }

  @Test
  public void testResolveResourceIdDecodeUrlEntities () throws Exception {
    String path = "http://solr.apache.org/schema/analysis/synonyms/de/%C3%84ndern";
    String resourceId = RestManager.ManagedEndpoint.resolveResourceId(path);
    assertEquals(resourceId, "/schema/analysis/synonyms/de/Ändern");
  }
}
