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
import java.util.Locale;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.rest.schema.analysis.ManagedWordSetResource;
import org.junit.Ignore;
import org.junit.Test;
import org.noggit.JSONUtil;
import org.restlet.Request;
import org.restlet.data.Reference;

/**
 * Tests {@link RestManager} functionality, including resource registration,
 * and REST API requests and responses.
 */
public class TestRestManager extends SolrRestletTestBase {
  
  private class BogusManagedResource extends ManagedResource {

    protected BogusManagedResource(String resourceId,
        SolrResourceLoader loader, StorageIO storageIO) throws SolrException {
      super(resourceId, loader, storageIO);
    }

    @Override
    protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
        throws SolrException {}

    @Override
    protected Object applyUpdatesToManagedData(Object updates) {
      return null;
    }

    @Override
    public void doDeleteChild(BaseSolrResource endpoint, String childId) {}

    @Override
    public void doGet(BaseSolrResource endpoint, String childId) {}
    
  }
  
  private static class MockAnalysisComponent implements ManagedResourceObserver {

    @Override
    public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
        throws SolrException {
      assertTrue(res instanceof ManagedWordSetResource);      
    }
  }
  
  /**
   * Test RestManager initialization and handling of registered ManagedResources. 
   */
  @Test
  @Ignore
  public void testManagedResourceRegistrationAndInitialization() throws Exception {
    // first, we need to register some ManagedResources, which is done with the registry
    // provided by the SolrResourceLoader
    SolrResourceLoader loader = new SolrResourceLoader(Paths.get("./"));
    
    RestManager.Registry registry = loader.getManagedResourceRegistry();
    assertNotNull("Expected a non-null RestManager.Registry from the SolrResourceLoader!", registry);
    
    String resourceId = "/config/test/foo";
    registry.registerManagedResource(resourceId, 
                                     ManagedWordSetResource.class, 
                                     new MockAnalysisComponent());
    
    // verify the two different components can register the same ManagedResource in the registry
    registry.registerManagedResource(resourceId, 
                                     ManagedWordSetResource.class, 
                                     new MockAnalysisComponent());
    
    // verify we can register another resource under a different resourceId
    registry.registerManagedResource("/config/test/foo2", 
                                     ManagedWordSetResource.class, 
                                     new MockAnalysisComponent());

    ignoreException("REST API path .* already registered to instances of ");

    String failureMessage = "Should not be able to register a different"
                          + " ManagedResource implementation for {}";

    // verify that some other hooligan cannot register another type
    // of ManagedResource implementation under the same resourceId
    try {
      registry.registerManagedResource(resourceId, 
                                       BogusManagedResource.class, 
                                       new MockAnalysisComponent());
      fail(String.format(Locale.ROOT, failureMessage, resourceId));
    } catch (SolrException solrExc) {
      // expected output
    }

    resetExceptionIgnores();

    ignoreException("is a reserved endpoint used by the Solr REST API!");

    failureMessage = "Should not be able to register reserved endpoint {}";

    // verify that already-spoken-for REST API endpoints can't be registered
    Set<String> reservedEndpoints = registry.getReservedEndpoints();
    assertTrue(reservedEndpoints.size() > 2);
    assertTrue(reservedEndpoints.contains(RestManager.SCHEMA_BASE_PATH + RestManager.MANAGED_ENDPOINT));
    for (String endpoint : reservedEndpoints) {

      try {
        registry.registerManagedResource
            (endpoint, BogusManagedResource.class, new MockAnalysisComponent());
        fail(String.format(Locale.ROOT, failureMessage, endpoint));
      } catch (SolrException solrExc) {
        // expected output
      }

      // also try to register already-spoken-for REST API endpoints with a child segment
      endpoint += "/kid";
      try {
        registry.registerManagedResource
            (endpoint, BogusManagedResource.class, new MockAnalysisComponent());
        fail(String.format(Locale.ROOT, failureMessage, endpoint));
      } catch (SolrException solrExc) {
        // expected output
      }
    }

    resetExceptionIgnores();
    
    NamedList<String> initArgs = new NamedList<>();
    RestManager restManager = new RestManager();
    restManager.init(loader, initArgs, new ManagedResourceStorage.InMemoryStorageIO());
    
    ManagedResource res = restManager.getManagedResource(resourceId);
    assertTrue(res instanceof ManagedWordSetResource);    
    assertEquals(res.getResourceId(), resourceId);
    
    restManager.getManagedResource("/config/test/foo2"); // exception if it isn't registered
  }

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
    assertJPut(newEndpoint, JSONUtil.toJSON(Arrays.asList("this", "is", "a", "test")), "/responseHeader/status==0");

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
    Request testRequest = new Request();
    Reference rootRef = new Reference("http://solr.apache.org/");
    testRequest.setRootRef(rootRef);

    Reference resourceRef = new Reference("http://solr.apache.org/schema/analysis/synonyms/de");
    testRequest.setResourceRef(resourceRef);

    String resourceId = RestManager.ManagedEndpoint.resolveResourceId(testRequest);
    assertEquals(resourceId, "/schema/analysis/synonyms/de");
  }

  @Test
  public void testResolveResourceIdDecodeUrlEntities () throws Exception {
    Request testRequest = new Request();
    Reference rootRef = new Reference("http://solr.apache.org/");
    testRequest.setRootRef(rootRef);

    Reference resourceRef = new Reference("http://solr.apache.org/schema/analysis/synonyms/de/%C3%84ndern");
    testRequest.setResourceRef(resourceRef);

    String resourceId = RestManager.ManagedEndpoint.resolveResourceId(testRequest);
    assertEquals(resourceId, "/schema/analysis/synonyms/de/Ändern");
  }
}
