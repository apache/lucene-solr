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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.junit.Test;

/**
 * Tests {@link ManagedResource} functionality.
 */
public class TestManagedResource extends SolrTestCaseJ4 {

  /**
   * Mock class that acts like an analysis component that depends on
   * data managed by a ManagedResource
   */
  private static class MockAnalysisComponent implements ManagedResourceObserver {
    
    private boolean wasNotified = false;

    @SuppressWarnings("unchecked")
    @Override
    public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res) throws SolrException {

      assertEquals("someVal", args.get("someArg"));

      assertTrue(res instanceof ManagedTestResource);      
      ManagedTestResource testRes = (ManagedTestResource)res;
      List<String> data = (List<String>)testRes.managedData;
      assertTrue(data.contains("1"));
      assertTrue(data.contains("2"));
      assertTrue(data.contains("3"));
      
      wasNotified = true;
    }
  }

  private class ManagedTestResource extends ManagedResource {

    private Object managedData;
    
    private ManagedTestResource(String resourceId, SolrResourceLoader loader,
        StorageIO storageIO) throws SolrException {
      super(resourceId, loader, storageIO);
    }

    @Override
    protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
        throws SolrException {

      assertNotNull(managedData);
      assertTrue(managedData instanceof List);

      // {'initArgs':{'someArg':'someVal', 'arg2':true, 'arg3':['one','two','three'],
      //              'arg4':18, 'arg5':0.9, 'arg6':{ 'uno':1, 'dos':2 }},'"
      assertEquals("someVal", managedInitArgs.get("someArg"));
      assertEquals(true, managedInitArgs.get("arg2"));
      List<String> arg3List = Arrays.asList("one", "two", "three");
      assertEquals(arg3List, managedInitArgs.get("arg3"));
      assertEquals(18L, managedInitArgs.get("arg4"));
      assertEquals(0.9, managedInitArgs.get("arg5"));
      Map<String,Long> arg6map = new LinkedHashMap<>(2);
      arg6map.put("uno", 1L);
      arg6map.put("dos", 2L);
      assertEquals(arg6map, managedInitArgs.get("arg6"));

      this.managedData = managedData;
    }

    
    // NOTE: These methods are better tested from the REST API
    // so they are stubbed out here and not used in this test
    
    @Override
    protected Object applyUpdatesToManagedData(Object updates) {
      return null;
    }

    @Override
    public void doDeleteChild(BaseSolrResource endpoint, String childId) {}

    @Override
    public void doGet(BaseSolrResource endpoint, String childId) {}    
  }

  /**
   * Implements a Java serialization based storage format.
   */
  @SuppressForbidden(reason = "XXX: security hole")
  private static class SerializableStorage extends ManagedResourceStorage {
    
    SerializableStorage(StorageIO storageIO, SolrResourceLoader loader) {
      super(storageIO, loader);
    }

    @SuppressForbidden(reason = "XXX: security hole")
    @Override
    public Object load(String resourceId) throws IOException {
      String storedId = getStoredResourceId(resourceId);
      InputStream inputStream = storageIO.openInputStream(storedId);
      if (inputStream == null) {
        return null;
      }
      Object serialized = null;
      ObjectInputStream ois = null;
      try {
        ois = new ObjectInputStream(inputStream);
        serialized = ois.readObject();
      } catch (ClassNotFoundException e) {
        // unlikely
        throw new IOException(e);
      } finally {
        if (ois != null) {
          try {
            ois.close();
          } catch (Exception ignore){}
        }
      }
      return serialized;      
    }

    @SuppressForbidden(reason = "XXX: security hole")
    @Override
    public void store(String resourceId, Object toStore) throws IOException {
      if (!(toStore instanceof Serializable))
        throw new IOException("Instance of "+
          toStore.getClass().getName()+" is not Serializable!");
      
      String storedId = getStoredResourceId(resourceId);
      ObjectOutputStream oos = null;
      try {
        oos = new ObjectOutputStream(storageIO.openOutputStream(storedId));
        oos.writeObject(toStore);
        oos.flush();
      } finally {
        if (oos != null) {
          try {
            oos.close();
          } catch (Exception ignore){}
        }
      }    
    }

    @Override
    public String getStoredResourceId(String resourceId) {
      return resourceId.replace('/','_')+".bin";
    }
  }  
  
  private class CustomStorageFormatResource extends ManagedTestResource {
    private CustomStorageFormatResource(String resourceId, SolrResourceLoader loader,
        StorageIO storageIO) throws SolrException {
      super(resourceId, loader, storageIO);
    }
    
    @Override
    protected ManagedResourceStorage createStorage(StorageIO storageIO, SolrResourceLoader loader) 
        throws SolrException
    {
      return new SerializableStorage(storageIO, loader); 
    }
  }

  /**
   * Tests managed data storage to and loading from {@link ManagedResourceStorage.InMemoryStorageIO}.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testLoadingAndStoringOfManagedData() throws Exception {
    String resourceId = "/config/test/foo";
    String storedResourceId = "_config_test_foo.json";
    
    MockAnalysisComponent observer = new MockAnalysisComponent();
    List<ManagedResourceObserver> observers = 
        Arrays.asList((ManagedResourceObserver)observer);
    
    // put some data in the storage impl so that we can test 
    // initialization of managed data from storage
    String storedJson = "{'initArgs':{'someArg':'someVal', 'arg2':true, 'arg3':['one','two','three'],"
                      + " 'arg4':18, 'arg5':0.9, 'arg6':{ 'uno':1, 'dos':2}},'"
                      + ManagedResource.MANAGED_JSON_LIST_FIELD+"':['1','2','3']}";
    ManagedResourceStorage.InMemoryStorageIO storageIO = 
        new ManagedResourceStorage.InMemoryStorageIO();
    storageIO.storage.put(storedResourceId, new BytesRef(json(storedJson)));
    
    ManagedTestResource res = 
        new ManagedTestResource(resourceId, new SolrResourceLoader(Paths.get("./")), storageIO);
    res.loadManagedDataAndNotify(observers);
    
    assertTrue("Observer was not notified by ManagedResource!", observer.wasNotified);

    // now update the managed data (as if it came from the REST API)
    List<String> updatedData = new ArrayList<>();
    updatedData.add("1");
    updatedData.add("2");
    updatedData.add("3");
    updatedData.add("4");    
    res.storeManagedData(updatedData);

    Map<String,Object> jsonObject =
        (Map<String,Object>) Utils.fromJSONString(storageIO.storage.get(storedResourceId).utf8ToString());
    List<String> jsonList = 
        (List<String>)jsonObject.get(ManagedResource.MANAGED_JSON_LIST_FIELD);
    
    assertTrue("Managed data was not updated correctly!", jsonList.contains("4"));    
  }
  
  /**
   * The ManagedResource storage framework allows the end developer to use a different
   * storage format other than JSON, as demonstrated by this test. 
   */
  @SuppressWarnings("rawtypes")
  @Test
  public void testCustomStorageFormat() throws Exception {
    String resourceId = "/schema/test/foo";
    String storedResourceId = "_schema_test_foo.bin";
    
    MockAnalysisComponent observer = new MockAnalysisComponent();
    List<ManagedResourceObserver> observers = 
        Arrays.asList((ManagedResourceObserver)observer);
    
    // put some data in the storage impl so that we can test 
    // initialization of managed data from storage
    Map<String,Object> storedData = new HashMap<>();
    Map<String,Object> initArgs = new HashMap<>();

    // {'initArgs':{'someArg':'someVal', 'arg2':true, 'arg3':['one','two','three'],
    //              'arg4':18, 'arg5':0.9, 'arg6':{ 'uno':1, 'dos':2 }},'"
    initArgs.put("someArg", "someVal");
    initArgs.put("arg2", Boolean.TRUE);
    List<String> arg3list = Arrays.asList("one", "two", "three");
    initArgs.put("arg3", arg3list);
    initArgs.put("arg4", 18L);
    initArgs.put("arg5", 0.9);
    Map<String,Long> arg6map = new HashMap<>();
    arg6map.put("uno", 1L);
    arg6map.put("dos", 2L);
    initArgs.put("arg6", arg6map);

    storedData.put("initArgs", initArgs);
    List<String> managedList = new ArrayList<>();
    managedList.add("1");
    managedList.add("2");
    managedList.add("3");
    storedData.put(ManagedResource.MANAGED_JSON_LIST_FIELD, managedList);
    ManagedResourceStorage.InMemoryStorageIO storageIO = 
        new ManagedResourceStorage.InMemoryStorageIO();
    storageIO.storage.put(storedResourceId, ser2bytes((Serializable)storedData));
    
    CustomStorageFormatResource res = 
        new CustomStorageFormatResource(resourceId, new SolrResourceLoader(Paths.get("./")), storageIO);
    res.loadManagedDataAndNotify(observers);
    
    assertTrue("Observer was not notified by ManagedResource!", observer.wasNotified);

    // now store some data (as if it came from the REST API)
    List<String> updatedData = new ArrayList<>();
    updatedData.add("1");
    updatedData.add("2");
    updatedData.add("3");
    updatedData.add("4");    
    res.storeManagedData(updatedData);
    
    Object stored = res.storage.load(resourceId);
    assertNotNull(stored);
    assertTrue(stored instanceof Map);
    Map storedMap = (Map)stored;
    assertNotNull(storedMap.get("initArgs"));
    List storedList = (List)storedMap.get(ManagedResource.MANAGED_JSON_LIST_FIELD);
    assertTrue(storedList.contains("4"));
  }


  /**
   * Converts the given Serializable object to bytes
   */
  @SuppressForbidden(reason = "XXX: security hole")
  private BytesRef ser2bytes(Serializable ser) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(out);
      oos.writeObject(ser);
      oos.flush();
    } finally {
      if (oos != null) {
        try {
          oos.close();
        } catch (Exception ignore){}
      }
    } 
    return new BytesRef(out.toByteArray());    
  }
}
