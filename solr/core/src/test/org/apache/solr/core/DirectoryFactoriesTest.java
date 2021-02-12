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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;

import org.apache.solr.common.util.NamedList;

import org.apache.solr.SolrTestCaseJ4;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test some expected equivilencies of all DirectoryFactory implementations.
 * <p>
 * TODO: test more methods besides exists(String)
 * </p>
 */
public class DirectoryFactoriesTest extends SolrTestCaseJ4 {

  // TODO: what do we need to setup to be able to test HdfsDirectoryFactory?
  public static final List<Class<? extends DirectoryFactory>> ALL_CLASSES
    = Arrays.asList(MMapDirectoryFactory.class,
                    MockDirectoryFactory.class,
                    MockFSDirectoryFactory.class,
                    NRTCachingDirectoryFactory.class,
                    NIOFSDirectoryFactory.class,
                    RAMDirectoryFactory.class,
                    SimpleFSDirectoryFactory.class,
                    StandardDirectoryFactory.class);
  
  /* Test that MockDirectoryFactory's exist method behaves consistent w/other impls */
  public void testExistsEquivilence() throws Exception {
    // TODO: ideally we'd init all of these using DirectoryFactory.loadDirectoryFactory() ...
    // ...but the scaffolding needed for dealing with the CoreContainer/SolrConfig is a PITA

    for (Class<? extends DirectoryFactory> clazz : ALL_CLASSES) {
      testExistsBehavior(clazz);
    }
  }

  @SuppressWarnings({"rawtypes"})
  private void testExistsBehavior(Class<? extends DirectoryFactory> clazz) throws Exception {
    final String path = createTempDir().toString() + "/" + clazz + "_somedir";
    DirectoryFactory dirFac = null;
    try {
      dirFac = clazz.newInstance();
      dirFac.initCoreContainer(null); // greybox testing directly against path
      dirFac.init(new NamedList());

      assertFalse(path + " should not exist yet", dirFac.exists(path));
      Directory dir = dirFac.get(path, DirectoryFactory.DirContext.DEFAULT,
                                 DirectoryFactory.LOCK_TYPE_SINGLE);
      try {
        assertFalse(path + " should still not exist", dirFac.exists(path));
        try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
          file.writeInt(42);

          // TODO: even StandardDirectoryFactory & NRTCachingDirectoryFactory can't agree on this...
          // ... should we consider this explicitly undefined?
          // ... or should *all* Caching DirFactories consult the cache as well as the disk itself?
          // assertFalse(path + " should still not exist until file is closed", dirFac.exists(path));
          
        } // implicitly close file...
        
        // TODO: even StandardDirectoryFactory & NRTCachingDirectoryFactory can't agree on this...
        // ... should we consider this explicitly undefined?
        // ... or should *all* Caching DirFactories consult the cache as well as the disk itself?
        // assertTrue(path + " should exist once file is closed", dirFac.exists(path));
        
        dir.sync(Collections.singleton("test_file"));
        assertTrue(path + " should exist once file is synced", dirFac.exists(path));

        
      } finally {
        dirFac.release(dir);
      }
      assertTrue(path + " should still exist even after being released", dirFac.exists(path));
      
    } catch (AssertionError ae) {
      throw new AssertionError(clazz + ": " + ae.getMessage());
    } finally {
      if (null != dirFac) {
        dirFac.close();
      }
    }
  }
}
