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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.MockFSDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;

public class CoreMergeIndexesAdminHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    useFactory(FailingDirectoryFactory.class.getName());
    initCore("solrconfig.xml", "schema.xml");
  }

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());


  private static String WRAPPED_FAILING_MSG = "Error handling 'mergeindexes' action";
  private static String FAILING_CAUSE_MSG = "Creating a directory using FailingDirectoryFactoryException always fails";
  public static class FailingDirectoryFactory extends MockFSDirectoryFactory {
    public static class FailingDirectoryFactoryException extends RuntimeException {
      public FailingDirectoryFactoryException() {
        super(FAILING_CAUSE_MSG);
      }
    }

    public boolean fail = false;
    @Override
    public Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
      if (fail) {
        throw new FailingDirectoryFactoryException();
      } else {
        return super.create(path, lockFactory, dirContext);
      }
    }
  }

  @Test
  public void testMergeIndexesCoreAdminHandler() throws Exception {
    final File workDir = createTempDir().toFile();

    final CoreContainer cores = h.getCoreContainer();
    cores.getAllowPaths().add(workDir.toPath());

    try (final CoreAdminHandler admin = new CoreAdminHandler(cores);
         SolrCore core = cores.getCore("collection1")) {
      DirectoryFactory df = core.getDirectoryFactory();
      FailingDirectoryFactory dirFactory = (FailingDirectoryFactory) df;

      try {
        dirFactory.fail = true;
        ignoreException(WRAPPED_FAILING_MSG);
        SolrException e = expectThrows(SolrException.class, () -> {
          admin.handleRequestBody
              (req(CoreAdminParams.ACTION,
                  CoreAdminParams.CoreAdminAction.MERGEINDEXES.toString(),
                  CoreAdminParams.CORE, "collection1",
                  CoreAdminParams.INDEX_DIR, workDir.getAbsolutePath()),
                  new SolrQueryResponse());
        });
        assertEquals(FailingDirectoryFactory.FailingDirectoryFactoryException.class, e.getCause().getClass());
      } finally {
        unIgnoreException(WRAPPED_FAILING_MSG);
      }
      dirFactory.fail = false;
    }
  }
}
