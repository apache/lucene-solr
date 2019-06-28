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
package org.apache.solr.update;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;

public class TestAtomicUpdateErrorCases extends SolrTestCaseJ4 {

  public void testUpdateNoTLog() throws Exception {
    try {
      System.setProperty("enable.update.log", "false");
      initCore("solrconfig.xml","schema15.xml");
      
      UpdateHandler uh = h.getCore().getUpdateHandler();
      assertTrue("this test requires DirectUpdateHandler2",
                 uh instanceof DirectUpdateHandler2);

      assertNull("this test requires that the updateLog not be enabled, it " +
                 "seems that someone modified the configs",
                 ((DirectUpdateHandler2)uh).getUpdateLog());
      
      // creating docs should work fine
      addAndGetVersion(sdoc("id", "1", "val_i", "42"), null);
      assertU(commit());

      try {
        ignoreException("updateLog");

        // updating docs should fail
        addAndGetVersion(sdoc("id", "1", "val_i", map("inc",-666)), null);
        
        fail("didn't get error about needing updateLog");
      } catch (SolrException ex) {
        assertEquals(400, ex.code());
        // if the message doesn't match our expectation, wrap & rethrow
        if (ex.getMessage().indexOf("unless <updateLog/> is configured") < 0) {
          throw new RuntimeException("exception message is not expected", ex);
        }
      } finally {
        resetExceptionIgnores();
      }

    } finally {
      System.clearProperty("enable.update.log");
      deleteCore();
    }
  }

  public void testUpdateNoDistribProcessor() throws Exception {
    try {
      initCore("solrconfig-tlog.xml","schema15.xml");
      
      assertNotNull("this test requires an update chain named 'nodistrib'",
                    h.getCore().getUpdateProcessingChain("nodistrib")); 


      // creating docs should work fine
      addAndGetVersion(sdoc("id", "1", "val_i", "42"), 
                       params("update.chain","nodistrib"));
      assertU(commit());

      try {
        ignoreException("DistributedUpdateProcessorFactory");

        // updating docs should fail
        addAndGetVersion(sdoc("id", "1", "val_i", map("inc",-666)), 
                         params("update.chain","nodistrib"));
        
        fail("didn't get error about needing DistributedUpdateProcessorFactory");
      } catch (SolrException ex) {
        assertEquals(400, ex.code());
        // if the message doesn't match our expectation, wrap & rethrow
        if (ex.getMessage().indexOf("DistributedUpdateProcessorFactory") < 0) {
          throw new RuntimeException("exception message is not expected", ex);
        }
      } finally {
        resetExceptionIgnores();
      }

    } finally {
      deleteCore();
    }
  }

}
