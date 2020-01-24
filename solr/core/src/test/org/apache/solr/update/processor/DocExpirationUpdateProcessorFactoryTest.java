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
package org.apache.solr.update.processor;

import org.apache.solr.common.SolrInputDocument;

import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

import org.junit.BeforeClass;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests various configurations of DocExpirationUpdateProcessorFactory
 */
public class DocExpirationUpdateProcessorFactoryTest extends UpdateProcessorTestBase {
  
  public static final String CONFIG_XML = "solrconfig-doc-expire-update-processor.xml";
  public static final String SCHEMA_XML = "schema15.xml";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(CONFIG_XML, SCHEMA_XML);
  }

  public void testTTLDefaultsConversion() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("convert-ttl-defaults",
                   params("NOW","1394059630042"),
                   doc(f("id", "1111"),
                       f("_ttl_","+5MINUTES")));
    assertNotNull(d);
    assertEquals(new Date(1394059930042L), d.getFieldValue("_expire_at_tdt"));

    d = processAdd("convert-ttl-defaults",
                   params("NOW","1394059630042",
                          "_ttl_","+5MINUTES"),
                   doc(f("id", "1111")));

    assertNotNull(d);
    assertEquals(new Date(1394059930042L), d.getFieldValue("_expire_at_tdt"));
  }

  public void testTTLFieldConversion() throws Exception {
    final String chain = "convert-ttl-field";
    SolrInputDocument d = null;
    d = processAdd(chain,
                   params("NOW","1394059630042"),
                   doc(f("id", "1111"),
                       f("_ttl_field_","+5MINUTES")));
    assertNotNull(d);
    assertEquals(new Date(1394059930042L), d.getFieldValue("_expire_at_tdt"));

    d = processAdd(chain,
                   params("NOW","1394059630042"),
                   doc(f("id", "2222"),
                       f("_ttl_field_","+27MINUTES")));
    assertNotNull(d);
    assertEquals(new Date(1394061250042L), d.getFieldValue("_expire_at_tdt"));

    d = processAdd(chain,
                   params("NOW","1394059630042"),
                   doc(f("id", "3333"),
                       f("_ttl_field_","+1YEAR")));
    assertNotNull(d);
    assertEquals(new Date(1425595630042L), d.getFieldValue("_expire_at_tdt"));

    d = processAdd(chain,
                   params("NOW","1394059630042"),
                   doc(f("id", "1111"),
                       f("_ttl_field_","/DAY+1YEAR")));
    assertNotNull(d);
    assertEquals(new Date(1425513600000L), d.getFieldValue("_expire_at_tdt"));

    // default ttlParamName is disabled, this should not convert...
    d = processAdd(chain,
                   params("NOW","1394059630042",
                          "_ttl_","+5MINUTES"),
                   doc(f("id", "1111")));
    assertNotNull(d);
    assertNull(d.getFieldValue("_expire_at_tdt"));
  }

  public void testTTLParamConversion() throws Exception {
    final String chain = "convert-ttl-param";
    SolrInputDocument d = null;
    d = processAdd(chain,
                   params("NOW","1394059630042",
                          "_ttl_param_","+5MINUTES"),
                   doc(f("id", "1111")));

    assertNotNull(d);
    assertEquals(new Date(1394059930042L), d.getFieldValue("_expire_at_tdt"));

    d = processAdd(chain,
                   params("NOW","1394059630042",
                          "_ttl_param_","+27MINUTES"),
                   doc(f("id", "2222")));
    assertNotNull(d);
    assertEquals(new Date(1394061250042L), d.getFieldValue("_expire_at_tdt"));

    // default ttlFieldName is disabled, param should be used...
    d = processAdd(chain,
                   params("NOW","1394059630042",
                          "_ttl_param_","+5MINUTES"),
                   doc(f("id", "1111"),
                       f("_ttl_field_","+999MINUTES")));
    assertNotNull(d);
    assertEquals(new Date(1394059930042L), d.getFieldValue("_expire_at_tdt"));

    // default ttlFieldName is disabled, this should not convert...
    d = processAdd(chain,
                   params("NOW","1394059630042"),
                   doc(f("id", "1111"),
                       f("_ttl_","/DAY+1YEAR")));
    assertNotNull(d);
    assertNull(d.getFieldValue("_expire_at_tdt"));
  }

  public void testTTLFieldConversionWithDefaultParam() throws Exception {
    final String chain = "convert-ttl-field-with-param-default";
    SolrInputDocument d = null;
    d = processAdd(chain,
                   params("NOW","1394059630042",
                          "_ttl_param_","+999MINUTES"),
                   doc(f("id", "1111"),
                       f("_ttl_field_","+5MINUTES")));
    assertNotNull(d);
    assertEquals(new Date(1394059930042L), d.getFieldValue("_expire_at_tdt"));

    d = processAdd(chain,
                   params("NOW","1394059630042",
                          "_ttl_param_","+27MINUTES"),
                   doc(f("id", "2222")));
    assertNotNull(d);
    assertEquals(new Date(1394061250042L), d.getFieldValue("_expire_at_tdt"));

  }

  public void testAutomaticDeletes() throws Exception {

    // get a handle on our recorder

    UpdateRequestProcessorChain chain = 
      h.getCore().getUpdateProcessingChain("scheduled-delete");

    assertNotNull(chain);

    List<UpdateRequestProcessorFactory> factories = chain.getProcessors();
    assertEquals("did number of processors configured in chain get changed?", 
                 5, factories.size());
    assertTrue("Expected [1] RecordingUpdateProcessorFactory: " + factories.get(1).getClass(),
               factories.get(1) instanceof RecordingUpdateProcessorFactory);
    RecordingUpdateProcessorFactory recorder = 
      (RecordingUpdateProcessorFactory) factories.get(1);

    // now start recording, and monitor for the expected commands

    try {
      recorder.startRecording();
      
      // more then one iter to verify it's recurring
      final int numItersToCheck = 1 + RANDOM_MULTIPLIER;
      
      for (int i = 0; i < numItersToCheck; i++) { 
        UpdateCommand tmp;
        
        // be generous in how long we wait, some jenkins machines are slooooow
        tmp = recorder.commandQueue.poll(30, TimeUnit.SECONDS);
        
        // we can be confident in the order because DocExpirationUpdateProcessorFactory
        // uses the same request for both the delete & the commit -- and both 
        // RecordingUpdateProcessorFactory's getInstance & startRecording methods are 
        // synchronized.  So it should not be possible to start recording in the 
        // middle of the two commands
        assertTrue("expected DeleteUpdateCommand: " + tmp.getClass(),
                   tmp instanceof DeleteUpdateCommand);
        
        DeleteUpdateCommand delete = (DeleteUpdateCommand) tmp;
        assertFalse(delete.isDeleteById());
        assertNotNull(delete.getQuery());
        assertTrue(delete.getQuery(), 
                   delete.getQuery().startsWith("{!cache=false}eXpField_tdt:[* TO "));
        
        // commit should be immediately after the delete
        tmp = recorder.commandQueue.poll(5, TimeUnit.SECONDS);
        assertTrue("expected CommitUpdateCommand: " + tmp.getClass(),
                   tmp instanceof CommitUpdateCommand);
        
        CommitUpdateCommand commit = (CommitUpdateCommand) tmp;
        assertTrue(commit.softCommit);
        assertTrue(commit.openSearcher);
      } 
    } finally {
      recorder.stopRecording();
    }
  }


}
