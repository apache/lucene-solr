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

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class UpdateRequestProcessorFactoryTest extends AbstractSolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-transformers.xml", "schema.xml");
  }

  public void testRequestTimeUrp(){
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams()
        .add("processor", "Template")
        .add("Template.field", "id_t:${firstName}_${lastName}")
        .add("Template.field", "another_t:${lastName}_${firstName}")
        .add("Template.field", "missing_t:${lastName}_${unKnown}");
    UpdateRequestProcessorChain chain = core.getUpdateProcessorChain(params);
    List<UpdateRequestProcessorFactory> l = chain.getProcessors();
    assertTrue(l.get(0) instanceof TemplateUpdateProcessorFactory);


  }
  
  public void testConfiguration() throws Exception 
  {
    SolrCore core = h.getCore();

    // make sure it loaded the factories
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain( "standard" );
    
    // Make sure it got 3 items (4 configured, 1 is enable=false)
    assertEquals("wrong number of (enabled) factories in chain",
                 3, chained.getProcessors().size() );

    // first one should be log, and it should be configured properly
    UpdateRequestProcessorFactory first = chained.getProcessors().get(0);
    assertEquals("wrong factory at front of chain",
                 LogUpdateProcessorFactory.class, first.getClass());
    LogUpdateProcessorFactory log = (LogUpdateProcessorFactory)first;
    assertEquals("wrong config for LogUpdateProcessorFactory.maxNumToLog",
                 100, log.maxNumToLog );
    assertEquals("wrong config for LogUpdateProcessorFactory.slowUpdateThresholdMillis",
                 2000, log.slowUpdateThresholdMillis);


    UpdateRequestProcessorChain custom = core.getUpdateProcessingChain( null );
    CustomUpdateRequestProcessorFactory link = (CustomUpdateRequestProcessorFactory) custom.getProcessors().get(0);
    
    assertEquals( custom, core.getUpdateProcessingChain( "" ) );
    assertEquals( custom, core.getUpdateProcessingChain( "custom" ) );
    
    // Make sure the NamedListArgs got through ok
    assertEquals( "{name={n8=88,n9=99}}", link.args.toString() );
  }

  public void testUpdateDistribChainSkipping() throws Exception {

    // a key part of this test is verifying that LogUpdateProcessor is found in all chains because it
    // is a @RunAlways processor -- but in order for that to work, we have to sanity check that the log
    // level is at least "INFO" otherwise the factory won't even produce a processor and all our assertions
    // are for nought.  (see LogUpdateProcessorFactory.getInstance)
    //
    // TODO: maybe create a new mock Processor w/ @RunAlways annot if folks feel requiring INFO is evil.
    assertTrue("Tests must be run with INFO level logging "+
               "otherwise LogUpdateProcessor isn't used and can't be tested.", log.isInfoEnabled());
    
    final int EXPECTED_CHAIN_LENGTH = 5;
    SolrCore core = h.getCore();
    for (final String name : Arrays.asList("distrib-chain-explicit",
                                           "distrib-chain-implicit",
                                           "distrib-chain-noop")) {

      UpdateRequestProcessor proc;
      List<UpdateRequestProcessor> procs;
      
      UpdateRequestProcessorChain chain = core.getUpdateProcessingChain(name);
      assertNotNull(name, chain);

      // either explicitly, or because of injection
      assertEquals(name + " chain length: " + chain.toString(), EXPECTED_CHAIN_LENGTH,
                   chain.getProcessors().size());

      // test a basic (non distrib) chain
      proc = chain.createProcessor(req(), new SolrQueryResponse());
      procs = procToList(proc);
      assertEquals(name + " procs size: " + procs.toString(),
                   // -1 = NoOpDistributingUpdateProcessorFactory produces no processor
                   EXPECTED_CHAIN_LENGTH - ("distrib-chain-noop".equals(name) ? 1 : 0),
                   procs.size());
      
      // Custom comes first in all three of our chains
      assertTrue(name + " first processor isn't a CustomUpdateRequestProcessor: " + procs.toString(),
                 ( // compare them both just because i'm going insane and the more checks the better
                   proc instanceof CustomUpdateRequestProcessor
                   && procs.get(0) instanceof CustomUpdateRequestProcessor));

      // Log should always come second in our chain.
      assertNotNull(name + " proc.next is null", proc.next);
      assertNotNull(name + " second proc is null", procs.get(1));

      assertTrue(name + " second proc isn't LogUpdateProcessor: " + procs.toString(),
                 ( // compare them both just because i'm going insane and the more checks the better
                   proc.next instanceof LogUpdateProcessorFactory.LogUpdateProcessor
                   && procs.get(1) instanceof LogUpdateProcessorFactory.LogUpdateProcessor));

      // fetch the distributed version of this chain
      proc = chain.createProcessor(req(DISTRIB_UPDATE_PARAM, "non_blank_value"),
                                   new SolrQueryResponse());
      procs = procToList(proc);
      assertNotNull(name + " (distrib) chain produced null proc", proc);
      assertFalse(name + " (distrib) procs is empty", procs.isEmpty());

      // for these 3 (distrib) chains, the first proc should always be LogUpdateProcessor
      assertTrue(name + " (distrib) first proc should be LogUpdateProcessor because of @RunAllways: "
                 + procs.toString(),
                 ( // compare them both just because i'm going insane and the more checks the better
                   proc instanceof LogUpdateProcessorFactory.LogUpdateProcessor
                   && procs.get(0) instanceof LogUpdateProcessorFactory.LogUpdateProcessor));

      // for these 3 (distrib) chains, the last proc should always be RunUpdateProcessor
      assertTrue(name + " (distrib) last processor isn't a RunUpdateProcessor: " + procs.toString(),
                 procs.get(procs.size()-1) instanceof RunUpdateProcessor );

      // either 1 proc was droped in distrib mode, or 1 for the "implicit" chain
      assertEquals(name + " (distrib) chain has wrong length: " + procs.toString(),
                   // -1 = all chains lose CustomUpdateRequestProcessorFactory
                   // -1 = distrib-chain-noop: NoOpDistributingUpdateProcessorFactory produces no processor
                   // -1 = distrib-chain-implicit: does RemoveBlank before distrib
                   EXPECTED_CHAIN_LENGTH - ( "distrib-chain-explicit".equals(name) ? 1 : 2),
                   procs.size());
    }

  }

  /**
   * walks the "next" values of the proc building up a List of the procs for easier testing
   */
  public static List<UpdateRequestProcessor> procToList(UpdateRequestProcessor proc) {
    List<UpdateRequestProcessor> result = new ArrayList<UpdateRequestProcessor>(7);
    while (null != proc) {
      result.add(proc);
      proc = proc.next;
    }
    return result;
  }
}

