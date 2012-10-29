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

import java.util.Arrays;

import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;

/**
 * 
 */
public class UpdateRequestProcessorFactoryTest extends AbstractSolrTestCase {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-transformers.xml", "schema.xml");
  }
  

  public void testConfiguration() throws Exception 
  {
    SolrCore core = h.getCore();

    // make sure it loaded the factories
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain( "standard" );
    
    // Make sure it got 3 items (4 configured, 1 is enable=false)
    assertEquals("wrong number of (enabled) factories in chain",
                 3, chained.getFactories().length );

    // first one should be log, and it should be configured properly
    UpdateRequestProcessorFactory first = chained.getFactories()[0];
    assertEquals("wrong factory at front of chain",
                 LogUpdateProcessorFactory.class, first.getClass());
    LogUpdateProcessorFactory log = (LogUpdateProcessorFactory)first;
    assertEquals("wrong config for LogUpdateProcessorFactory",
                 100, log.maxNumToLog );
    
    
    UpdateRequestProcessorChain custom = core.getUpdateProcessingChain( null );
    CustomUpdateRequestProcessorFactory link = (CustomUpdateRequestProcessorFactory) custom.getFactories()[0];
    
    assertEquals( custom, core.getUpdateProcessingChain( "" ) );
    assertEquals( custom, core.getUpdateProcessingChain( "custom" ) );
    
    // Make sure the NamedListArgs got through ok
    assertEquals( "{name={n8=88,n9=99}}", link.args.toString() );
  }

  public void testUpdateDistribChainSkipping() throws Exception {
    SolrCore core = h.getCore();
    for (final String name : Arrays.asList("distrib-chain-explicit",
                                           "distrib-chain-implicit",
                                           "distrib-chain-noop")) {

      UpdateRequestProcessor proc;
      UpdateRequestProcessorChain chain = core.getUpdateProcessingChain(name);
      assertNotNull(name, chain);

      // either explicitly, or because of injection
      assertEquals(name + " chain length", 4,
                   chain.getFactories().length);

      // Custom comes first in all three of our chains
      proc = chain.createProcessor(req(), new SolrQueryResponse());
      assertTrue(name + " first processor isn't a CustomUpdateRequestProcessor: " 
                 + proc.getClass().getName(),
                 proc instanceof CustomUpdateRequestProcessor);

      // varies depending on chain, but definitely shouldn't be Custom
      proc = chain.createProcessor(req(DISTRIB_UPDATE_PARAM, "non_blank_value"),
                                   new SolrQueryResponse());
      assertFalse(name + " post distrib proc should not be a CustomUpdateRequestProcessor: " 
                 + proc.getClass().getName(),
                 proc instanceof CustomUpdateRequestProcessor);
      

    }

  }

}
