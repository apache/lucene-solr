/**
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

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * 
 */
public class UpdateRequestProcessorFactoryTest extends AbstractSolrTestCase {

  @Override public String getSchemaFile()     { return "schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig-transformers.xml"; }
  

  public void testConfiguration() throws Exception 
  {
    SolrCore core = h.getCore();

    // make sure it loaded the factories
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain( "standard" );
    
    // Make sure it got 3 items and configured the Log chain ok
    assertEquals( 3, chained.getFactories().length );
    LogUpdateProcessorFactory log = (LogUpdateProcessorFactory)chained.getFactories()[0];
    assertEquals( 100, log.maxNumToLog );
    
    
    UpdateRequestProcessorChain custom = core.getUpdateProcessingChain( null );
    CustomUpdateRequestProcessorFactory link = (CustomUpdateRequestProcessorFactory) custom.getFactories()[0];
    
    assertEquals( custom, core.getUpdateProcessingChain( "" ) );
    assertEquals( custom, core.getUpdateProcessingChain( "custom" ) );
    
    // Make sure the NamedListArgs got through ok
    assertEquals( "{name={n8=88,n9=99}}", link.args.toString() );
  }
}
