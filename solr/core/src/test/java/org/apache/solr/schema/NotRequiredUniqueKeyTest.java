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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a simple test to make sure the unique key is not required 
 * when it is specified as 'false' 
 * 
 * It needs its own file so it can load a special schema file
 */
public class NotRequiredUniqueKeyTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // usecase doesn't work with updateLog
    initCore("solrconfig.xml","schema-not-required-unique-key.xml");
  }

  @Test
  public void testSchemaLoading() 
  {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    SchemaField uniqueKey = schema.getUniqueKeyField();
    
    assertFalse( uniqueKey.isRequired() );
    
    assertFalse( schema.getRequiredFields().contains( uniqueKey ) );
  }
}
