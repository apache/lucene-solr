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
package org.apache.solr.servlet;

import java.net.URLEncoder;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;


public class DirectSolrConnectionTest extends SolrTestCaseJ4 {

  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solr/crazy-path-to-config.xml", "solr/crazy-path-to-schema.xml");
  }

  
  DirectSolrConnection direct;
  
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    direct = new DirectSolrConnection(h.getCore());
  }

  // Check that a request gets back the echoParams call
  public void testSimpleRequest() throws Exception 
  { 
    String pathAndParams = "/select?wt=xml&version=2.2&echoParams=explicit&q=*:*";
    
    String got = direct.request( pathAndParams, null );
    
    assertTrue( got.indexOf( "<str name=\"echoParams\">explicit</str>" ) > 5 );
    
    // It should throw an exception for unknown handler
    expectThrows(Exception.class, () -> direct.request( "/path to nonexistang thingy!!", null ));
  }
  

  // Check that a request gets back the echoParams call
  public void testInsertThenSelect() throws Exception 
  { 
    String value = "Kittens!!! \u20AC";
    String[] cmds = new String[] {
      "<delete><id>42</id></delete>",
      "<add><doc><field name=\"id\">42</field><field name=\"subject\">"+value+"</field></doc></add>",
      "<commit/>"
    };
    String getIt = "/select?wt=xml&q=id:42";
    
    // Test using the Stream body parameter
    for( String cmd : cmds ) {
      direct.request( "/update?"+CommonParams.STREAM_BODY+"="+URLEncoder.encode(cmd, "UTF-8"), null );
    }
    String got = direct.request( getIt, null );
    assertTrue( got.indexOf( value ) > 0 );
    
    // Same thing using the posted body
    for( String cmd : cmds ) {
      direct.request( "/update", cmd );
    }
    got = direct.request( getIt, null );
    assertTrue( got.indexOf( value ) > 0 );
  }
}
