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

package org.apache.solr.schema;

import java.util.regex.Pattern;
import java.util.LinkedList;
import java.util.List;

import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.AbstractSolrTestCase;



/**
 */
public class BadIndexSchemaTest extends AbstractSolrTestCase {

  private static final String bad_type = "StrField (bad_type)";

  @Override public String getSchemaFile() { return "bad-schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig.xml"; }

  @Override 
  public void setUp() throws Exception {
    ignoreException("_twice");
    ignoreException("ftAgain");
    ignoreException("fAgain");
    ignoreException(Pattern.quote(bad_type));

    super.setUp();
  }
  
  @Override 
  public void tearDown() throws Exception {
    SolrConfig.severeErrors.clear();
    super.tearDown();
  }

  
  private Throwable findErrorWithSubstring( List<Throwable> err, String v )
  {
    for( Throwable t : err ) {
      if( t.getMessage().indexOf( v ) > 0 ) {
        return t;
      }
    }
    return null;
  }
  
  
  public void testSevereErrors() 
  {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();

    for( Throwable t : SolrConfig.severeErrors ) {
      log.info( "got ex:"+t.getMessage() );
    }
    
    assertEquals( 4, SolrConfig.severeErrors.size() );

    List<Throwable> err = new LinkedList<Throwable>();
    err.addAll( SolrConfig.severeErrors );
    
    Throwable t = findErrorWithSubstring( err, "*_twice" );
    assertNotNull( t );
    err.remove( t );
    
    t = findErrorWithSubstring( err, "ftAgain" );
    assertNotNull( t );
    err.remove( t );
    
    t = findErrorWithSubstring( err, "fAgain" );
    assertNotNull( t );
    err.remove( t );

    t = findErrorWithSubstring( err, bad_type );
    assertNotNull( t );
    err.remove( t );

    // make sure thats all of them
    assertTrue( err.isEmpty() );
  }
}
