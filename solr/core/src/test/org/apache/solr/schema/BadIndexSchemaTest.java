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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrConfig;

import org.apache.solr.search.SolrIndexSearcher;
import org.junit.Test;

/**
 */
public class BadIndexSchemaTest extends SolrTestCaseJ4 {

  private void doTest(final String schema, final String errString) 
    throws Exception {

    ignoreException(Pattern.quote(errString));
    try {
      initCore( "solrconfig.xml", schema );
    } catch (SolrException e) {
      // short circuit out if we found what we expected
      if (-1 != e.getMessage().indexOf(errString)) return;
      // Test the cause too in case the expected error is wrapped
      if (-1 != e.getCause().getMessage().indexOf(errString)) return;

      // otherwise, rethrow it, possibly completley unrelated
      throw new SolrException
        (ErrorCode.SERVER_ERROR, 
         "Unexpected error, expected error matching: " + errString, e);
    } finally {
      SolrConfig.severeErrors.clear();
    }
    fail("Did not encounter any exception from: " + schema);
  }

  @Test
  public void testSevereErrorsForInvalidFieldOptions() throws Exception {
    doTest("bad-schema-not-indexed-but-norms.xml", "bad_field");
    doTest("bad-schema-not-indexed-but-tf.xml", "bad_field");
    doTest("bad-schema-not-indexed-but-pos.xml", "bad_field");
    doTest("bad-schema-omit-tf-but-not-pos.xml", "bad_field");
  }
  
  private Throwable findErrorWithSubstring( List<Throwable> err, String v )
  {
    for( Throwable t : err ) {
      if( t.getMessage().indexOf( v ) > -1 ) {
        return t;
      }
    }
    return null;
  }

  @Test
  public void testSevereErrors() throws Exception {
    final String bad_type = "StrField (bad_type)";
    try {
      initCore( "solrconfig.xml", "bad-schema.xml" );

      ignoreException("_twice");
      ignoreException("ftAgain");
      ignoreException("fAgain");
      ignoreException(Pattern.quote(bad_type));

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

      // make sure that's all of them
      assertTrue( err.isEmpty() );
    } finally {
      SolrConfig.severeErrors.clear();
      deleteCore();
    }
  }

  @Test
  public void testBadExternalFileField() throws Exception {
    try {
      initCore( "solrconfig.xml", "bad-schema-external-filefield.xml" );

      ignoreException("Only float and pfloat");

      for( Throwable t : SolrConfig.severeErrors ) {
        log.info( "got ex:"+t.getMessage() );
      }

      assertEquals( 1, SolrConfig.severeErrors.size() );

      List<Throwable> err = new LinkedList<Throwable>();
      err.addAll( SolrConfig.severeErrors );

      Throwable t = findErrorWithSubstring( err, "Only float and pfloat" );
      assertNotNull( t );
      err.remove( t );

      // make sure thats all of them
      assertTrue( err.isEmpty() );
    } finally {
      SolrConfig.severeErrors.clear();
      deleteCore();
    }
  }
}
