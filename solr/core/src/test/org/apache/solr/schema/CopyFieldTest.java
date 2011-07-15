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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a simple test to make sure the <code>CopyField</code> works.
 * It uses its own special schema file.
 *
 * @since solr 1.4
 */
public class CopyFieldTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-copyfield-test.xml");
  }    

  @Test
  public void testCopyFieldSchemaFieldSchemaField() {
    try {
      new CopyField(new SchemaField("source", new TextField()), null);
      fail("CopyField failed with null SchemaField argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
    }
    try {
      new CopyField(null, new SchemaField("destination", new TextField()));
      fail("CopyField failed with null SchemaField argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
    }
    try {
      new CopyField(null, null);
      fail("CopyField failed with null SchemaField argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
    }
  }

  @Test
  public void testCopyFieldSchemaFieldSchemaFieldInt() {
    try {
      new CopyField(null,
          new SchemaField("destination", new TextField()), 1000);
      fail("CopyField failed with null SchemaField argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
    }
    try {
      new CopyField(new SchemaField("source", new TextField()), null,
          1000);
      fail("CopyField failed with null SchemaField argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
    }
    try {
      new CopyField(null, null, 1000);
      fail("CopyField failed with null SchemaField argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
    }
    try {
      new CopyField(new SchemaField("source", new TextField()),
          new SchemaField("destination", new TextField()), -1000);
      fail("CopyField failed with negative length argument.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains(
          "can't have a negative value"));
    }
    new CopyField(new SchemaField("source", new TextField()),
        new SchemaField("destination", new TextField()), CopyField.UNLIMITED);
  }

  @Test
  public void testGetSource() {
    final CopyField copyField = new CopyField(new SchemaField("source",
        new TextField()), new SchemaField("destination",
        new TextField()), 1000);
    assertEquals("source", copyField.getSource().name);
  }

  @Test
  public void testGetDestination() {
    final CopyField copyField = new CopyField(new SchemaField("source",
        new TextField()), new SchemaField("destination",
        new TextField()), 1000);
    assertEquals("destination", copyField.getDestination().name);
  }

  @Test
  public void testGetMaxChars() {
    final CopyField copyField = new CopyField(new SchemaField("source",
        new TextField()), new SchemaField("destination",
        new TextField()), 1000);
    assertEquals(1000, copyField.getMaxChars());
  }

  @Test
  public void testCopyFieldFunctionality() 
    {
      SolrCore core = h.getCore();
      assertU(adoc("id", "10", "title", "test copy field", "text_en", "this is a simple test of the copy field functionality"));
      assertU(commit());
      
      Map<String,String> args = new HashMap<String, String>();
      args.put( CommonParams.Q, "text_en:simple" );
      args.put( "indent", "true" );
      SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      
      assertQ("Make sure they got in", req
              ,"//*[@numFound='1']"
              ,"//result/doc[1]/int[@name='id'][.='10']"
              );
      
      args = new HashMap<String, String>();
      args.put( CommonParams.Q, "highlight:simple" );
      args.put( "indent", "true" );
      req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      assertQ("dynamic source", req
              ,"//*[@numFound='1']"
              ,"//result/doc[1]/int[@name='id'][.='10']"
              ,"//result/doc[1]/arr[@name='highlight']/str[.='this is a simple test of ']"
              );

      args = new HashMap<String, String>();
      args.put( CommonParams.Q, "text_en:functionality" );
      args.put( "indent", "true" );
      req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      assertQ("Make sure they got in", req
              ,"//*[@numFound='1']");
      
      args = new HashMap<String, String>();
      args.put( CommonParams.Q, "highlight:functionality" );
      args.put( "indent", "true" );
      req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      assertQ("dynamic source", req
              ,"//*[@numFound='0']");
    }
}
