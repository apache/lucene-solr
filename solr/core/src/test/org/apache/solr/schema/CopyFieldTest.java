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
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(new SchemaField("source", new TextField()), null);
    });
    assertTrue(e.getLocalizedMessage().contains("can't be NULL"));

    e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(null, new SchemaField("destination", new TextField()));
    });
    assertTrue(e.getLocalizedMessage().contains("can't be NULL"));

    e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(null, null);
    });
    assertTrue(e.getLocalizedMessage().contains("can't be NULL"));
  }

  @Test
  public void testCopyFieldSchemaFieldSchemaFieldInt() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(null, new SchemaField("destination", new TextField()), 1000);
    });
    assertTrue(e.getLocalizedMessage().contains("can't be NULL"));

    e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(new SchemaField("source", new TextField()), null, 1000);
    });
    assertTrue(e.getLocalizedMessage().contains("can't be NULL"));

    e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(null, null, 1000);
    });
    assertTrue(e.getLocalizedMessage().contains("can't be NULL"));

    e = expectThrows(IllegalArgumentException.class, () -> {
      new CopyField(new SchemaField("source", new TextField()),
          new SchemaField("destination", new TextField()), -1000);
    });
    assertTrue(e.getLocalizedMessage().contains("can't have a negative value"));

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
      assertU(adoc("id", "5", "title", "test copy field", "text_en", "this is a simple test of the copy field functionality"));
      assertU(commit());
      
      Map<String,String> args = new HashMap<>();
      args.put( CommonParams.Q, "text_en:simple" );
      args.put( "indent", "true" );
      SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      
      assertQ("Make sure they got in", req
              ,"//*[@numFound='1']"
              ,"//result/doc[1]/str[@name='id'][.='5']"
              );
      
      args = new HashMap<>();
      args.put( CommonParams.Q, "highlight:simple" );
      args.put( "indent", "true" );
      req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      assertQ("dynamic source", req
              ,"//*[@numFound='1']"
              ,"//result/doc[1]/str[@name='id'][.='5']"
              ,"//result/doc[1]/arr[@name='highlight']/str[.='this is a simple test of ']"
              );

      args = new HashMap<>();
      args.put( CommonParams.Q, "text_en:functionality" );
      args.put( "indent", "true" );
      req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      assertQ("Make sure they got in", req
              ,"//*[@numFound='1']");
      
      args = new HashMap<>();
      args.put( CommonParams.Q, "highlight:functionality" );
      args.put( "indent", "true" );
      req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
      assertQ("dynamic source", req
              ,"//*[@numFound='0']");
    }

  @Test
  public void testExplicitSourceGlob()
  {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    
    assertTrue("schema should contain explicit field 'sku1'", schema.getFields().containsKey("sku1"));
    assertTrue("schema should contain explicit field 'sku2'", schema.getFields().containsKey("sku2"));
    assertNull("'sku*' should not be (or match) a dynamic field", schema.getDynamicPattern("sku*"));
    
    assertTrue("schema should contain dynamic field '*_s'", schema.getDynamicPattern("*_s").equals("*_s"));

    final String subsetPattern = "*_dest_sub_s";
    final String dynamicPattern1 = schema.getDynamicPattern(subsetPattern);
    assertTrue("'" + subsetPattern + "' should match dynamic field '*_s', but instead matches '" + dynamicPattern1 + "'",
               dynamicPattern1.equals("*_s"));
    
    final String dest_sub_no_ast_s = "dest_sub_no_ast_s";
    assertFalse(schema.getFields().containsKey(dest_sub_no_ast_s)); // Should not be an explicit field
    final String dynamicPattern2 = schema.getDynamicPattern(dest_sub_no_ast_s);
    assertTrue("'" + dest_sub_no_ast_s + "' should match dynamic field '*_s', but instead matches '" + dynamicPattern2 + "'",
               dynamicPattern2.equals("*_s"));
    
    assertU(adoc("id", "5", "sku1", "10-1839ACX-93", "sku2", "AAM46"));
    assertU(commit());

    Map<String,String> args = new HashMap<>();
    args.put( CommonParams.Q, "text:AAM46" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("sku2 copied to text", req
        ,"//*[@numFound='1']"
        ,"//result/doc[1]/str[@name='id'][.='5']"
    );

    args = new HashMap<>();
    args.put( CommonParams.Q, "1_s:10-1839ACX-93" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("sku1 copied to dynamic dest *_s", req
        ,"//*[@numFound='1']"
        ,"//result/doc[1]/str[@name='id'][.='5']"
        ,"//result/doc[1]/arr[@name='sku1']/str[.='10-1839ACX-93']"
    );

    args = new HashMap<>();
    args.put( CommonParams.Q, "1_dest_sub_s:10-1839ACX-93" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("sku1 copied to *_dest_sub_s (*_s subset pattern)", req
        ,"//*[@numFound='1']");

    args = new HashMap<>();
    args.put( CommonParams.Q, "dest_sub_no_ast_s:AAM46" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("sku2 copied to dest_sub_no_ast_s (*_s subset pattern no asterisk)", req
        ,"//*[@numFound='1']");
  }

  @Test
  public void testSourceGlobMatchesNoDynamicOrExplicitField()
  {
    // SOLR-4650: copyField source globs should not have to match an explicit or dynamic field 
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();

    assertNull("'testing123_*' should not be (or match) a dynamic or explicit field", schema.getFieldOrNull("testing123_*"));

    assertTrue("schema should contain dynamic field '*_s'", schema.getDynamicPattern("*_s").equals("*_s"));

    assertU(adoc("id", "5", "sku1", "10-1839ACX-93", "testing123_s", "AAM46"));
    assertU(commit());

    Map<String,String> args = new HashMap<>();
    args.put( CommonParams.Q, "text:AAM46" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("sku2 copied to text", req
        ,"//*[@numFound='1']"
        ,"//result/doc[1]/str[@name='id'][.='5']"
    );
  }

  public void testCatchAllCopyField() {
    IndexSchema schema = h.getCore().getLatestSchema();

    assertNull("'*' should not be (or match) a dynamic field", 
               schema.getDynamicPattern("*"));
    
    assertU(adoc("id", "5", "sku1", "10-1839ACX-93", "testing123_s", "AAM46"));
    assertU(commit());
    for (String q : new String[] {"5", "10-1839ACX-93", "AAM46" }) {
      assertQ(req("q","catchall_t:" + q)
              ,"//*[@numFound='1']"
              ,"//result/doc[1]/str[@name='id'][.='5']");
    }
  }
}
