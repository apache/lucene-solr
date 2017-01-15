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
package org.apache.solr.core;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;

public class TestCodecSupport extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig_codec.xml", "schema_codec.xml");
  }

  public void testPostingsFormats() {
    Codec codec = h.getCore().getCodec();
    Map<String, SchemaField> fields = h.getCore().getLatestSchema().getFields();
    SchemaField schemaField = fields.get("string_direct_f");
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    assertEquals("Direct", format.getPostingsFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_standard_f");
    assertEquals(TestUtil.getDefaultPostingsFormat().getName(), format.getPostingsFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_f");
    assertEquals(TestUtil.getDefaultPostingsFormat().getName(), format.getPostingsFormatForField(schemaField.getName()).getName());
  }

  public void testDocValuesFormats() {
    Codec codec = h.getCore().getCodec();
    Map<String, SchemaField> fields = h.getCore().getLatestSchema().getFields();
    SchemaField schemaField = fields.get("string_disk_f");
    PerFieldDocValuesFormat format = (PerFieldDocValuesFormat) codec.docValuesFormat();
    assertEquals(TestUtil.getDefaultDocValuesFormat().getName(), format.getDocValuesFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_memory_f");
    assertEquals("Memory",
        format.getDocValuesFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_f");
    assertEquals(TestUtil.getDefaultDocValuesFormat().getName(),
        format.getDocValuesFormatForField(schemaField.getName()).getName());
  }

  public void testDynamicFieldsPostingsFormats() {
    Codec codec = h.getCore().getCodec();
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();

    assertEquals("Direct", format.getPostingsFormatForField("foo_direct").getName());
    assertEquals("Direct", format.getPostingsFormatForField("bar_direct").getName());
    assertEquals(TestUtil.getDefaultPostingsFormat().getName(), format.getPostingsFormatForField("foo_standard").getName());
    assertEquals(TestUtil.getDefaultPostingsFormat().getName(), format.getPostingsFormatForField("bar_standard").getName());
  }

  public void testDynamicFieldsDocValuesFormats() {
    Codec codec = h.getCore().getCodec();
    PerFieldDocValuesFormat format = (PerFieldDocValuesFormat) codec.docValuesFormat();

    assertEquals(TestUtil.getDefaultDocValuesFormat().getName(), format.getDocValuesFormatForField("foo_disk").getName());
    assertEquals(TestUtil.getDefaultDocValuesFormat().getName(), format.getDocValuesFormatForField("bar_disk").getName());
    assertEquals("Memory", format.getDocValuesFormatForField("foo_memory").getName());
    assertEquals("Memory", format.getDocValuesFormatForField("bar_memory").getName());
  }
  
  private void reloadCoreAndRecreateIndex() {
    h.getCoreContainer().reload(h.coreName);
    assertU(delQ("*:*"));
    assertU(commit());
    assertU(add(doc("string_f", "foo")));
    assertU(commit());
  }
  
  private void doTestCompressionMode(String propertyValue, String expectedModeString) throws IOException {
    if (propertyValue != null) {
      System.setProperty("tests.COMPRESSION_MODE", propertyValue);
    }
    try {
      reloadCoreAndRecreateIndex();
      assertCompressionMode(expectedModeString, h.getCore());  
    } finally {
      System.clearProperty("tests.COMPRESSION_MODE");
    }
  }

  protected void assertCompressionMode(String expectedModeString, SolrCore core) throws IOException {
    RefCounted<SolrIndexSearcher> ref = null;
    SolrIndexSearcher searcher = null;
    try {
      ref = core.getSearcher();
      searcher = ref.get();
      SegmentInfos infos = SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());
      SegmentInfo info = infos.info(infos.size() - 1).info;
      assertEquals("Expecting compression mode string to be " + expectedModeString + 
          " but got: " + info.getAttribute(Lucene50StoredFieldsFormat.MODE_KEY) +
          "\n SegmentInfo: " + info +
          "\n SegmentInfos: " + infos + 
          "\n Codec: " + core.getCodec(),
          expectedModeString, info.getAttribute(Lucene50StoredFieldsFormat.MODE_KEY));
    } finally {
      if (ref != null) ref.decref();
    }
  }
  
  public void testCompressionMode() throws Exception {
    assertEquals("incompatible change in compressionMode property", 
        "compressionMode", SchemaCodecFactory.COMPRESSION_MODE);
    doTestCompressionMode("BEST_SPEED", "BEST_SPEED");
    doTestCompressionMode("BEST_COMPRESSION", "BEST_COMPRESSION");
    doTestCompressionMode("best_speed", "BEST_SPEED");
    doTestCompressionMode("best_compression", "BEST_COMPRESSION");
  }
  
  public void testMixedCompressionMode() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
    System.setProperty("tests.COMPRESSION_MODE", "BEST_SPEED");
    h.getCoreContainer().reload(h.coreName);
    assertU(add(doc("string_f", "1", "text", "foo bar")));
    assertU(commit());
    assertCompressionMode("BEST_SPEED", h.getCore());
    System.setProperty("tests.COMPRESSION_MODE", "BEST_COMPRESSION");
    h.getCoreContainer().reload(h.coreName);
    assertU(add(doc("string_f", "2", "text", "foo zar")));
    assertU(commit());
    assertCompressionMode("BEST_COMPRESSION", h.getCore());
    System.setProperty("tests.COMPRESSION_MODE", "BEST_SPEED");
    h.getCoreContainer().reload(h.coreName);
    assertU(add(doc("string_f", "3", "text", "foo zoo")));
    assertU(commit());
    assertCompressionMode("BEST_SPEED", h.getCore());
    assertQ(req("q", "*:*"), 
        "//*[@numFound='3']");
    assertQ(req("q", "text:foo"), 
        "//*[@numFound='3']");
    assertU(optimize());
    assertCompressionMode("BEST_SPEED", h.getCore());
    System.clearProperty("tests.COMPRESSION_MODE");
  }
  
  public void testBadCompressionMode() throws Exception {
    try {
      doTestCompressionMode("something_that_doesnt_exist", "something_that_doesnt_exist");
      fail("Expecting exception");
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, e.code());
      assertTrue("Unexpected Exception message: " + e.getMessage(), 
          e.getMessage().contains("Unable to reload core"));
    }
    
    SchemaCodecFactory factory = new SchemaCodecFactory();
    NamedList<String> nl = new NamedList<>();
    nl.add(SchemaCodecFactory.COMPRESSION_MODE, "something_that_doesnt_exist");
    try {
      factory.init(nl);
      fail("Expecting exception");
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, e.code());
      assertTrue("Unexpected Exception message: " + e.getMessage(), 
          e.getMessage().contains("Invalid compressionMode: 'something_that_doesnt_exist'"));
    }
    
    factory = new SchemaCodecFactory();
    nl = new NamedList<>();
    nl.add(SchemaCodecFactory.COMPRESSION_MODE, "");
    try {
      factory.init(nl);
      fail("Expecting exception");
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, e.code());
      assertTrue("Unexpected Exception message: " + e.getMessage(), 
          e.getMessage().contains("Invalid compressionMode: ''"));
    }
    
  }
  
  public void testCompressionModeDefault() throws IOException {
    assertEquals("Default Solr compression mode changed. Is this expected?", 
        SchemaCodecFactory.SOLR_DEFAULT_COMPRESSION_MODE, Mode.valueOf("BEST_SPEED"));

    String previousCoreName = h.coreName;
    String newCoreName = "core_with_default_compression";
    SolrCore c = null;
    
    SolrConfig config = TestHarness.createConfig(testSolrHome, previousCoreName, "solrconfig_codec2.xml");
    assertEquals("Unexpected codec factory for this test.", "solr.SchemaCodecFactory", config.get("codecFactory/@class"));
    assertNull("Unexpected configuration of codec factory for this test. Expecting empty element", 
        config.getNode("codecFactory", false).getFirstChild());
    IndexSchema schema = IndexSchemaFactory.buildIndexSchema("schema_codec.xml", config);
    
    try {
      c = new SolrCore(new CoreDescriptor(h.getCoreContainer(), newCoreName, testSolrHome.resolve(newCoreName)), 
          new ConfigSet("fakeConfigset", config, schema, null));
      assertNull(h.getCoreContainer().registerCore(newCoreName, c, false, false));
      h.coreName = newCoreName;
      assertEquals("We are not using the correct core", "solrconfig_codec2.xml", h.getCore().getConfigResource());
      assertU(add(doc("string_f", "foo")));
      assertU(commit());
      assertCompressionMode(SchemaCodecFactory.SOLR_DEFAULT_COMPRESSION_MODE.name(), h.getCore());
    } finally {
      h.coreName = previousCoreName;
      h.getCoreContainer().unload(newCoreName);
    }
    
  }
}
