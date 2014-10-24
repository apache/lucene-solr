package org.apache.solr.core;

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

import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.SchemaField;
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
    schemaField = fields.get("string_simpletext_f");
    assertEquals("SimpleText",
        format.getPostingsFormatForField(schemaField.getName()).getName());
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

    assertEquals("SimpleText", format.getPostingsFormatForField("foo_simple").getName());
    assertEquals("SimpleText", format.getPostingsFormatForField("bar_simple").getName());
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
}
