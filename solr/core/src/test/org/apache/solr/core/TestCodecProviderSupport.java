package org.apache.solr.core;

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

import java.util.Map;

import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.perfield.PerFieldPostingsFormat;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

public class TestCodecProviderSupport extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig_codec.xml", "schema_codec.xml");
  }

  public void testPostingsFormats() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();
    Map<String, SchemaField> fields = h.getCore().getSchema().getFields();
    SchemaField schemaField = fields.get("string_pulsing_f");
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codecProvider.getDefaultCodec().postingsFormat();
    assertEquals("Pulsing", format.getPostingsFormatForField(schemaField.getName()));
    schemaField = fields.get("string_simpletext_f");
    assertEquals("SimpleText",
        format.getPostingsFormatForField(schemaField.getName()));
    schemaField = fields.get("string_standard_f");
    assertEquals("Lucene40", format.getPostingsFormatForField(schemaField.getName()));
    schemaField = fields.get("string_f");
    assertEquals("Pulsing", format.getPostingsFormatForField(schemaField.getName()));
  }

  public void testDynamicFields() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codecProvider.getDefaultCodec().postingsFormat();

    assertEquals("SimpleText", format.getPostingsFormatForField("foo_simple"));
    assertEquals("SimpleText", format.getPostingsFormatForField("bar_simple"));
    assertEquals("Pulsing", format.getPostingsFormatForField("foo_pulsing"));
    assertEquals("Pulsing", format.getPostingsFormatForField("bar_pulsing"));
    assertEquals("Lucene40", format.getPostingsFormatForField("foo_standard"));
    assertEquals("Lucene40", format.getPostingsFormatForField("bar_standard"));
  }

  public void testUnknownField() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codecProvider.getDefaultCodec().postingsFormat();
    try {
      format.getPostingsFormatForField("notexisting");
      fail("field is not existing");
    } catch (IllegalArgumentException e) {
      //
    }

  }
}
