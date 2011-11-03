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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.perfield.PerFieldPostingsFormat;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

public class TestCodecSupport extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema_codec.xml");
  }

  public void testPostingsFormats() {
    Codec codec = h.getCore().getCodec();
    Map<String, SchemaField> fields = h.getCore().getSchema().getFields();
    SchemaField schemaField = fields.get("string_pulsing_f");
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    assertEquals("Pulsing40", format.getPostingsFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_simpletext_f");
    assertEquals("SimpleText",
        format.getPostingsFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_standard_f");
    assertEquals("Lucene40", format.getPostingsFormatForField(schemaField.getName()).getName());
    schemaField = fields.get("string_f");
    assertEquals("Lucene40", format.getPostingsFormatForField(schemaField.getName()).getName());
  }

  public void testDynamicFields() {
    Codec codec = h.getCore().getCodec();
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();

    assertEquals("SimpleText", format.getPostingsFormatForField("foo_simple").getName());
    assertEquals("SimpleText", format.getPostingsFormatForField("bar_simple").getName());
    assertEquals("Pulsing40", format.getPostingsFormatForField("foo_pulsing").getName());
    assertEquals("Pulsing40", format.getPostingsFormatForField("bar_pulsing").getName());
    assertEquals("Lucene40", format.getPostingsFormatForField("foo_standard").getName());
    assertEquals("Lucene40", format.getPostingsFormatForField("bar_standard").getName());
  }

  public void testUnknownField() {
    Codec codec = h.getCore().getCodec();
    PerFieldPostingsFormat format = (PerFieldPostingsFormat) codec.postingsFormat();
    try {
      format.getPostingsFormatForField("notexisting");
      fail("field is not existing");
    } catch (IllegalArgumentException e) {
      //
    }

  }
}
