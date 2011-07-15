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
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

public class TestCodecProviderSupport extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig_codec.xml", "schema_codec.xml");
  }

  public void testCodecs() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();
    Map<String, SchemaField> fields = h.getCore().getSchema().getFields();
    SchemaField schemaField = fields.get("string_pulsing_f");
    assertEquals("Pulsing", codecProvider.getFieldCodec(schemaField.getName()));
    schemaField = fields.get("string_simpletext_f");
    assertEquals("SimpleText",
        codecProvider.getFieldCodec(schemaField.getName()));
    schemaField = fields.get("string_standard_f");
    assertEquals("Standard", codecProvider.getFieldCodec(schemaField.getName()));
    schemaField = fields.get("string_f");
    assertEquals("Pulsing", codecProvider.getFieldCodec(schemaField.getName()));

    assertTrue(codecProvider.hasFieldCodec("string_simpletext_f"));
    assertTrue(codecProvider.hasFieldCodec("string_standard_f"));
    assertTrue(codecProvider.hasFieldCodec("string_f"));
  }

  public void testDynamicFields() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();

    assertTrue(codecProvider.hasFieldCodec("bar_simple"));
    assertTrue(codecProvider.hasFieldCodec("bar_pulsing"));
    assertTrue(codecProvider.hasFieldCodec("bar_standard"));

    assertEquals("SimpleText", codecProvider.getFieldCodec("foo_simple"));
    assertEquals("Pulsing", codecProvider.getFieldCodec("foo_pulsing"));
    assertEquals("Standard", codecProvider.getFieldCodec("foo_standard"));
  }

  public void testUnmodifiable() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();
    try {
      codecProvider.setDefaultFieldCodec("foo");
      fail("should be unmodifiable");
    } catch (UnsupportedOperationException e) {
      //
    }

    try {
      codecProvider.setFieldCodec("foo", "bar");
      fail("should be unmodifiable");
    } catch (UnsupportedOperationException e) {
      //
    }

    try {
      codecProvider.register(new StandardCodec());
      fail("should be unmodifiable");
    } catch (UnsupportedOperationException e) {
      //
    }

    try {
      codecProvider.unregister(new StandardCodec());
      fail("should be unmodifiable");
    } catch (UnsupportedOperationException e) {
      //
    }
  }

  public void testUnknownField() {
    CodecProvider codecProvider = h.getCore().getCodecProvider();
    try {
      codecProvider.getFieldCodec("notexisting");
      fail("field is not existing");
    } catch (IllegalArgumentException e) {
      //
    }

  }
}
