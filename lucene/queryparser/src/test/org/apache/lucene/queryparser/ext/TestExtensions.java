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
package org.apache.lucene.queryparser.ext;

import org.apache.lucene.util.LuceneTestCase;

/**
 * Testcase for the {@link Extensions} class
 */
public class TestExtensions extends LuceneTestCase {

  private Extensions ext;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.ext = new Extensions();
  }

  public void testBuildExtensionField() {
    assertEquals("field\\:key", ext.buildExtensionField("key", "field"));
    assertEquals("\\:key", ext.buildExtensionField("key"));

    ext = new Extensions('.');
    assertEquals("field.key", ext.buildExtensionField("key", "field"));
    assertEquals(".key", ext.buildExtensionField("key"));
  }

  public void testSplitExtensionField() {
    assertEquals("field\\:key", ext.buildExtensionField("key", "field"));
    assertEquals("\\:key", ext.buildExtensionField("key"));

    ext = new Extensions('.');
    assertEquals("field.key", ext.buildExtensionField("key", "field"));
    assertEquals(".key", ext.buildExtensionField("key"));
  }

  public void testAddGetExtension() {
    ParserExtension extension = new ExtensionStub();
    assertNull(ext.getExtension("foo"));
    ext.add("foo", extension);
    assertSame(extension, ext.getExtension("foo"));
    ext.add("foo", null);
    assertNull(ext.getExtension("foo"));
  }

  public void testGetExtDelimiter() {
    assertEquals(Extensions.DEFAULT_EXTENSION_FIELD_DELIMITER, this.ext
        .getExtensionFieldDelimiter());
    ext = new Extensions('?');
    assertEquals('?', this.ext.getExtensionFieldDelimiter());
  }

  public void testEscapeExtension() {
    assertEquals("abc\\:\\?\\{\\}\\[\\]\\\\\\(\\)\\+\\-\\!\\~", ext
        .escapeExtensionField("abc:?{}[]\\()+-!~"));
    try {
      ext.escapeExtensionField(null);
      fail("should throw NPE - escape string is null");
    } catch (NullPointerException e) {
      // 
    }
  }
}
