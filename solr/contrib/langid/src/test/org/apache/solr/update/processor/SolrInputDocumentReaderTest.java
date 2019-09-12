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
package org.apache.solr.update.processor;

import java.util.Arrays;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SolrInputDocumentReaderTest {
  private SolrInputDocument doc;
  private String[] allFields;

  @Before
  public void setUp() throws Exception {
    doc = new SolrInputDocument();
    doc.addField("f1", "a b c");
    doc.addField("f2", "multi");
    doc.addField("f2", "valued");
    doc.addField("f2", "field");
    doc.addField("f3", 123);
    doc.addField("f4", "12345678901234567890");
    allFields = new String[] {"f1", "f2", "f3", "f4"};
  }
  
  @Test
  public void readChunked() throws Exception {
    SolrInputDocumentReader reader = new SolrInputDocumentReader(
        doc,
        allFields,
        20,
        18,
        " - ");
    assertTrue(reader.ready());
    char[] chars = new char[1000];
    int len;
    assertEquals(9, len=reader.read(chars, 0, 9));
    assertArrEqu("a b c - m", chars, len);
    len += reader.read(chars, 9, 2);
    assertArrEqu("a b c - mul", chars, len);
    len += reader.read(chars, 11, 1);
    assertArrEqu("a b c - mult", chars, len);
    len += reader.read(chars, 12, 10);
    // We now hit totalMaxChars
    assertArrEqu("a b c - multi - valu", chars, len);
  }

  @Test
  public void maxFieldValueLength() throws Exception {
    SolrInputDocumentReader reader = new SolrInputDocumentReader(
        doc,
        allFields,
        21,
        2,
        " - "
    );
    assertTrue(reader.ready());
    char[] chars = new char[1000];
    int len = reader.read(chars, 0, 22);
    assertEquals(21, len);
    assertArrEqu("a  - mu - va - fi - 1", chars, len);
  }

  @Test
  public void allStrFields() throws Exception {
    SolrInputDocumentReader reader = new SolrInputDocumentReader(
        doc,
        20000,
        10000
    );
    assertTrue(reader.ready());
    char[] chars = new char[1000];
    int len = reader.read(chars, 0, 1000);
    assertEquals(45, len);
    assertArrEqu("a b c multi valued field 12345678901234567890", chars, len);
  }
  
  @Test
  public void testGetStringFields() throws Exception {
    String[] expected = new String[] {"f1", "f2", "f4"};
    assertArrayEquals(expected, SolrInputDocumentReader.getStringFields(doc));
  }

  private void assertArrEqu(String expected, char[] chars, int len) {
    String str = new String(Arrays.copyOf(chars, len));
    assertEquals(expected, str);
  }

}