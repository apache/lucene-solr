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
package org.apache.solr.common.util;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

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
    doc.addField("f3", new Integer(123));
    doc.addField("f4", "12345678901234567890");
    doc.addField("f4", "12345678901234567890");
    allFields = new String[] {"f1", "f2", "f3"};
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
    assertEquals(9, reader.read(chars, 0, 9));
    assertArrEqu("a b c - m", chars);
    reader.read(chars, 9, 2);
    assertArrEqu("a b c - mul", chars);
    reader.read(chars, 11, 1);
    assertArrEqu("a b c - mult", chars);
    reader.read(chars, 12, 10);
    // We now hit totalMaxChars
    assertArrEqu("a b c - multi - valu", chars);
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
    int num = reader.read(chars, 0, 22);
    assertEquals(21, num);
    assertArrEqu("a  - mu - va - fi - 1", chars);
  }
  
  private void assertArrEqu(String expected, char[] chars) {
    String str = new String(chars, 0, expected.length());
    assertEquals(expected, str);
  }

}