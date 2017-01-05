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
package org.apache.lucene.document;


import java.nio.charset.StandardCharsets;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests {@link Document} class.
 */
public class TestBinaryDocument extends LuceneTestCase {

  String binaryValStored = "this text will be stored as a byte array in the index";
  String binaryValCompressed = "this text will be also stored and compressed as a byte array in the index";
  
  public void testBinaryFieldInIndex()
    throws Exception
  {
    FieldType ft = new FieldType();
    ft.setStored(true);
    StoredField binaryFldStored = new StoredField("binaryStored", binaryValStored.getBytes(StandardCharsets.UTF_8));
    Field stringFldStored = new Field("stringStored", binaryValStored, ft);

    Document doc = new Document();
    
    doc.add(binaryFldStored);
    
    doc.add(stringFldStored);

    /** test for field count */
    assertEquals(2, doc.getFields().size());
    
    /** add the doc to a ram index */
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(doc);
    
    /** open a reader and fetch the document */ 
    IndexReader reader = writer.getReader();
    Document docFromReader = reader.document(0);
    assertTrue(docFromReader != null);
    
    /** fetch the binary stored field and compare its content with the original one */
    BytesRef bytes = docFromReader.getBinaryValue("binaryStored");
    assertNotNull(bytes);
    String binaryFldStoredTest = new String(bytes.bytes, bytes.offset, bytes.length, StandardCharsets.UTF_8);
    assertTrue(binaryFldStoredTest.equals(binaryValStored));
    
    /** fetch the string field and compare its content with the original one */
    String stringFldStoredTest = docFromReader.get("stringStored");
    assertTrue(stringFldStoredTest.equals(binaryValStored));
    
    writer.close();
    reader.close();
    dir.close();
  }
}
