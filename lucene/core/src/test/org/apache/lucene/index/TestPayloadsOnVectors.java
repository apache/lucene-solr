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
package org.apache.lucene.index;


import java.io.StringReader;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestPayloadsOnVectors extends LuceneTestCase {

  /** some docs have payload att, some not */
  public void testMixupDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorPayloads(true);
    customType.setStoreTermVectorOffsets(random().nextBoolean());
    Field field = new Field("field", "", customType);
    TokenStream ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("here we go"));
    field.setTokenStream(ts);
    doc.add(field);
    writer.addDocument(doc);
    
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    writer.addDocument(doc);
    
    ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("another"));
    field.setTokenStream(ts);
    writer.addDocument(doc);
    
    DirectoryReader reader = writer.getReader();
    Terms terms = reader.getTermVector(1, "field");
    assert terms != null;
    TermsEnum termsEnum = terms.iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("withPayload")));
    PostingsEnum de = termsEnum.postings(null, PostingsEnum.ALL);
    assertEquals(0, de.nextDoc());
    assertEquals(0, de.nextPosition());
    assertEquals(new BytesRef("test"), de.getPayload());
    writer.close();
    reader.close();
    dir.close();
  }
  
  /** some field instances have payload att, some not */
  public void testMixupMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorPayloads(true);
    customType.setStoreTermVectorOffsets(random().nextBoolean());
    Field field = new Field("field", "", customType);
    TokenStream ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("here we go"));
    field.setTokenStream(ts);
    doc.add(field);
    Field field2 = new Field("field", "", customType);
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    field2.setTokenStream(ts);
    doc.add(field2);
    Field field3 = new Field("field", "", customType);
    ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("nopayload"));
    field3.setTokenStream(ts);
    doc.add(field3);
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    Terms terms = reader.getTermVector(0, "field");
    assert terms != null;
    TermsEnum termsEnum = terms.iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("withPayload")));
    PostingsEnum de = termsEnum.postings(null, PostingsEnum.ALL);
    assertEquals(0, de.nextDoc());
    assertEquals(3, de.nextPosition());
    assertEquals(new BytesRef("test"), de.getPayload());
    writer.close();
    reader.close();
    dir.close();
  }
  
  public void testPayloadsWithoutPositions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(false);
    customType.setStoreTermVectorPayloads(true);
    customType.setStoreTermVectorOffsets(random().nextBoolean());
    doc.add(new Field("field", "foo", customType));
    try {
      writer.addDocument(doc);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
    writer.close();
    dir.close();
  }

}
