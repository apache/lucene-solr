package org.apache.lucene.index;

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

import java.io.StringReader;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
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
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.enableTermVectorPayloads("field");
    if (random().nextBoolean()) {
      fieldTypes.enableTermVectorOffsets("field");
    }

    Document doc = writer.newDocument();
    TokenStream ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("here we go"));
    doc.addLargeText("field", ts);
    writer.addDocument(doc);
    
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    doc = writer.newDocument();
    doc.addLargeText("field", ts);
    writer.addDocument(doc);
    
    ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("another"));
    doc = writer.newDocument();
    doc.addLargeText("field", ts);
    writer.addDocument(doc);
    
    DirectoryReader reader = writer.getReader();
    Terms terms = reader.getTermVector(1, "field");
    assert terms != null;
    TermsEnum termsEnum = terms.iterator(null);
    assertTrue(termsEnum.seekExact(new BytesRef("withPayload")));
    DocsAndPositionsEnum de = termsEnum.docsAndPositions(null, null);
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

    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.enableTermVectorPayloads("field");
    if (random().nextBoolean()) {
      fieldTypes.enableTermVectorOffsets("field");
    }
    fieldTypes.setMultiValued("field");
    TokenStream ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("here we go"));
    Document doc = writer.newDocument();
    doc.addLargeText("field", ts);

    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    doc.addLargeText("field", ts);

    ts = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    ((Tokenizer)ts).setReader(new StringReader("nopayload"));
    doc.addLargeText("field", ts);
    writer.addDocument(doc);

    DirectoryReader reader = writer.getReader();
    Terms terms = reader.getTermVector(0, "field");
    assert terms != null;
    TermsEnum termsEnum = terms.iterator(null);
    assertTrue(termsEnum.seekExact(new BytesRef("withPayload")));
    DocsAndPositionsEnum de = termsEnum.docsAndPositions(null, null);
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
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    try {
      fieldTypes.enableTermVectorPayloads("field");
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    writer.close();
    dir.close();
  }

}
