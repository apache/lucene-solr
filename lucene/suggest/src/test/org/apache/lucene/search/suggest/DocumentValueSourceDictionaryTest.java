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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.SumFloatFunction;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class DocumentValueSourceDictionaryTest extends LuceneTestCase {
  
  static final String FIELD_NAME = "f1";
  static final String WEIGHT_FIELD_NAME_1 = "w1";
  static final String WEIGHT_FIELD_NAME_2 = "w2";
  static final String WEIGHT_FIELD_NAME_3 = "w3";
  static final String PAYLOAD_FIELD_NAME = "p1";
  static final String CONTEXTS_FIELD_NAME = "c1";
  
  @Test
  public void testEmptyReader() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    // Make sure the index is created?
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary = new DocumentValueSourceDictionary(ir, FIELD_NAME,  new DoubleConstValueSource(10), PAYLOAD_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();

    assertNull(inputIterator.next());
    assertEquals(inputIterator.weight(), 0);
    assertNull(inputIterator.payload());

    IOUtils.close(ir, analyzer, dir);
  }
  
  @Test
  public void testBasic() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(atLeast(100));
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    ValueSource[] toAdd = new ValueSource[] {new LongFieldSource(WEIGHT_FIELD_NAME_1), new LongFieldSource(WEIGHT_FIELD_NAME_2), new LongFieldSource(WEIGHT_FIELD_NAME_3)};
    Dictionary dictionary = new DocumentValueSourceDictionary(ir, FIELD_NAME, new SumFloatFunction(toAdd), PAYLOAD_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      long w3 = doc.getField(WEIGHT_FIELD_NAME_3).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(inputIterator.weight(), (w1 + w2 + w3));
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      if (payloadField == null) assertTrue(inputIterator.payload().length == 0);
      else assertEquals(inputIterator.payload(), payloadField.binaryValue());
    }
    assertTrue(docs.isEmpty());
    IOUtils.close(ir, analyzer, dir);
  }
  
  @Test
  public void testWithContext() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(atLeast(100));
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    ValueSource[] toAdd = new ValueSource[] {new LongFieldSource(WEIGHT_FIELD_NAME_1), new LongFieldSource(WEIGHT_FIELD_NAME_2), new LongFieldSource(WEIGHT_FIELD_NAME_3)};
    Dictionary dictionary = new DocumentValueSourceDictionary(ir, FIELD_NAME, new SumFloatFunction(toAdd), PAYLOAD_FIELD_NAME, CONTEXTS_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      long w3 = doc.getField(WEIGHT_FIELD_NAME_3).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(inputIterator.weight(), (w1 + w2 + w3));
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      if (payloadField == null) assertTrue(inputIterator.payload().length == 0);
      else assertEquals(inputIterator.payload(), payloadField.binaryValue());
      Set<BytesRef> originalCtxs = new HashSet<>();
      for (IndexableField ctxf: doc.getFields(CONTEXTS_FIELD_NAME)) {
        originalCtxs.add(ctxf.binaryValue());
      }
      assertEquals(originalCtxs, inputIterator.contexts());
    }
    assertTrue(docs.isEmpty());
    IOUtils.close(ir, analyzer, dir);
  }

  @Test
  public void testWithoutPayload() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(atLeast(100));
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    ValueSource[] toAdd = new ValueSource[] {new LongFieldSource(WEIGHT_FIELD_NAME_1), new LongFieldSource(WEIGHT_FIELD_NAME_2), new LongFieldSource(WEIGHT_FIELD_NAME_3)};
    Dictionary dictionary = new DocumentValueSourceDictionary(ir, FIELD_NAME,  new SumFloatFunction(toAdd));
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      long w3 = doc.getField(WEIGHT_FIELD_NAME_3).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(inputIterator.weight(), (w1 + w2 + w3));
      assertNull(inputIterator.payload());
    }
    assertTrue(docs.isEmpty());
    IOUtils.close(ir, analyzer, dir);
  }
  
  @Test
  public void testWithDeletions() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(atLeast(100));
    Random rand = random();
    List<String> termsToDel = new ArrayList<>();
    for(Document doc : docs.values()) {
      if(rand.nextBoolean() && termsToDel.size() < docs.size()-1) {
        termsToDel.add(doc.get(FIELD_NAME));
      }
      writer.addDocument(doc);
    }
    writer.commit();
    
    Term[] delTerms = new Term[termsToDel.size()];
    for(int i=0; i < termsToDel.size() ; i++) {
      delTerms[i] = new Term(FIELD_NAME, termsToDel.get(i));
    }
    
    for(Term delTerm: delTerms) {
      writer.deleteDocuments(delTerm);  
    }
    writer.commit();
    writer.close();
    
    for(String termToDel: termsToDel) {
      assertTrue(null!=docs.remove(termToDel));
    }
    
    IndexReader ir = DirectoryReader.open(dir);
    assertTrue("NumDocs should be > 0 but was " + ir.numDocs(), ir.numDocs() > 0);
    assertEquals(ir.numDocs(), docs.size());
    ValueSource[] toAdd = new ValueSource[] {new LongFieldSource(WEIGHT_FIELD_NAME_1), new LongFieldSource(WEIGHT_FIELD_NAME_2)};

    Dictionary dictionary = new DocumentValueSourceDictionary(ir, FIELD_NAME,  new SumFloatFunction(toAdd), PAYLOAD_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(inputIterator.weight(), w2+w1);
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      if (payloadField == null) assertTrue(inputIterator.payload().length == 0);
      else assertEquals(inputIterator.payload(), payloadField.binaryValue());
    }
    assertTrue(docs.isEmpty());
    IOUtils.close(ir, analyzer, dir);
  }
  
  @Test
  public void testWithValueSource() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(atLeast(100));
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary = new DocumentValueSourceDictionary(ir, FIELD_NAME, new DoubleConstValueSource(10), PAYLOAD_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(inputIterator.weight(), 10);
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      if (payloadField == null) assertTrue(inputIterator.payload().length == 0);
      else assertEquals(inputIterator.payload(), payloadField.binaryValue());
    }
    assertTrue(docs.isEmpty());
    IOUtils.close(ir, analyzer, dir);
  }

  private Map<String, Document> generateIndexDocuments(int ndocs) {
    Map<String, Document> docs = new HashMap<>();
    for(int i = 0; i < ndocs ; i++) {
      Field field = new TextField(FIELD_NAME, "field_" + i, Field.Store.YES);
      Field weight1 = new NumericDocValuesField(WEIGHT_FIELD_NAME_1, 10 + i);
      Field weight2 = new NumericDocValuesField(WEIGHT_FIELD_NAME_2, 20 + i);
      Field weight3 = new NumericDocValuesField(WEIGHT_FIELD_NAME_3, 30 + i);
      Field contexts = new StoredField(CONTEXTS_FIELD_NAME, new BytesRef("ctx_"  + i + "_0"));
      Document doc = new Document();
      doc.add(field);
      // even if payload is not required usually have it
      if (usually()) {
        Field payload = new StoredField(PAYLOAD_FIELD_NAME, new BytesRef("payload_" + i));
        doc.add(payload);
      }
      doc.add(weight1);
      doc.add(weight2);
      doc.add(weight3);
      doc.add(contexts);
      for(int j = 1; j < atLeast(3); j++) {
        contexts.setBytesValue(new BytesRef("ctx_" + i + "_" + j));
        doc.add(contexts);
      }
      docs.put(field.stringValue(), doc);
    }
    return docs;
  }
}
