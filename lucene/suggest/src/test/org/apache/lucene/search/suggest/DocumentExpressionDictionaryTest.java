package org.apache.lucene.search.suggest;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class DocumentExpressionDictionaryTest extends LuceneTestCase {
  
  static final String FIELD_NAME = "f1";
  static final String WEIGHT_FIELD_NAME_1 = "w1";
  static final String WEIGHT_FIELD_NAME_2 = "w2";
  static final String WEIGHT_FIELD_NAME_3 = "w3";
  static final String PAYLOAD_FIELD_NAME = "p1";
  
  private Map<String, Document> generateIndexDocuments(int ndocs) {
    Map<String, Document> docs = new HashMap<>();
    for(int i = 0; i < ndocs ; i++) {
      Field field = new TextField(FIELD_NAME, "field_" + i, Field.Store.YES);
      Field payload = new StoredField(PAYLOAD_FIELD_NAME, new BytesRef("payload_" + i));
      Field weight1 = new NumericDocValuesField(WEIGHT_FIELD_NAME_1, 10 + i);
      Field weight2 = new NumericDocValuesField(WEIGHT_FIELD_NAME_2, 20 + i);
      Field weight3 = new NumericDocValuesField(WEIGHT_FIELD_NAME_3, 30 + i);
      Document doc = new Document();
      doc.add(field);
      doc.add(payload);
      doc.add(weight1);
      doc.add(weight2);
      doc.add(weight3);
      docs.put(field.stringValue(), doc);
    }
    return docs;
  }
  
  @Test
  public void testBasic() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(10);
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Set<SortField> sortFields = new HashSet<SortField>(); 
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_1, SortField.Type.LONG));
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_2, SortField.Type.LONG));
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_3, SortField.Type.LONG));
    Dictionary dictionary = new DocumentExpressionDictionary(ir, FIELD_NAME, "((w1 + w2) - w3)", sortFields, PAYLOAD_FIELD_NAME);
    InputIterator tfp = (InputIterator) dictionary.getWordsIterator();
    BytesRef f;
    while((f = tfp.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      long w3 = doc.getField(WEIGHT_FIELD_NAME_3).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(tfp.weight(), (w1 + w2) - w3);
      assertTrue(tfp.payload().equals(doc.getField(PAYLOAD_FIELD_NAME).binaryValue()));
    }
    assertTrue(docs.isEmpty());
    ir.close();
    dir.close();
  }

  @Test
  public void testWithoutPayload() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(10);
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Set<SortField> sortFields = new HashSet<SortField>(); 
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_1, SortField.Type.LONG));
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_2, SortField.Type.LONG));
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_3, SortField.Type.LONG));
    Dictionary dictionary = new DocumentExpressionDictionary(ir, FIELD_NAME, "w1 + (0.2 * w2) - (w3 - w1)/2", sortFields);
    InputIterator tfp = (InputIterator) dictionary.getWordsIterator();
    BytesRef f;
    while((f = tfp.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      long w3 = doc.getField(WEIGHT_FIELD_NAME_3).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(tfp.weight(), (long)(w1 + (0.2 * w2) - (w3 - w1)/2));
      assertEquals(tfp.payload(), null);
    }
    assertTrue(docs.isEmpty());
    ir.close();
    dir.close();
  }
  
  @Test
  public void testWithDeletions() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map<String, Document> docs = generateIndexDocuments(10);
    Random rand = random();
    List<String> termsToDel = new ArrayList<>();
    for(Document doc : docs.values()) {
      if(rand.nextBoolean()) {
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
    assertEquals(ir.numDocs(), docs.size());
    Set<SortField> sortFields = new HashSet<SortField>(); 
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_1, SortField.Type.LONG));
    sortFields.add(new SortField(WEIGHT_FIELD_NAME_2, SortField.Type.LONG));
    Dictionary dictionary = new DocumentExpressionDictionary(ir, FIELD_NAME, "w2-w1", sortFields, PAYLOAD_FIELD_NAME);
    InputIterator tfp = (InputIterator) dictionary.getWordsIterator();
    BytesRef f;
    while((f = tfp.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      long w1 = doc.getField(WEIGHT_FIELD_NAME_1).numericValue().longValue();
      long w2 = doc.getField(WEIGHT_FIELD_NAME_2).numericValue().longValue();
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      assertEquals(tfp.weight(), w2-w1);
      assertTrue(tfp.payload().equals(doc.getField(PAYLOAD_FIELD_NAME).binaryValue()));
    }
    assertTrue(docs.isEmpty());
    ir.close();
    dir.close();
  }
  
}
