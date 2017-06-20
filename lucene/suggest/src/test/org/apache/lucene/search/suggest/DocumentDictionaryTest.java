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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class DocumentDictionaryTest extends LuceneTestCase {
  
  static final String FIELD_NAME = "f1";
  static final String WEIGHT_FIELD_NAME = "w1";
  static final String PAYLOAD_FIELD_NAME = "p1";
  static final String CONTEXT_FIELD_NAME = "c1";
  
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
    Dictionary dictionary = new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME, PAYLOAD_FIELD_NAME);
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
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(atLeast(1000), false);
    Map<String, Document> docs = res.getValue();
    List<String> invalidDocTerms = res.getKey();
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary = new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME, PAYLOAD_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      if (payloadField == null) assertTrue(inputIterator.payload().length == 0);
      else assertEquals(inputIterator.payload(), payloadField.binaryValue());
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
    }
    assertTrue(docs.isEmpty());
    
    IOUtils.close(ir, analyzer, dir);
  }

  @Test
  public void testWithOptionalPayload() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    // Create a document that is missing the payload field
    Document doc = new Document();
    Field field = new TextField(FIELD_NAME, "some field", Field.Store.YES);
    doc.add(field);
    // do not store the payload or the contexts
    Field weight = new NumericDocValuesField(WEIGHT_FIELD_NAME, 100);
    doc.add(weight);
    writer.addDocument(doc);
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);

    // Even though the payload field is missing, the dictionary iterator should not skip the document
    // because the payload field is optional.
    Dictionary dictionaryOptionalPayload =
        new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME, PAYLOAD_FIELD_NAME);
    InputIterator inputIterator = dictionaryOptionalPayload.getEntryIterator();
    BytesRef f = inputIterator.next();
    assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
    IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
    assertEquals(inputIterator.weight(), weightField.numericValue().longValue());
    IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
    assertNull(payloadField);
    assertTrue(inputIterator.payload().length == 0);
    IOUtils.close(ir, analyzer, dir);
  }
 
  @Test
  public void testWithoutPayload() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(atLeast(1000), false);
    Map<String, Document> docs = res.getValue();
    List<String> invalidDocTerms = res.getKey();
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary = new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      assertNull(inputIterator.payload());
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
    }
    
    assertTrue(docs.isEmpty());
    
    IOUtils.close(ir, analyzer, dir);
  }
  
  @Test
  public void testWithContexts() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(atLeast(1000), true);
    Map<String, Document> docs = res.getValue();
    List<String> invalidDocTerms = res.getKey();
    for(Document doc: docs.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary = new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME, PAYLOAD_FIELD_NAME, CONTEXT_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      if (payloadField == null) assertTrue(inputIterator.payload().length == 0);
      else assertEquals(inputIterator.payload(), payloadField.binaryValue());
      Set<BytesRef> oriCtxs = new HashSet<>();
      Set<BytesRef> contextSet = inputIterator.contexts();
      for (IndexableField ctxf : doc.getFields(CONTEXT_FIELD_NAME)) {
        oriCtxs.add(ctxf.binaryValue());
      }
      assertEquals(oriCtxs.size(), contextSet.size());
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
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
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(atLeast(1000), false);
    Map<String, Document> docs = res.getValue();
    List<String> invalidDocTerms = res.getKey();
    Random rand = random();
    List<String> termsToDel = new ArrayList<>();
    for(Document doc : docs.values()) {
      IndexableField f = doc.getField(FIELD_NAME);
      if(rand.nextBoolean() && f != null && !invalidDocTerms.contains(f.stringValue())) {
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
    Dictionary dictionary = new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    while((f = inputIterator.next())!=null) {
      Document doc = docs.remove(f.utf8ToString());
      assertTrue(f.equals(new BytesRef(doc.get(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      assertNull(inputIterator.payload());
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
    }
    assertTrue(docs.isEmpty());
    
    IOUtils.close(ir, analyzer, dir);
  }

  @Test
  public void testMultiValuedField() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(random(), analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    List<Suggestion> suggestions = indexMultiValuedDocuments(atLeast(1000), writer);
    writer.commit();
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary = new DocumentDictionary(ir, FIELD_NAME, WEIGHT_FIELD_NAME, PAYLOAD_FIELD_NAME, CONTEXT_FIELD_NAME);
    InputIterator inputIterator = dictionary.getEntryIterator();
    BytesRef f;
    Iterator<Suggestion> suggestionsIter = suggestions.iterator();
    while((f = inputIterator.next())!=null) {
      Suggestion nextSuggestion = suggestionsIter.next();
      assertTrue(f.equals(nextSuggestion.term));
      long weight = nextSuggestion.weight;
      assertEquals(inputIterator.weight(), (weight != -1) ? weight : 0);
      assertEquals(inputIterator.payload(), nextSuggestion.payload);
      assertTrue(inputIterator.contexts().equals(nextSuggestion.contexts));
    }
    assertFalse(suggestionsIter.hasNext());
    IOUtils.close(ir, analyzer, dir);
  }

  /** Returns Pair(list of invalid document terms, Map of document term -&gt; document) */
  private Map.Entry<List<String>, Map<String, Document>> generateIndexDocuments(int ndocs, boolean requiresContexts) {
    Map<String, Document> docs = new HashMap<>();
    List<String> invalidDocTerms = new ArrayList<>();
    for(int i = 0; i < ndocs ; i++) {
      Document doc = new Document();
      boolean invalidDoc = false;
      Field field = null;
      // usually have valid term field in document
      if (usually()) {
        field = new TextField(FIELD_NAME, "field_" + i, Field.Store.YES);
        doc.add(field);
      } else {
        invalidDoc = true;
      }

      // even if payload is not required usually have it
      if (usually()) {
        Field payload = new StoredField(PAYLOAD_FIELD_NAME, new BytesRef("payload_" + i));
        doc.add(payload);
      }

      if (requiresContexts || usually()) {
        if (usually()) {
          for (int j = 0; j < atLeast(2); j++) {
            doc.add(new StoredField(CONTEXT_FIELD_NAME, new BytesRef("context_" + i + "_"+ j)));
          }
        }
        // we should allow entries without context
      }

      // usually have valid weight field in document
      if (usually()) {
        Field weight = (rarely()) ?
                new StoredField(WEIGHT_FIELD_NAME, 100d + i) :
                new NumericDocValuesField(WEIGHT_FIELD_NAME, 100 + i);
        doc.add(weight);
      }

      String term = null;
      if (invalidDoc) {
        term = (field!=null) ? field.stringValue() : "invalid_" + i;
        invalidDocTerms.add(term);
      } else {
        term = field.stringValue();
      }

      docs.put(term, doc);
    }
    return new SimpleEntry<>(invalidDocTerms, docs);
  }

  private List<Suggestion> indexMultiValuedDocuments(int numDocs, RandomIndexWriter writer) throws IOException {
    List<Suggestion> suggestionList = new ArrayList<>(numDocs);

    for(int i=0; i<numDocs; i++) {
      Document doc = new Document();
      Field field;
      BytesRef payloadValue;
      Set<BytesRef> contextValues = new HashSet<>();
      long numericValue = -1; //-1 for missing weight
      BytesRef term;

      payloadValue = new BytesRef("payload_" + i);
      field = new StoredField(PAYLOAD_FIELD_NAME, payloadValue);
      doc.add(field);

      if (usually()) {
        numericValue = 100 + i;
        field = new NumericDocValuesField(WEIGHT_FIELD_NAME, numericValue);
        doc.add(field);
      }

      int numContexts = atLeast(1);
      for (int j=0; j<numContexts; j++) {
        BytesRef contextValue = new BytesRef("context_" + i + "_" + j);
        field = new StoredField(CONTEXT_FIELD_NAME, contextValue);
        doc.add(field);
        contextValues.add(contextValue);
      }

      int numSuggestions = atLeast(2);
      for (int j=0; j<numSuggestions; j++) {
        term = new BytesRef("field_" + i + "_" + j);
        field = new StoredField(FIELD_NAME, term);
        doc.add(field);

        Suggestion suggestionValue = new Suggestion();
        suggestionValue.payload = payloadValue;
        suggestionValue.contexts = contextValues;
        suggestionValue.weight = numericValue;
        suggestionValue.term = term;
        suggestionList.add(suggestionValue);
      }
      writer.addDocument(doc);
    }
    return suggestionList;
  }

  private static class Suggestion {
    private long weight;
    private BytesRef payload;
    private Set<BytesRef> contexts;
    private BytesRef term;
  }


}
