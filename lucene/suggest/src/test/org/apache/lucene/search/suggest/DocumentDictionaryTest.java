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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

public class DocumentDictionaryTest extends LuceneTestCase {
  
  static final String FIELD_NAME = "f1";
  static final String WEIGHT_FIELD_NAME = "w1";
  static final String PAYLOAD_FIELD_NAME = "p1";
  static final String CONTEXT_FIELD_NAME = "c1";
  
  /** Returns Pair(list of invalid document terms, Map of document term -&gt; document) */
  private Map.Entry<List<String>, Map<String, Document>> generateIndexDocuments(RandomIndexWriter writer, int ndocs, boolean requiresPayload, boolean requiresContexts) {
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued(CONTEXT_FIELD_NAME);

    Map<String, Document> docs = new HashMap<>();
    List<String> invalidDocTerms = new ArrayList<>();
    boolean useStoredFieldWeights = rarely();
    for(int i = 0; i < ndocs ; i++) {
      Document doc = writer.newDocument();
      boolean invalidDoc = false;
      // usually have valid term field in document
      String term;
      if (usually()) {
        term = "field_" + i;
        doc.addLargeText(FIELD_NAME, term);
      } else {
        term = "invalid_" + i;
        invalidDoc = true;
      }
      
      // even if payload is not required usually have it
      if (requiresPayload || usually()) {
        // usually have valid payload field in document
        if (usually()) {
          doc.addStored(PAYLOAD_FIELD_NAME, new BytesRef("payload_" + i));
        } else if (requiresPayload) {
          invalidDoc = true;
        }
      }
      
      if (requiresContexts || usually()) {
        if (usually()) {
          for (int j = 0; j < atLeast(2); j++) {
            doc.addStored(CONTEXT_FIELD_NAME, new BytesRef("context_" + i + "_"+ j));
          }
        }
        // we should allow entries without context
      }
      
      // usually have valid weight field in document
      if (usually()) {
        if (useStoredFieldWeights) {
          doc.addStoredDouble(WEIGHT_FIELD_NAME, 100d + i);
        } else {
          doc.addLong(WEIGHT_FIELD_NAME, 100 + i);
        }
      }
      
      if (invalidDoc) {
        invalidDocTerms.add(term);
      }
      
      docs.put(term, doc);
    }
    return new SimpleEntry<>(invalidDocTerms, docs);
  }
  
  @Test
  public void testEmptyReader() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
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
    
    ir.close();
    dir.close();
  }
  
  @Test
  public void testBasic() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(writer, atLeast(1000), true, false);
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
      assertTrue(f.equals(new BytesRef(doc.getString(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      assertTrue(inputIterator.payload().equals(doc.getField(PAYLOAD_FIELD_NAME).binaryValue()));
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
    }
    assertTrue(docs.isEmpty());
    
    ir.close();
    dir.close();
  }
 
  @Test
  public void testWithoutPayload() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(writer, atLeast(1000), false, false);
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
      assertTrue(f.equals(new BytesRef(doc.getString(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      assertEquals(inputIterator.payload(), null);
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
    }
    
    assertTrue(docs.isEmpty());
    
    ir.close();
    dir.close();
  }
  
  @Test
  public void testWithContexts() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(writer, atLeast(1000), true, true);
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
      assertTrue(f.equals(new BytesRef(doc.getString(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      assertTrue(inputIterator.payload().equals(doc.getField(PAYLOAD_FIELD_NAME).binaryValue()));
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
    
    ir.close();
    dir.close();
  }
  
  @Test
  public void testWithDeletions() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Map.Entry<List<String>, Map<String, Document>> res = generateIndexDocuments(writer, atLeast(1000), false, false);
    Map<String, Document> docs = res.getValue();
    List<String> invalidDocTerms = res.getKey();
    Random rand = random();
    List<String> termsToDel = new ArrayList<>();
    for(Document doc : docs.values()) {
      IndexableField f = doc.getField(FIELD_NAME);
      if(rand.nextBoolean() && f != null && !invalidDocTerms.contains(f.stringValue())) {
        termsToDel.add(doc.getString(FIELD_NAME));
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
      assertTrue(f.equals(new BytesRef(doc.getString(FIELD_NAME))));
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(inputIterator.weight(), (weightField != null) ? weightField.numericValue().longValue() : 0);
      assertEquals(inputIterator.payload(), null);
    }
    
    for (String invalidTerm : invalidDocTerms) {
      assertNotNull(docs.remove(invalidTerm));
    }
    assertTrue(docs.isEmpty());
    
    ir.close();
    dir.close();
  }

  @Test
  public void testMultiValuedField() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(random(), new MockAnalyzer(random()));
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
      assertTrue(inputIterator.payload().equals(nextSuggestion.payload));
      assertTrue(inputIterator.contexts().equals(nextSuggestion.contexts));
    }
    assertFalse(suggestionsIter.hasNext());
    ir.close();
    dir.close();
  }

  private List<Suggestion> indexMultiValuedDocuments(int numDocs, RandomIndexWriter writer) throws IOException {
    List<Suggestion> suggestionList = new ArrayList<>(numDocs);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued(FIELD_NAME);

    for(int i=0; i<numDocs; i++) {
      Document doc = writer.newDocument();
      BytesRef payloadValue;
      Set<BytesRef> contextValues = new HashSet<>();
      long numericValue = -1; //-1 for missing weight
      BytesRef term;

      payloadValue = new BytesRef("payload_" + i);
      doc.addStored(PAYLOAD_FIELD_NAME, payloadValue);

      if (usually()) {
        numericValue = 100 + i;
        doc.addLong(WEIGHT_FIELD_NAME, numericValue);
      }

      int numContexts = atLeast(1);
      for (int j=0; j<numContexts; j++) {
        BytesRef contextValue = new BytesRef("context_" + i + "_" + j);
        doc.addStored(CONTEXT_FIELD_NAME, contextValue);
        contextValues.add(contextValue);
      }

      int numSuggestions = atLeast(2);
      for (int j=0; j<numSuggestions; j++) {
        term = new BytesRef("field_" + i + "_" + j);
        doc.addStored(FIELD_NAME, term);

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

  private class Suggestion {
    private long weight;
    private BytesRef payload;
    private Set<BytesRef> contexts;
    private BytesRef term;
  }


}
