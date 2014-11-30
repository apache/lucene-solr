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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

public class TestLazyDocument extends LuceneTestCase {

  public final int NUM_DOCS = atLeast(10);
  public final String[] FIELDS = new String[] 
    { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k" };
  public final int NUM_VALUES = atLeast(100);

  public Directory dir = newDirectory();
  
  @After
  public void removeIndex() {
    if (null != dir) {
      try { 
        dir.close(); 
        dir = null;
      } catch (Exception e) { /* NOOP */ }
    }
  }

  @Before
  public void createIndex() throws Exception {

    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter
      (dir, newIndexWriterConfig(analyzer));
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(String f : FIELDS) {
      fieldTypes.setMultiValued(f);
    }
    try {
      for (int docid = 0; docid < NUM_DOCS; docid++) {
        Document d = writer.newDocument();
        d.addAtom("docid", ""+docid);
        d.addAtom("never_load", "fail");
        for (String f : FIELDS) {
          for (int val = 0; val < NUM_VALUES; val++) {
            d.addAtom(f, docid+"_"+f+"_"+val);
          }
        }
        d.addAtom("load_later", "yes");
        writer.addDocument(d);
      }
    } finally {
      writer.close();
    }
  }

  public void testLazy() throws Exception {
    final int id = random().nextInt(NUM_DOCS);
    IndexReader reader = DirectoryReader.open(dir);
    try {
      Query q = new TermQuery(new Term("docid", ""+id));
      IndexSearcher searcher = newSearcher(reader);
      ScoreDoc[] hits = searcher.search(q, 100).scoreDocs;
      assertEquals("Too many docs", 1, hits.length);
      LazyTestingStoredFieldVisitor visitor 
        = new LazyTestingStoredFieldVisitor(reader.getFieldTypes(), new LazyDocument(reader, hits[0].doc),
                                            FIELDS);
      reader.document(hits[0].doc, visitor);
      List<FieldValue> d = visitor.doc;

      int numFieldValues = 0;
      Map<String,Integer> fieldValueCounts = new HashMap<>();

      // at this point, all FIELDS should be Lazy and unrealized
      for (FieldValue f : d) {
        numFieldValues++;   
        if (f.name.equals("never_load")) {
          fail("never_load was loaded");
        }
        if (f.name.equals("load_later")) {
          fail("load_later was loaded on first pass");
        }
        if (f.name.equals("docid")) {
          assertFalse(f.name, f.value instanceof LazyDocument.LazyField);
        } else {
          int count = fieldValueCounts.containsKey(f.name) ?
            fieldValueCounts.get(f.name) : 0;
          count++;
          fieldValueCounts.put(f.name, count);
          assertTrue(f.name + " is " + f.value.getClass(),
                     f.value instanceof LazyDocument.LazyField);
          LazyDocument.LazyField lf = (LazyDocument.LazyField) f.value;
          assertFalse(f.name + " is loaded", lf.hasBeenLoaded());
        }
      }
      if (VERBOSE) System.out.println("numFieldValues == " + numFieldValues);
      assertEquals("numFieldValues", 1 + (NUM_VALUES * FIELDS.length), 
                   numFieldValues);
        
      for (String fieldName : fieldValueCounts.keySet()) {
        assertEquals("fieldName count: " + fieldName, 
                     NUM_VALUES, (int)fieldValueCounts.get(fieldName));
      }

      // pick a single field name to load a single value
      final String fieldName = FIELDS[random().nextInt(FIELDS.length)];
      List<FieldValue> fieldValues = new ArrayList<>();
      for(FieldValue f : d) {
        if (f.name.equals(fieldName)) {
          fieldValues.add(f);
        }
      }
      assertEquals("#vals in field: " + fieldName, 
                   NUM_VALUES, fieldValues.size());
      final int valNum = random().nextInt(fieldValues.size());
      assertEquals(id + "_" + fieldName + "_" + valNum,
                   ((LazyDocument.LazyField) fieldValues.get(valNum).value).stringValue());
      
      // now every value of fieldName should be loaded
      for (FieldValue f : d) {
        if (f.name.equals("never_load")) {
          fail("never_load was loaded");
        }
        if (f.name.equals("load_later")) {
          fail("load_later was loaded too soon");
        }
        if (f.name.equals("docid")) {
          assertFalse(f.name, f.value instanceof LazyDocument.LazyField);
        } else {
          assertTrue(f.name + " is " + f.value.getClass(),
                     f.value instanceof LazyDocument.LazyField);
          LazyDocument.LazyField lf = (LazyDocument.LazyField) f.value;
          assertEquals(f.name + " is loaded?", 
                       lf.name().equals(fieldName), lf.hasBeenLoaded());
        }
      }

      // use the same LazyDoc to ask for one more lazy field
      visitor = new LazyTestingStoredFieldVisitor(reader.getFieldTypes(), new LazyDocument(reader, hits[0].doc),
                                                  "load_later");
      reader.document(hits[0].doc, visitor);
      d = visitor.doc;
      
      // ensure we have all the values we expect now, and that
      // adding one more lazy field didn't "unload" the existing LazyField's
      // we already loaded.
      for (FieldValue f : d) {
        if (f.name.equals("never_load")) {
          fail("never_load was loaded");
        }
        if (f.name.equals("docid")) {
          assertFalse(f.name, f.value instanceof LazyDocument.LazyField);
        } else {
          assertTrue(f.name + " is " + f.value.getClass(),
                     f.value instanceof LazyDocument.LazyField);
          LazyDocument.LazyField lf = (LazyDocument.LazyField) f.value;
          assertEquals(f.name + " is loaded?", 
                       lf.name().equals(fieldName), lf.hasBeenLoaded());
        }
      }

      // even the underlying doc shouldn't have never_load
      assertNull("never_load was loaded in wrapped doc",
                 visitor.lazyDoc.getDocument().getField("never_load"));

    } finally {
      reader.close();
    }
  }

  private static class FieldValue {
    String name;
    Object value;
    public FieldValue(String name, Object value) {
      this.name = name;
      this.value = value;
    }
  }

  private static class LazyTestingStoredFieldVisitor extends StoredFieldVisitor {
    public final List<FieldValue> doc = new ArrayList<>();
    public final LazyDocument lazyDoc;
    public final Set<String> lazyFieldNames;

    LazyTestingStoredFieldVisitor(FieldTypes fieldTypes, LazyDocument l, String... fields) {
      lazyDoc = l;
      lazyFieldNames = new HashSet<>(Arrays.asList(fields));
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
      if (fieldInfo.name.equals("docid")) {
        return Status.YES;
      } else if (fieldInfo.name.equals("never_load")) {
        return Status.NO;
      } else {
        if (lazyFieldNames.contains(fieldInfo.name)) {
          doc.add(new FieldValue(fieldInfo.name, lazyDoc.getField(fieldInfo)));
        }
      }
      return Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      doc.add(new FieldValue(fieldInfo.name, value));
    }

  }
}
