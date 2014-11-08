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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSegmentReader extends LuceneTestCase {
  private Directory dir;
  private SegmentReader reader = null;
  private FieldTypes fieldTypes;

  //TODO: Setup the reader w/ multiple documents
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    SegmentCommitInfo info = DocHelper.writeDoc(random(), dir);
    fieldTypes = FieldTypes.getFieldTypes(dir, new MockAnalyzer(random()));
    reader = new SegmentReader(fieldTypes, info, IOContext.READ);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void test() {
    assertTrue(dir != null);
    assertTrue(reader != null);
    assertEquals(DocHelper.getAll(fieldTypes).size(), DocHelper.numFields()+1);
  }
  
  public void testDocument() throws IOException {
    assertTrue(reader.numDocs() == 1);
    assertTrue(reader.maxDoc() >= 1);
    Document2 result = reader.document(0);
    assertTrue(result != null);
    //There are 2 unstored fields on the document that are not preserved across writing
    assertEquals(DocHelper.numFields() - DocHelper.getUnstored(fieldTypes).size() + 1, DocHelper.numFields(result));
    
    List<IndexableField> fields = result.getFields();
    Set<String> allFieldNames = DocHelper.getAll(fieldTypes);
    for (final IndexableField field : fields ) { 
      assertTrue(field != null);
      assertTrue(allFieldNames.contains(field.name()));
    }
  }
  
  public void testGetFieldNameVariations() {
    Collection<String> allFieldNames = new HashSet<>();
    Collection<String> indexedFieldNames = new HashSet<>();
    Collection<String> notIndexedFieldNames = new HashSet<>();
    Collection<String> tvFieldNames = new HashSet<>();
    Collection<String> noTVFieldNames = new HashSet<>();

    for(FieldInfo fieldInfo : reader.getFieldInfos()) {
      final String name = fieldInfo.name;
      allFieldNames.add(name);
      if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
        indexedFieldNames.add(name);
      } else {
        notIndexedFieldNames.add(name);
      }
      if (fieldInfo.hasVectors()) {
        tvFieldNames.add(name);
      } else if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
        noTVFieldNames.add(name);
      }
    }

    assertEquals(allFieldNames, DocHelper.getAll(fieldTypes));
    assertEquals(indexedFieldNames, DocHelper.getIndexed(fieldTypes));
    assertEquals(notIndexedFieldNames, DocHelper.getNotIndexed(fieldTypes));
    assertEquals(tvFieldNames, DocHelper.getTermVectorFields(fieldTypes));
    assertEquals(noTVFieldNames, DocHelper.getNoTermVectorFields(fieldTypes));
  } 
  
  public void testTerms() throws IOException {
    Fields fields = MultiFields.getFields(reader);
    for (String field : fields) {
      if (field.equals(FieldTypes.FIELD_NAMES_FIELD)) {
        continue;
      }
      Terms terms = fields.terms(field);
      assertNotNull(terms);
      TermsEnum termsEnum = terms.iterator(null);
      while(termsEnum.next() != null) {
        BytesRef term = termsEnum.term();
        assertTrue(term != null);
        String fieldValue = (String) DocHelper.nameValues.get(field);
        assertTrue(fieldValue.indexOf(term.utf8ToString()) != -1);
      }
    }
    
    DocsEnum termDocs = TestUtil.docs(random(), reader,
        DocHelper.TEXT_FIELD_1_KEY,
        new BytesRef("field"),
        MultiFields.getLiveDocs(reader),
        null,
        0);
    assertTrue(termDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    termDocs = TestUtil.docs(random(), reader,
        DocHelper.NO_NORMS_KEY,
        new BytesRef(DocHelper.NO_NORMS_TEXT),
        MultiFields.getLiveDocs(reader),
        null,
        0);

    assertTrue(termDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    
    DocsAndPositionsEnum positions = MultiFields.getTermPositionsEnum(reader,
                                                                      MultiFields.getLiveDocs(reader),
                                                                      DocHelper.TEXT_FIELD_1_KEY,
                                                                      new BytesRef("field"));
    // NOTE: prior rev of this test was failing to first
    // call next here:
    assertTrue(positions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(positions.docID() == 0);
    assertTrue(positions.nextPosition() >= 0);
  }    
  
  public void testNorms() throws IOException {
    //TODO: Not sure how these work/should be tested
/*
    try {
      byte [] norms = reader.norms(DocHelper.TEXT_FIELD_1_KEY);
      System.out.println("Norms: " + norms);
      assertTrue(norms != null);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
*/

    checkNorms(fieldTypes, reader);
  }

  public static void checkNorms(FieldTypes fieldTypes, LeafReader reader) throws IOException {
    // test omit norms
    for (String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getIndexOptions(fieldName) != IndexOptions.NONE) {
        assertEquals("field " + fieldName, reader.getNormValues(fieldName) != null, fieldTypes.getNorms(fieldName));
        if (reader.getNormValues(fieldName) == null) {
          // test for norms of null
          NumericDocValues norms = MultiDocValues.getNormValues(reader, fieldName);
          assertNull(norms);
        }
      }
    }
  }
  
  public void testTermVectors() throws IOException {
    Terms result = reader.getTermVectors(0).terms(DocHelper.TEXT_FIELD_2_KEY);
    assertNotNull(result);
    assertEquals(3, result.size());
    TermsEnum termsEnum = result.iterator(null);
    while(termsEnum.next() != null) {
      String term = termsEnum.term().utf8ToString();
      int freq = (int) termsEnum.totalTermFreq();
      assertTrue(DocHelper.FIELD_2_TEXT.indexOf(term) != -1);
      assertTrue(freq > 0);
    }

    Fields results = reader.getTermVectors(0);
    assertTrue(results != null);
    assertEquals("We do not have 3 term freq vectors", 3, results.size());
  }    
  
  public void testOutOfBoundsAccess() throws IOException {
    int numDocs = reader.maxDoc();
    try {
      reader.document(-1);
      fail();
    } catch (IndexOutOfBoundsException expected) {}
    
    try {
      reader.getTermVectors(-1);
      fail();
    } catch (IndexOutOfBoundsException expected) {}
    
    try {
      reader.document(numDocs);
      fail();
    } catch (IndexOutOfBoundsException expected) {}
    
    try {
      reader.getTermVectors(numDocs);
      fail();
    } catch (IndexOutOfBoundsException expected) {}
    
  }
}
