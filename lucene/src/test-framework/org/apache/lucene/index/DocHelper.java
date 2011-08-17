package org.apache.lucene.index;

/**
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
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.util.LuceneTestCase.TEST_VERSION_CURRENT;

class DocHelper {
  public static final String FIELD_1_TEXT = "field one text";
  public static final String TEXT_FIELD_1_KEY = "textField1";
  public static Field textField1 = new Field(TEXT_FIELD_1_KEY, FIELD_1_TEXT,
      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO);
  
  public static final String FIELD_2_TEXT = "field field field two text";
  //Fields will be lexicographically sorted.  So, the order is: field, text, two
  public static final int [] FIELD_2_FREQS = {3, 1, 1}; 
  public static final String TEXT_FIELD_2_KEY = "textField2";
  public static Field textField2 = new Field(TEXT_FIELD_2_KEY, FIELD_2_TEXT, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
  
  public static final String FIELD_3_TEXT = "aaaNoNorms aaaNoNorms bbbNoNorms";
  public static final String TEXT_FIELD_3_KEY = "textField3";
  public static Field textField3 = new Field(TEXT_FIELD_3_KEY, FIELD_3_TEXT, Field.Store.YES, Field.Index.ANALYZED);
  static { textField3.setOmitNorms(true); }

  public static final String KEYWORD_TEXT = "Keyword";
  public static final String KEYWORD_FIELD_KEY = "keyField";
  public static Field keyField = new Field(KEYWORD_FIELD_KEY, KEYWORD_TEXT,
      Field.Store.YES, Field.Index.NOT_ANALYZED);

  public static final String NO_NORMS_TEXT = "omitNormsText";
  public static final String NO_NORMS_KEY = "omitNorms";
  public static Field noNormsField = new Field(NO_NORMS_KEY, NO_NORMS_TEXT,
      Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);

  public static final String NO_TF_TEXT = "analyzed with no tf and positions";
  public static final String NO_TF_KEY = "omitTermFreqAndPositions";
  public static Field noTFField = new Field(NO_TF_KEY, NO_TF_TEXT,
      Field.Store.YES, Field.Index.ANALYZED);
  static {
    noTFField.setIndexOptions(IndexOptions.DOCS_ONLY);
  }

  public static final String UNINDEXED_FIELD_TEXT = "unindexed field text";
  public static final String UNINDEXED_FIELD_KEY = "unIndField";
  public static Field unIndField = new Field(UNINDEXED_FIELD_KEY, UNINDEXED_FIELD_TEXT,
      Field.Store.YES, Field.Index.NO);


  public static final String UNSTORED_1_FIELD_TEXT = "unstored field text";
  public static final String UNSTORED_FIELD_1_KEY = "unStoredField1";
  public static Field unStoredField1 = new Field(UNSTORED_FIELD_1_KEY, UNSTORED_1_FIELD_TEXT,
      Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);

  public static final String UNSTORED_2_FIELD_TEXT = "unstored field text";
  public static final String UNSTORED_FIELD_2_KEY = "unStoredField2";
  public static Field unStoredField2 = new Field(UNSTORED_FIELD_2_KEY, UNSTORED_2_FIELD_TEXT,
      Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES);

  public static final String LAZY_FIELD_BINARY_KEY = "lazyFieldBinary";
  public static byte [] LAZY_FIELD_BINARY_BYTES;
  public static Field lazyFieldBinary;
  
  public static final String LAZY_FIELD_KEY = "lazyField";
  public static final String LAZY_FIELD_TEXT = "These are some field bytes";
  public static Field lazyField = new Field(LAZY_FIELD_KEY, LAZY_FIELD_TEXT, Field.Store.YES, Field.Index.ANALYZED);
  
  public static final String LARGE_LAZY_FIELD_KEY = "largeLazyField";
  public static String LARGE_LAZY_FIELD_TEXT;
  public static Field largeLazyField;
  
  //From Issue 509
  public static final String FIELD_UTF1_TEXT = "field one \u4e00text";
  public static final String TEXT_FIELD_UTF1_KEY = "textField1Utf8";
  public static Field textUtfField1 = new Field(TEXT_FIELD_UTF1_KEY, FIELD_UTF1_TEXT,
      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO);

  public static final String FIELD_UTF2_TEXT = "field field field \u4e00two text";
  //Fields will be lexicographically sorted.  So, the order is: field, text, two
  public static final int [] FIELD_UTF2_FREQS = {3, 1, 1};
  public static final String TEXT_FIELD_UTF2_KEY = "textField2Utf8";
  public static Field textUtfField2 = new Field(TEXT_FIELD_UTF2_KEY, FIELD_UTF2_TEXT, Field.Store.YES, 
          Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
 
  
  
  
  public static Map<String,Object> nameValues = null;

  // ordered list of all the fields...
  // could use LinkedHashMap for this purpose if Java1.4 is OK
  public static Field[] fields = new Field[] {
    textField1,
    textField2,
    textField3,
    keyField,
    noNormsField,
    noTFField,
    unIndField,
    unStoredField1,
    unStoredField2,
    textUtfField1,
    textUtfField2,
    lazyField,
    lazyFieldBinary,//placeholder for binary field, since this is null.  It must be second to last.
    largeLazyField//placeholder for large field, since this is null.  It must always be last
  };

  public static Map<String,Fieldable> all     =new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> indexed =new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> stored  =new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> unstored=new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> unindexed=new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> termvector=new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> notermvector=new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> lazy= new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> noNorms=new HashMap<String,Fieldable>();
  public static Map<String,Fieldable> noTf=new HashMap<String,Fieldable>();

  static {
    //Initialize the large Lazy Field
    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < 10000; i++)
    {
      buffer.append("Lazily loading lengths of language in lieu of laughing ");
    }
    
    try {
      LAZY_FIELD_BINARY_BYTES = "These are some binary field bytes".getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
    }
    lazyFieldBinary = new Field(LAZY_FIELD_BINARY_KEY, LAZY_FIELD_BINARY_BYTES);
    fields[fields.length - 2] = lazyFieldBinary;
    LARGE_LAZY_FIELD_TEXT = buffer.toString();
    largeLazyField = new Field(LARGE_LAZY_FIELD_KEY, LARGE_LAZY_FIELD_TEXT, Field.Store.YES, Field.Index.ANALYZED);
    fields[fields.length - 1] = largeLazyField;
    for (int i=0; i<fields.length; i++) {
      Fieldable f = fields[i];
      add(all,f);
      if (f.isIndexed()) add(indexed,f);
      else add(unindexed,f);
      if (f.isTermVectorStored()) add(termvector,f);
      if (f.isIndexed() && !f.isTermVectorStored()) add(notermvector,f);
      if (f.isStored()) add(stored,f);
      else add(unstored,f);
      if (f.getOmitNorms()) add(noNorms,f);
      if (f.getIndexOptions() == IndexOptions.DOCS_ONLY) add(noTf,f);
      if (f.isLazy()) add(lazy, f);
    }
  }


  private static void add(Map<String,Fieldable> map, Fieldable field) {
    map.put(field.name(), field);
  }


  static
  {
    nameValues = new HashMap<String,Object>();
    nameValues.put(TEXT_FIELD_1_KEY, FIELD_1_TEXT);
    nameValues.put(TEXT_FIELD_2_KEY, FIELD_2_TEXT);
    nameValues.put(TEXT_FIELD_3_KEY, FIELD_3_TEXT);
    nameValues.put(KEYWORD_FIELD_KEY, KEYWORD_TEXT);
    nameValues.put(NO_NORMS_KEY, NO_NORMS_TEXT);
    nameValues.put(NO_TF_KEY, NO_TF_TEXT);
    nameValues.put(UNINDEXED_FIELD_KEY, UNINDEXED_FIELD_TEXT);
    nameValues.put(UNSTORED_FIELD_1_KEY, UNSTORED_1_FIELD_TEXT);
    nameValues.put(UNSTORED_FIELD_2_KEY, UNSTORED_2_FIELD_TEXT);
    nameValues.put(LAZY_FIELD_KEY, LAZY_FIELD_TEXT);
    nameValues.put(LAZY_FIELD_BINARY_KEY, LAZY_FIELD_BINARY_BYTES);
    nameValues.put(LARGE_LAZY_FIELD_KEY, LARGE_LAZY_FIELD_TEXT);
    nameValues.put(TEXT_FIELD_UTF1_KEY, FIELD_UTF1_TEXT);
    nameValues.put(TEXT_FIELD_UTF2_KEY, FIELD_UTF2_TEXT);
  }   
  
  /**
   * Adds the fields above to a document 
   * @param doc The document to write
   */ 
  public static void setupDoc(Document doc) {
    for (int i=0; i<fields.length; i++) {
      doc.add(fields[i]);
    }
  }                         

  /**
   * Writes the document to the directory using a segment
   * named "test"; returns the SegmentInfo describing the new
   * segment 
   * @param dir
   * @param doc
   * @throws IOException
   */ 
  public static SegmentInfo writeDoc(Random random, Directory dir, Document doc) throws IOException
  {
    return writeDoc(random, dir, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false), null, doc);
  }

  /**
   * Writes the document to the directory using the analyzer
   * and the similarity score; returns the SegmentInfo
   * describing the new segment
   * @param dir
   * @param analyzer
   * @param similarity
   * @param doc
   * @throws IOException
   */ 
  public static SegmentInfo writeDoc(Random random, Directory dir, Analyzer analyzer, SimilarityProvider similarity, Document doc) throws IOException {
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig( /* LuceneTestCase.newIndexWriterConfig(random, */ 
        TEST_VERSION_CURRENT, analyzer).setSimilarityProvider(similarity));
    //writer.setUseCompoundFile(false);
    writer.addDocument(doc);
    writer.commit();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    return info;
  }

  public static int numFields(Document doc) {
    return doc.getFields().size();
  }
  
  public static Document createDocument(int n, String indexName, int numFields) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    doc.add(new Field("id", Integer.toString(n), Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("indexname", indexName, Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    sb.append("a");
    sb.append(n);
    doc.add(new Field("field1", sb.toString(), Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i + 1), sb.toString(), Store.YES,
                        Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    }
    return doc;
  }
}
