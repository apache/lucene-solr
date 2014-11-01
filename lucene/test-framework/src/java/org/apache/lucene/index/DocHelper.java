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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

class DocHelper {

  public static final String FIELD_1_TEXT = "field one text";
  public static final String TEXT_FIELD_1_KEY = "textField1";

  public static final String FIELD_2_TEXT = "field field field two text";
  //Fields will be lexicographically sorted.  So, the order is: field, text, two
  public static final int [] FIELD_2_FREQS = {3, 1, 1}; 
  public static final String TEXT_FIELD_2_KEY = "textField2";
  
  public static final String FIELD_3_TEXT = "aaaNoNorms aaaNoNorms bbbNoNorms";
  public static final String TEXT_FIELD_3_KEY = "textField3";
  
  public static final String KEYWORD_TEXT = "Keyword";
  public static final String KEYWORD_FIELD_KEY = "keyField";

  public static final String NO_NORMS_TEXT = "omitNormsText";
  public static final String NO_NORMS_KEY = "omitNorms";

  public static final String NO_TF_TEXT = "analyzed with no tf and positions";
  public static final String NO_TF_KEY = "omitTermFreqAndPositions";

  public static final String UNINDEXED_FIELD_TEXT = "unindexed field text";
  public static final String UNINDEXED_FIELD_KEY = "unIndField";

  public static final String UNSTORED_1_FIELD_TEXT = "unstored field text";
  public static final String UNSTORED_FIELD_1_KEY = "unStoredField1";

  public static final String UNSTORED_2_FIELD_TEXT = "unstored field text";
  public static final String UNSTORED_FIELD_2_KEY = "unStoredField2";

  public static final String LAZY_FIELD_BINARY_KEY = "lazyFieldBinary";
  public static final BytesRef LAZY_FIELD_BINARY_BYTES = new BytesRef("These are some binary field bytes");

  public static final String LAZY_FIELD_KEY = "lazyField";
  public static final String LAZY_FIELD_TEXT = "These are some field bytes";
  
  public static final String LARGE_LAZY_FIELD_KEY = "largeLazyField";
  public static String LARGE_LAZY_FIELD_TEXT;
  
  //From Issue 509
  public static final String FIELD_UTF1_TEXT = "field one \u4e00text";
  public static final String TEXT_FIELD_UTF1_KEY = "textField1Utf8";

  public static final String FIELD_UTF2_TEXT = "field field field \u4e00two text";
  //Fields will be lexicographically sorted.  So, the order is: field, text, two
  public static final int [] FIELD_UTF2_FREQS = {3, 1, 1};
  public static final String TEXT_FIELD_UTF2_KEY = "textField2Utf8";
 
  public static Map<String,Object> nameValues = null;

  static {
    //Initialize the large Lazy Field
    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      buffer.append("Lazily loading lengths of language in lieu of laughing ");
    }
    
    LARGE_LAZY_FIELD_TEXT = buffer.toString();
  }

  static {
    nameValues = new HashMap<>();
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
  private static void setupDoc(FieldTypes fieldTypes, Document2 doc) {

    fieldTypes.enableTermVectors(TEXT_FIELD_2_KEY);
    fieldTypes.disableHighlighting(TEXT_FIELD_2_KEY);
    fieldTypes.enableTermVectorPositions(TEXT_FIELD_2_KEY);
    fieldTypes.enableTermVectorOffsets(TEXT_FIELD_2_KEY);

    fieldTypes.enableTermVectors(TEXT_FIELD_UTF2_KEY);
    fieldTypes.disableHighlighting(TEXT_FIELD_UTF2_KEY);
    fieldTypes.enableTermVectorPositions(TEXT_FIELD_UTF2_KEY);
    fieldTypes.enableTermVectorOffsets(TEXT_FIELD_UTF2_KEY);

    fieldTypes.disableHighlighting(TEXT_FIELD_3_KEY);
    fieldTypes.disableNorms(TEXT_FIELD_3_KEY);

    fieldTypes.disableHighlighting(NO_NORMS_KEY);
    fieldTypes.setIndexOptions(NO_NORMS_KEY, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldTypes.disableNorms(NO_NORMS_KEY);

    // Otherwise large text:
    fieldTypes.disableHighlighting(NO_TF_KEY);
    fieldTypes.setIndexOptions(NO_TF_KEY, IndexOptions.DOCS_ONLY);

    // Otherwise large text:
    fieldTypes.disableStored(UNSTORED_FIELD_1_KEY);

    // Otherwise large text:
    fieldTypes.enableTermVectors(UNSTORED_FIELD_2_KEY);
    fieldTypes.disableStored(UNSTORED_FIELD_2_KEY);

    doc.addLargeText(TEXT_FIELD_1_KEY, FIELD_1_TEXT);
    doc.addLargeText(TEXT_FIELD_2_KEY, FIELD_2_TEXT);
    doc.addLargeText(TEXT_FIELD_3_KEY, FIELD_3_TEXT);
    doc.addAtom(KEYWORD_FIELD_KEY, KEYWORD_TEXT);
    doc.addLargeText(NO_NORMS_KEY, NO_NORMS_TEXT);
    doc.addLargeText(NO_TF_KEY, NO_TF_TEXT);
    doc.addStored(UNINDEXED_FIELD_KEY, UNINDEXED_FIELD_TEXT);
    doc.addLargeText(UNSTORED_FIELD_1_KEY, UNSTORED_1_FIELD_TEXT);
    doc.addLargeText(UNSTORED_FIELD_2_KEY, UNSTORED_2_FIELD_TEXT);
    doc.addLargeText(LAZY_FIELD_KEY, LAZY_FIELD_TEXT);
    doc.addStored(LAZY_FIELD_BINARY_KEY, LAZY_FIELD_BINARY_BYTES);
    doc.addLargeText(LARGE_LAZY_FIELD_KEY, LARGE_LAZY_FIELD_TEXT);
    doc.addLargeText(TEXT_FIELD_UTF1_KEY, FIELD_UTF1_TEXT);
    doc.addLargeText(TEXT_FIELD_UTF2_KEY, FIELD_UTF2_TEXT);
  }                         

  public static Set<String> getUnstored(FieldTypes fieldTypes) {
    Set<String> unstored = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getStored(fieldName) == false) {
        unstored.add(fieldName);
      }
    }
    return unstored;
  }

  public static Set<String> getIndexed(FieldTypes fieldTypes) {
    Set<String> indexed = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getIndexOptions(fieldName) != null) {
        indexed.add(fieldName);
      }
    }
    return indexed;
  }

  public static Set<String> getNotIndexed(FieldTypes fieldTypes) {
    Set<String> notIndexed = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getIndexOptions(fieldName) == null) {
        notIndexed.add(fieldName);
      }
    }
    return notIndexed;
  }

  public static Set<String> getTermVectorFields(FieldTypes fieldTypes) {
    Set<String> tvFields = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getTermVectors(fieldName)) {
        tvFields.add(fieldName);
      }
    }
    return tvFields;
  }

  public static Set<String> getNoTermVectorFields(FieldTypes fieldTypes) {
    Set<String> noTVFields = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getIndexOptions(fieldName) != null && fieldTypes.getTermVectors(fieldName) == false) {
        noTVFields.add(fieldName);
      }
    }
    return noTVFields;
  }

  public static Set<String> getNoNorms(FieldTypes fieldTypes) {
    Set<String> noNorms = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      if (fieldTypes.getNorms(fieldName) == false) {
        noNorms.add(fieldName);
      }
    }
    return noNorms;
  }

  public static Set<String> getAll(FieldTypes fieldTypes) {
    Set<String> all = new HashSet<>();
    for(String fieldName : fieldTypes.getFieldNames()) {
      all.add(fieldName);
    }
    return all;
  }

  /**
   * Writes our test document to the directory and returns the SegmentCommitInfo
   * describing the new segment 
   */ 
  public static SegmentCommitInfo writeDoc(Random random, Directory dir) throws IOException {
    return writeDoc(random, dir, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false), null);
  }

  /**
   * Writes the document to the directory using the analyzer
   * and the similarity score; returns the SegmentInfo
   * describing the new segment
   */ 
  public static SegmentCommitInfo writeDoc(Random random, Directory dir, Analyzer analyzer, Similarity similarity) throws IOException {
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig( /* LuceneTestCase.newIndexWriterConfig(random, */ 
        analyzer).setSimilarity(similarity == null ? IndexSearcher.getDefaultSimilarity() : similarity));
    Document2 doc = writer.newDocument();
    setupDoc(writer.getFieldTypes(), doc);
    writer.addDocument(doc);
    writer.commit();
    SegmentCommitInfo info = writer.newestSegment();
    writer.close();
    return info;
  }

  public static int numFields() {
    return 14;
  }

  public static int numFields(Document2 doc) {
    return doc.getFields().size();
  }
  
  public static Document2 createDocument(IndexWriter writer, int n, String indexName, int numFields) {
    StringBuilder sb = new StringBuilder();

    FieldTypes fieldTypes = writer.getFieldTypes();
    for(int i=0;i<numFields+1;i++) {
      fieldTypes.enableTermVectors("field" + (i+1));
      fieldTypes.enableTermVectorPositions("field" + (i+1));
      fieldTypes.enableTermVectorOffsets("field" + (i+1));
    }
    fieldTypes.enableTermVectors("id");
    fieldTypes.enableTermVectorPositions("id");
    fieldTypes.enableTermVectorOffsets("id");

    Document2 doc = writer.newDocument();
    doc.addAtom("id", Integer.toString(n));
    doc.addAtom("indexname", indexName);
    sb.append("a");
    sb.append(n);
    doc.addLargeText("field1", sb.toString());
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.addLargeText("field" + (i+1), sb.toString());
    }
    return doc;
  }
}
