package org.apache.lucene.index;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Enumeration;

class DocHelper {
  public static final String FIELD_1_TEXT = "field one text";
  public static final String TEXT_FIELD_1_KEY = "textField1";
  public static Field textField1 = new Field(TEXT_FIELD_1_KEY, FIELD_1_TEXT,
      Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO);
  
  public static final String FIELD_2_TEXT = "field field field two text";
  //Fields will be lexicographically sorted.  So, the order is: field, text, two
  public static final int [] FIELD_2_FREQS = {3, 1, 1}; 
  public static final String TEXT_FIELD_2_KEY = "textField2";
  public static Field textField2 = new Field(TEXT_FIELD_2_KEY, FIELD_2_TEXT, Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS);

  public static final String FIELD_3_TEXT = "aaaNoNorms aaaNoNorms bbbNoNorms";
  public static final String TEXT_FIELD_3_KEY = "textField3";
  public static Field textField3 = new Field(TEXT_FIELD_3_KEY, FIELD_3_TEXT, Field.Store.YES, Field.Index.TOKENIZED);
  static { textField3.setOmitNorms(true); }

  public static final String KEYWORD_TEXT = "Keyword";
  public static final String KEYWORD_FIELD_KEY = "keyField";
  public static Field keyField = new Field(KEYWORD_FIELD_KEY, KEYWORD_TEXT,
      Field.Store.YES, Field.Index.UN_TOKENIZED);

  public static final String NO_NORMS_TEXT = "omitNormsText";
  public static final String NO_NORMS_KEY = "omitNorms";
  public static Field noNormsField = new Field(NO_NORMS_KEY, NO_NORMS_TEXT,
      Field.Store.YES, Field.Index.NO_NORMS);

  public static final String UNINDEXED_FIELD_TEXT = "unindexed field text";
  public static final String UNINDEXED_FIELD_KEY = "unIndField";
  public static Field unIndField = new Field(UNINDEXED_FIELD_KEY, UNINDEXED_FIELD_TEXT,
      Field.Store.YES, Field.Index.NO);


  public static final String UNSTORED_1_FIELD_TEXT = "unstored field text";
  public static final String UNSTORED_FIELD_1_KEY = "unStoredField1";
  public static Field unStoredField1 = new Field(UNSTORED_FIELD_1_KEY, UNSTORED_1_FIELD_TEXT,
      Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.NO);

  public static final String UNSTORED_2_FIELD_TEXT = "unstored field text";
  public static final String UNSTORED_FIELD_2_KEY = "unStoredField2";
  public static Field unStoredField2 = new Field(UNSTORED_FIELD_2_KEY, UNSTORED_2_FIELD_TEXT,
      Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES);

  public static final String REPEATED_1_TEXT = "repeated one";
  public static final String REPEATED_KEY = "repeated";
  public static Field repeatedField1 = new Field(REPEATED_KEY, REPEATED_1_TEXT,
      Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO);
  public static final String REPEATED_2_TEXT = "repeated two";
  public static Field repeatedField2 = new Field(REPEATED_KEY, REPEATED_2_TEXT,
      Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO);

  public static Map nameValues = null;

  // ordered list of all the fields...
  // could use LinkedHashMap for this purpose if Java1.4 is OK
  public static Field[] fields = new Field[] {
    textField1,
    textField2,
    textField3,
    keyField,
    noNormsField,
    unIndField,
    unStoredField1,
    unStoredField2,
    repeatedField1,
    repeatedField2
  };

  // Map<String fieldName, Field field>
  public static Map all=new HashMap();
  public static Map indexed=new HashMap();
  public static Map stored=new HashMap();
  public static Map unstored=new HashMap();
  public static Map unindexed=new HashMap();
  public static Map termvector=new HashMap();
  public static Map notermvector=new HashMap();
  public static Map noNorms=new HashMap();

  static {
    for (int i=0; i<fields.length; i++) {
      Field f = fields[i];
      add(all,f);
      if (f.isIndexed()) add(indexed,f);
      else add(unindexed,f);
      if (f.isTermVectorStored()) add(termvector,f);
      if (f.isIndexed() && !f.isTermVectorStored()) add(notermvector,f);
      if (f.isStored()) add(stored,f);
      else add(unstored,f);
      if (f.getOmitNorms()) add(noNorms,f);
    }
  }


  private static void add(Map map, Field field) {
    map.put(field.name(), field);
  }


  static
  {
    nameValues = new HashMap();
    nameValues.put(TEXT_FIELD_1_KEY, FIELD_1_TEXT);
    nameValues.put(TEXT_FIELD_2_KEY, FIELD_2_TEXT);
    nameValues.put(TEXT_FIELD_3_KEY, FIELD_3_TEXT);
    nameValues.put(KEYWORD_FIELD_KEY, KEYWORD_TEXT);
    nameValues.put(NO_NORMS_KEY, NO_NORMS_TEXT);
    nameValues.put(UNINDEXED_FIELD_KEY, UNINDEXED_FIELD_TEXT);
    nameValues.put(UNSTORED_FIELD_1_KEY, UNSTORED_1_FIELD_TEXT);
    nameValues.put(UNSTORED_FIELD_2_KEY, UNSTORED_2_FIELD_TEXT);
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
   * Writes the document to the directory using a segment named "test"
   * @param dir
   * @param doc
   * @throws IOException
   */ 
  public static void writeDoc(Directory dir, Document doc) throws IOException
  {
    writeDoc(dir, "test", doc);
  }

  /**
   * Writes the document to the directory in the given segment
   * @param dir
   * @param segment
   * @param doc
   * @throws IOException
   */ 
  public static void writeDoc(Directory dir, String segment, Document doc) throws IOException
  {
    Similarity similarity = Similarity.getDefault();
    writeDoc(dir, new WhitespaceAnalyzer(), similarity, segment, doc);
  }

  /**
   * Writes the document to the directory segment named "test" using the specified analyzer and similarity
   * @param dir
   * @param analyzer
   * @param similarity
   * @param doc
   * @throws IOException
   */ 
  public static void writeDoc(Directory dir, Analyzer analyzer, Similarity similarity, Document doc) throws IOException
  {
    writeDoc(dir, analyzer, similarity, "test", doc);
  }

  /**
   * Writes the document to the directory segment using the analyzer and the similarity score
   * @param dir
   * @param analyzer
   * @param similarity
   * @param segment
   * @param doc
   * @throws IOException
   */ 
  public static void writeDoc(Directory dir, Analyzer analyzer, Similarity similarity, String segment, Document doc) throws IOException
  {
    DocumentWriter writer = new DocumentWriter(dir, analyzer, similarity, 50);
    writer.addDocument(segment, doc);
  }

  public static int numFields(Document doc) {
    Enumeration fields = doc.fields();
    int result = 0;
    while (fields.hasMoreElements()) {
      String name = fields.nextElement().toString();
      name += "";   // avoid compiler warning
      result++;
    }
    return result;
  }
}
