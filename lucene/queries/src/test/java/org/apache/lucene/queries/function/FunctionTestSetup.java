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
package org.apache.lucene.queries.function;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedFloatFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedIntFieldSource;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Ignore;

/**
 * Setup for function tests
 */
@Ignore
public abstract class FunctionTestSetup extends LuceneTestCase {

  /**
   * Actual score computation order is slightly different than assumptios
   * this allows for a small amount of variation
   */
  protected static float TEST_SCORE_TOLERANCE_DELTA = 0.001f;

  protected static final int N_DOCS = 17; // select a primary number > 2

  protected static final String ID_FIELD = "id";
  protected static final String TEXT_FIELD = "text";
  protected static final String INT_FIELD = "iii";
  /**
   * This field is multiValued and should give the exact same results as
   * {@link #INT_FIELD} when used with MIN selector
   */
  protected static final String INT_FIELD_MV_MIN = "iii_min";
  /**
   * This field is multiValued and should give the exact same results as
   * {@link #INT_FIELD} when used with MAX selector
   */
  protected static final String INT_FIELD_MV_MAX = "iii_max";

  protected static final String FLOAT_FIELD = "fff";
  /**
   * This field is multiValued and should give the exact same results as
   * {@link #FLOAT_FIELD} when used with MIN selector
   */
  protected static final String FLOAT_FIELD_MV_MIN = "fff_min";
  /**
   * This field is multiValued and should give the exact same results as
   * {@link #FLOAT_FIELD} when used with MAX selector
   */
  protected static final String FLOAT_FIELD_MV_MAX = "fff_max";

  protected ValueSource INT_VALUESOURCE = new IntFieldSource(INT_FIELD);
  protected ValueSource INT_MV_MIN_VALUESOURCE = new MultiValuedIntFieldSource(INT_FIELD_MV_MIN, SortedNumericSelector.Type.MIN);
  protected ValueSource INT_MV_MAX_VALUESOURCE = new MultiValuedIntFieldSource(INT_FIELD_MV_MAX, SortedNumericSelector.Type.MAX);
  protected ValueSource FLOAT_VALUESOURCE = new FloatFieldSource(FLOAT_FIELD);
  protected ValueSource FLOAT_MV_MIN_VALUESOURCE = new MultiValuedFloatFieldSource(FLOAT_FIELD_MV_MIN, SortedNumericSelector.Type.MIN);
  protected ValueSource FLOAT_MV_MAX_VALUESOURCE = new MultiValuedFloatFieldSource(FLOAT_FIELD_MV_MAX, SortedNumericSelector.Type.MAX);

  private static final String DOC_TEXT_LINES[] = {
          "Well, this is just some plain text we use for creating the ",
          "test documents. It used to be a text from an online collection ",
          "devoted to first aid, but if there was there an (online) lawyers ",
          "first aid collection with legal advices, \"it\" might have quite ",
          "probably advised one not to include \"it\"'s text or the text of ",
          "any other online collection in one's code, unless one has money ",
          "that one don't need and one is happy to donate for lawyers ",
          "charity. Anyhow at some point, rechecking the usage of this text, ",
          "it became uncertain that this text is free to use, because ",
          "the web site in the disclaimer of he eBook containing that text ",
          "was not responding anymore, and at the same time, in projGut, ",
          "searching for first aid no longer found that eBook as well. ",
          "So here we are, with a perhaps much less interesting ",
          "text for the test, but oh much much safer. ",
  };

  protected static Directory dir;
  protected static Analyzer anlzr;

  @AfterClass
  public static void afterClassFunctionTestSetup() throws Exception {
    dir.close();
    dir = null;
    anlzr.close();
    anlzr = null;
  }

  protected static void createIndex(boolean doMultiSegment) throws Exception {
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    // prepare a small index with just a few documents.
    dir = newDirectory();
    anlzr = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(anlzr).setMergePolicy(newLogMergePolicy());
    if (doMultiSegment) {
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 2, 7));
    }
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    // add docs not exactly in natural ID order, to verify we do check the order of docs by scores
    int remaining = N_DOCS;
    boolean done[] = new boolean[N_DOCS];
    int i = 0;
    while (remaining > 0) {
      if (done[i]) {
        throw new Exception("to set this test correctly N_DOCS=" + N_DOCS + " must be primary and greater than 2!");
      }
      addDoc(iw, i);
      done[i] = true;
      i = (i + 4) % N_DOCS;
      remaining --;
    }
    if (!doMultiSegment) {
      if (VERBOSE) {
        System.out.println("TEST: setUp full merge");
      }
      iw.forceMerge(1);
    }
    iw.close();
    if (VERBOSE) {
      System.out.println("TEST: setUp done close");
    }
  }

  private static void addDoc(RandomIndexWriter iw, int i) throws Exception {
    Document d = new Document();
    Field f;
    int scoreAndID = i + 1;

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    customType.setOmitNorms(true);
    
    f = newField(ID_FIELD, id2String(scoreAndID), customType); // for debug purposes
    d.add(f);
    d.add(new SortedDocValuesField(ID_FIELD, new BytesRef(id2String(scoreAndID))));

    FieldType customType2 = new FieldType(TextField.TYPE_NOT_STORED);
    customType2.setOmitNorms(true);
    f = newField(TEXT_FIELD, "text of doc" + scoreAndID + textLine(i), customType2); // for regular search
    d.add(f);

    f = new StoredField(INT_FIELD, scoreAndID); // for function scoring
    d.add(f);
    d.add(new NumericDocValuesField(INT_FIELD, scoreAndID));

    f = new StoredField(FLOAT_FIELD, scoreAndID); // for function scoring
    d.add(f);
    d.add(new NumericDocValuesField(FLOAT_FIELD, Float.floatToRawIntBits(scoreAndID)));
    
    f = new StoredField(INT_FIELD_MV_MIN, scoreAndID);
    d.add(f);
    f = new StoredField(INT_FIELD_MV_MIN, scoreAndID + 1);
    d.add(f);
    d.add(new SortedNumericDocValuesField(INT_FIELD_MV_MIN, scoreAndID));
    d.add(new SortedNumericDocValuesField(INT_FIELD_MV_MIN, scoreAndID + 1));
    
    f = new StoredField(INT_FIELD_MV_MAX, scoreAndID);
    d.add(f);
    f = new StoredField(INT_FIELD_MV_MAX, scoreAndID - 1);
    d.add(f);
    d.add(new SortedNumericDocValuesField(INT_FIELD_MV_MAX, scoreAndID));
    d.add(new SortedNumericDocValuesField(INT_FIELD_MV_MAX, scoreAndID - 1));
    
    f = new StoredField(FLOAT_FIELD_MV_MIN, scoreAndID);
    d.add(f);
    f = new StoredField(FLOAT_FIELD_MV_MIN, scoreAndID + 1);
    d.add(f);
    d.add(new SortedNumericDocValuesField(FLOAT_FIELD_MV_MIN, NumericUtils.floatToSortableInt(scoreAndID)));
    d.add(new SortedNumericDocValuesField(FLOAT_FIELD_MV_MIN, NumericUtils.floatToSortableInt(scoreAndID + 1)));
    
    f = new StoredField(FLOAT_FIELD_MV_MAX, scoreAndID);
    d.add(f);
    f = new StoredField(FLOAT_FIELD_MV_MAX, scoreAndID - 1);
    d.add(f);
    d.add(new SortedNumericDocValuesField(FLOAT_FIELD_MV_MAX, NumericUtils.floatToSortableInt(scoreAndID)));
    d.add(new SortedNumericDocValuesField(FLOAT_FIELD_MV_MAX, NumericUtils.floatToSortableInt(scoreAndID - 1)));

    iw.addDocument(d);
    log("added: " + d);
  }

  // 17 --> ID00017
  protected static String id2String(int scoreAndID) {
    String s = "000000000" + scoreAndID;
    int n = ("" + N_DOCS).length() + 3;
    int k = s.length() - n;
    return "ID" + s.substring(k);
  }

  // some text line for regular search
  private static String textLine(int docNum) {
    return DOC_TEXT_LINES[docNum % DOC_TEXT_LINES.length];
  }

  // extract expected doc score from its ID Field: "ID7" --> 7.0
  protected static float expectedFieldScore(String docIDFieldVal) {
    return Float.parseFloat(docIDFieldVal.substring(2));
  }

  // debug messages (change DBG to true for anything to print)
  protected static void log(Object o) {
    if (VERBOSE) {
      System.out.println(o.toString());
    }
  }
}
