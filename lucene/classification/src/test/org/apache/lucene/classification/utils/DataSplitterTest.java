package org.apache.lucene.classification.utils;

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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for {@link org.apache.lucene.classification.utils.DatasetSplitter}
 */
public class DataSplitterTest extends LuceneTestCase {

  private LeafReader originalIndex;
  private RandomIndexWriter indexWriter;
  private Directory dir;

  private String textFieldName = "text";
  private String classFieldName = "class";
  private String idFieldName = "id";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    indexWriter = new RandomIndexWriter(random(), dir);

    FieldTypes fieldTypes = indexWriter.getFieldTypes();
    fieldTypes.enableTermVectors(idFieldName);
    fieldTypes.enableTermVectorPositions(idFieldName);
    fieldTypes.enableTermVectorOffsets(idFieldName);

    fieldTypes.enableTermVectors(textFieldName);
    fieldTypes.enableTermVectorPositions(textFieldName);
    fieldTypes.enableTermVectorOffsets(textFieldName);

    fieldTypes.enableTermVectors(classFieldName);
    fieldTypes.enableTermVectorPositions(classFieldName);
    fieldTypes.enableTermVectorOffsets(classFieldName);

    Random rnd = random();
    for (int i = 0; i < 100; i++) {
      Document doc = indexWriter.newDocument();
      doc.addAtom(idFieldName, Integer.toString(i));
      doc.addLargeText(textFieldName, TestUtil.randomUnicodeString(rnd, 1024));
      doc.addLargeText(classFieldName, TestUtil.randomUnicodeString(rnd, 10));
      indexWriter.addDocument(doc);
    }

    indexWriter.commit();

    originalIndex = SlowCompositeReaderWrapper.wrap(indexWriter.getReader());

  }

  @Override
  @After
  public void tearDown() throws Exception {
    originalIndex.close();
    indexWriter.close();
    dir.close();
    super.tearDown();
  }


  @Test
  public void testSplitOnAllFields() throws Exception {
    assertSplit(originalIndex, 0.1, 0.1);
  }


  @Test
  public void testSplitOnSomeFields() throws Exception {
    assertSplit(originalIndex, 0.2, 0.35, idFieldName, textFieldName);
  }

  public static void assertSplit(LeafReader originalIndex, double testRatio, double crossValidationRatio, String... fieldNames) throws Exception {

    BaseDirectoryWrapper trainingIndex = newDirectory();
    BaseDirectoryWrapper testIndex = newDirectory();
    BaseDirectoryWrapper crossValidationIndex = newDirectory();

    try {
      DatasetSplitter datasetSplitter = new DatasetSplitter(testRatio, crossValidationRatio);
      datasetSplitter.split(originalIndex, trainingIndex, testIndex, crossValidationIndex, new MockAnalyzer(random()), fieldNames);

      assertNotNull(trainingIndex);
      assertNotNull(testIndex);
      assertNotNull(crossValidationIndex);

      DirectoryReader trainingReader = DirectoryReader.open(trainingIndex);
      assertTrue((int) (originalIndex.maxDoc() * (1d - testRatio - crossValidationRatio)) == trainingReader.maxDoc());
      DirectoryReader testReader = DirectoryReader.open(testIndex);
      assertTrue((int) (originalIndex.maxDoc() * testRatio) == testReader.maxDoc());
      DirectoryReader cvReader = DirectoryReader.open(crossValidationIndex);
      assertTrue((int) (originalIndex.maxDoc() * crossValidationRatio) == cvReader.maxDoc());

      trainingReader.close();
      testReader.close();
      cvReader.close();
      closeQuietly(trainingReader);
      closeQuietly(testReader);
      closeQuietly(cvReader);
    } finally {
      if (trainingIndex != null) {
        trainingIndex.close();
      }
      if (testIndex != null) {
        testIndex.close();
      }
      if (crossValidationIndex != null) {
        crossValidationIndex.close();
      }
    }
  }

  private static void closeQuietly(IndexReader reader) throws IOException {
    try {
      if (reader != null)
        reader.close();
    } catch (Exception e) {
      // do nothing
    }
  }
}
