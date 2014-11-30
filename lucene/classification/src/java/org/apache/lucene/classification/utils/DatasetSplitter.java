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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

/**
 * Utility class for creating training / test / cross validation indexes from the original index.
 */
public class DatasetSplitter {

  private final double crossValidationRatio;
  private final double testRatio;

  /**
   * Create a {@link DatasetSplitter} by giving test and cross validation IDXs sizes
   *
   * @param testRatio            the ratio of the original index to be used for the test IDX as a <code>double</code> between 0.0 and 1.0
   * @param crossValidationRatio the ratio of the original index to be used for the c.v. IDX as a <code>double</code> between 0.0 and 1.0
   */
  public DatasetSplitter(double testRatio, double crossValidationRatio) {
    this.crossValidationRatio = crossValidationRatio;
    this.testRatio = testRatio;
  }

  /**
   * Split a given index into 3 indexes for training, test and cross validation tasks respectively
   *
   * @param originalIndex        an {@link org.apache.lucene.index.LeafReader} on the source index
   * @param trainingIndex        a {@link Directory} used to write the training index
   * @param testIndex            a {@link Directory} used to write the test index
   * @param crossValidationIndex a {@link Directory} used to write the cross validation index
   * @param analyzer             {@link Analyzer} used to create the new docs
   * @param fieldNames           names of fields that need to be put in the new indexes or <code>null</code> if all should be used
   * @throws IOException if any writing operation fails on any of the indexes
   */
  public void split(LeafReader originalIndex, Directory trainingIndex, Directory testIndex, Directory crossValidationIndex,
                    Analyzer analyzer, String... fieldNames) throws IOException {

    // create IWs for train / test / cv IDXs
    IndexWriter testWriter = new IndexWriter(testIndex, new IndexWriterConfig(analyzer));
    IndexWriter cvWriter = new IndexWriter(crossValidationIndex, new IndexWriterConfig(analyzer));
    IndexWriter trainingWriter = new IndexWriter(trainingIndex, new IndexWriterConfig(analyzer));

    for (IndexWriter w : new IndexWriter[] {testWriter, cvWriter, trainingWriter}) {
      FieldTypes fieldTypes = w.getFieldTypes();
      for (String fieldName : fieldNames) {
        fieldTypes.enableTermVectors(fieldName);
        fieldTypes.enableTermVectorPositions(fieldName);
        fieldTypes.enableTermVectorOffsets(fieldName);
      }
    }

    try {
      int size = originalIndex.maxDoc();

      IndexSearcher indexSearcher = new IndexSearcher(originalIndex);
      TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

      int b = 0;

      // iterate over existing documents
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        IndexWriter w;
        if (b % 2 == 0 && testWriter.maxDoc() < size * testRatio) {
          w = testWriter;
        } else if (cvWriter.maxDoc() < size * crossValidationRatio) {
          w = cvWriter;
        } else {
          w = trainingWriter;
        }

        // create a new document for indexing
        Document doc = w.newDocument();
        if (fieldNames != null && fieldNames.length > 0) {
          for (String fieldName : fieldNames) {
            doc.addLargeText(fieldName, originalIndex.document(scoreDoc.doc).getField(fieldName).stringValue());
          }
        } else {
          for (IndexableField storableField : originalIndex.document(scoreDoc.doc).getFields()) {
            doc.addLargeText(storableField.name(), storableField.stringValue());
          }
        }

        // add it to one of the IDXs
        w.addDocument(doc);
        b++;
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      testWriter.commit();
      cvWriter.commit();
      trainingWriter.commit();
      // close IWs
      testWriter.close();
      cvWriter.close();
      trainingWriter.close();
    }
  }

}
