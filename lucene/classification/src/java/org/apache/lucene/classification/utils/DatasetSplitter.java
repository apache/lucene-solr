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
package org.apache.lucene.classification.utils;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
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

    try {
      int size = originalIndex.maxDoc();

      IndexSearcher indexSearcher = new IndexSearcher(originalIndex);
      TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

      // set the type to be indexed, stored, with term vectors
      FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(true);
      ft.setStoreTermVectorPositions(true);

      int b = 0;

      // iterate over existing documents
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

        // create a new document for indexing
        Document doc = new Document();
        Document document = originalIndex.document(scoreDoc.doc);
        if (fieldNames != null && fieldNames.length > 0) {
          for (String fieldName : fieldNames) {
            IndexableField field = document.getField(fieldName);
            if (field != null) {
              doc.add(new Field(fieldName, field.stringValue(), ft));
            }
          }
        } else {
          for (IndexableField field : document.getFields()) {
            if (field.readerValue() != null) {
              doc.add(new Field(field.name(), field.readerValue(), ft));
            } else if (field.binaryValue() != null) {
              doc.add(new Field(field.name(), field.binaryValue(), ft));
            } else if (field.stringValue() != null) {
              doc.add(new Field(field.name(), field.stringValue(), ft));
            } else if (field.numericValue() != null) {
              doc.add(new Field(field.name(), field.numericValue().toString(), ft));
            }
          }
        }

        // add it to one of the IDXs
        if (b % 2 == 0 && testWriter.maxDoc() < size * testRatio) {
          testWriter.addDocument(doc);
        } else if (cvWriter.maxDoc() < size * crossValidationRatio) {
          cvWriter.addDocument(doc);
        } else {
          trainingWriter.addDocument(doc);
        }
        b++;
      }
      // commit
      testWriter.commit();
      cvWriter.commit();
      trainingWriter.commit();

      // merge
      testWriter.forceMerge(3);
      cvWriter.forceMerge(3);
      trainingWriter.forceMerge(3);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      // close IWs
      testWriter.close();
      cvWriter.close();
      trainingWriter.close();
    }
  }

}
