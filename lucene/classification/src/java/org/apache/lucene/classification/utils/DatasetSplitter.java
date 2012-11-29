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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * Utility class for creating training / test / cross validation indexes from the original index.
 */
public class DatasetSplitter {

  private double crossValidationRatio;
  private double testRatio;

  public DatasetSplitter(double testRatio, double crossValidationRatio) {
    this.crossValidationRatio = crossValidationRatio;
    this.testRatio = testRatio;
  }

  public void split(AtomicReader originalIndex, Directory trainingIndex, Directory testIndex, Directory crossValidationIndex,
                    Analyzer analyzer, String... fieldNames) throws IOException {

    // TODO : check that the passed fields are stored in the original index

    // create IWs for train / test / cv IDXs
    IndexWriter testWriter = new IndexWriter(testIndex, new IndexWriterConfig(Version.LUCENE_50, analyzer));
    IndexWriter cvWriter = new IndexWriter(crossValidationIndex, new IndexWriterConfig(Version.LUCENE_50, analyzer));
    IndexWriter trainingWriter = new IndexWriter(trainingIndex, new IndexWriterConfig(Version.LUCENE_50, analyzer));

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
        if (fieldNames != null && fieldNames.length > 0) {
          for (String fieldName : fieldNames) {
            doc.add(new Field(fieldName, originalIndex.document(scoreDoc.doc).getField(fieldName).stringValue(), ft));
          }
        } else {
          for (StorableField storableField : originalIndex.document(scoreDoc.doc).getFields()) {
            if (storableField.readerValue()!= null){
              doc.add(new Field(storableField.name(), storableField.readerValue(), ft));
            }
            else if (storableField.binaryValue()!= null){
              doc.add(new Field(storableField.name(), storableField.binaryValue(), ft));
            }
            else if (storableField.stringValue()!= null){
              doc.add(new Field(storableField.name(), storableField.stringValue(), ft));
            }
            else if (storableField.numericValue()!= null){
              doc.add(new Field(storableField.name(), storableField.numericValue().toString(), ft));
            }
          }
        }

        // add it to one of the IDXs
        if (b % 2 == 0 && testWriter.maxDoc() < size * testRatio) {
          testWriter.addDocument(doc);
          testWriter.commit();
        } else if (cvWriter.maxDoc() < size * crossValidationRatio) {
          cvWriter.addDocument(doc);
          cvWriter.commit();
        } else {
          trainingWriter.addDocument(doc);
          trainingWriter.commit();
        }
        b++;
      }
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
