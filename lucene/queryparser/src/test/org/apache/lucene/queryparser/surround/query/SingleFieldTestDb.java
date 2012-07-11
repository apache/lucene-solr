package org.apache.lucene.queryparser.surround.query;

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

import java.util.Random;

import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

public class SingleFieldTestDb {
  private Directory db;
  private String[] docs;
  private String fieldName;
  
  public SingleFieldTestDb(Random random, String[] documents, String fName) {
    try {
      db = new MockDirectoryWrapper(random, new RAMDirectory());
      docs = documents;
      fieldName = fName;
      IndexWriter writer = new IndexWriter(db, new IndexWriterConfig(
          Version.LUCENE_CURRENT,
          new MockAnalyzer(random)));
      for (int j = 0; j < docs.length; j++) {
        Document d = new Document();
        d.add(new TextField(fieldName, docs[j], Field.Store.NO));
        writer.addDocument(d);
      }
      writer.close();
    } catch (java.io.IOException ioe) {
      throw new Error(ioe);
    }
  }
  
  Directory getDb() {return db;}
  String[] getDocs() {return docs;}
  String getFieldname() {return fieldName;}
}


