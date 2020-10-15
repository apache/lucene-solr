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

package org.apache.lucene.luke.models.overview;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class OverviewTestBase extends LuceneTestCase {

  IndexReader reader;

  Directory dir;

  Path indexDir;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    indexDir = createIndex();
    dir = newFSDirectory(indexDir);
    reader = DirectoryReader.open(dir);
  }

  private Path createIndex() throws IOException {
    Path indexDir = createTempDir();

    Directory dir = newFSDirectory(indexDir);
    IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
    config.setMergePolicy(NoMergePolicy.INSTANCE);  // see LUCENE-8998
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    Document doc1 = new Document();
    doc1.add(newStringField("f1", "1", Field.Store.NO));
    doc1.add(newTextField("f2", "a b c d e", Field.Store.NO));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(newStringField("f1", "2", Field.Store.NO));
    doc2.add(new TextField("f2", "a c", Field.Store.NO));
    writer.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(newStringField("f1", "3", Field.Store.NO));
    doc3.add(newTextField("f2", "a f", Field.Store.NO));
    writer.addDocument(doc3);

    Map<String, String> userData = new HashMap<>();
    userData.put("data", "val");
    writer.w.setLiveCommitData(userData.entrySet());

    writer.commit();

    writer.close();
    dir.close();

    return indexDir;
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    dir.close();
  }

}
