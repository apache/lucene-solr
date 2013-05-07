package org.apache.lucene.store;

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

import java.io.File;

import org.apache.lucene.document.Field;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;

public class TestWindowsMMap extends LuceneTestCase {
  
  private final static String alphabet = "abcdefghijklmnopqrstuvwzyz";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  private String randomToken() {
    int tl = 1 + random().nextInt(7);
    StringBuilder sb = new StringBuilder();
    for(int cx = 0; cx < tl; cx ++) {
      int c = random().nextInt(25);
      sb.append(alphabet.substring(c, c+1));
    }
    return sb.toString();
  }
  
  private String randomField() {
    int fl = 1 + random().nextInt(3);
    StringBuilder fb = new StringBuilder();
    for(int fx = 0; fx < fl; fx ++) {
      fb.append(randomToken());
      fb.append(" ");
    }
    return fb.toString();
  }
  
  public void testMmapIndex() throws Exception {
    // sometimes the directory is not cleaned by rmDir, because on Windows it
    // may take some time until the files are finally dereferenced. So clean the
    // directory up front, or otherwise new IndexWriter will fail.
    File dirPath = _TestUtil.getTempDir("testLuceneMmap");
    rmDir(dirPath);
    MMapDirectory dir = new MMapDirectory(dirPath, null);
    
    // plan to add a set of useful stopwords, consider changing some of the
    // interior filters.
    MockAnalyzer analyzer = new MockAnalyzer(random());
    // TODO: something about lock timeouts and leftover locks.
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer)
        .setOpenMode(OpenMode.CREATE));
    writer.commit();
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    
    int num = atLeast(1000);
    for(int dx = 0; dx < num; dx ++) {
      String f = randomField();
      Document doc = new Document();
      doc.add(newTextField("data", f, Field.Store.YES));  
      writer.addDocument(doc);
    }
    
    reader.close();
    writer.close();
    rmDir(dirPath);
  }

  private void rmDir(File dir) {
    if (!dir.exists()) {
      return;
    }
    for (File file : dir.listFiles()) {
      file.delete();
    }
    dir.delete();
  }
}
