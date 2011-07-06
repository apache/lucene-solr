package org.apache.lucene.store.instantiated;

/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * @since 2009-mar-30 13:15:49
 */
public class TestUnoptimizedReaderOnConstructor extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocument(iw, "Hello, world!");
    addDocument(iw, "All work and no play makes jack a dull boy");
    iw.close();

    iw = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    addDocument(iw, "Hello, tellus!");
    addDocument(iw, "All work and no play makes danny a dull boy");
    iw.close();

    iw = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    addDocument(iw, "Hello, earth!");
    addDocument(iw, "All work and no play makes wendy a dull girl");
    iw.close();

    IndexReader unoptimizedReader = IndexReader.open(dir, false);
    unoptimizedReader.deleteDocument(2);

    try {
      new InstantiatedIndex(unoptimizedReader);
    } catch (Exception e) {
      e.printStackTrace(System.out);
      fail("No exceptions when loading an unoptimized reader!");
    }

    // todo some assertations.
    unoptimizedReader.close();
    dir.close();
  }

  private void addDocument(IndexWriter iw, String text) throws IOException {
    Document doc = new Document();
    doc.add(new Field("field", text, Field.Store.NO, Field.Index.ANALYZED));
    iw.addDocument(doc);
  }
}
