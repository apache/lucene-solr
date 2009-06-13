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

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * @since 2009-mar-30 13:15:49
 */
public class TestUnoptimizedReaderOnConstructor extends TestCase {

  public void test() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
    addDocument(iw, "Hello, world!");
    addDocument(iw, "All work and no play makes jack a dull boy");
    iw.close();

    iw = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.UNLIMITED);
    addDocument(iw, "Hello, tellus!");
    addDocument(iw, "All work and no play makes danny a dull boy");
    iw.close();

    iw = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.UNLIMITED);
    addDocument(iw, "Hello, earth!");
    addDocument(iw, "All work and no play makes wendy a dull girl");
    iw.close();

    IndexReader unoptimizedReader = IndexReader.open(dir);
    unoptimizedReader.deleteDocument(2);

    InstantiatedIndex ii;
    try {
     ii = new InstantiatedIndex(unoptimizedReader);
    } catch (Exception e) {
      fail("No exceptions when loading an unoptimized reader!");
    }

    // todo some assertations.

  }

  private void addDocument(IndexWriter iw, String text) throws IOException {
    Document doc = new Document();
    doc.add(new Field("field", text, Field.Store.NO, Field.Index.ANALYZED));
    iw.addDocument(doc);
  }
}
