package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

public class TestSerialization extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();

    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)));
    Document doc = new Document();
    doc.add(new Field("foo", "bar rab abr bra rba", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("moo", "bar rab abr bra rba", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    iw.addDocument(doc);
    iw.close();

    IndexReader ir = IndexReader.open(dir, false);
    InstantiatedIndex ii = new InstantiatedIndex(ir);
    ir.close();

    ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(ii);
    oos.close();
    baos.close();
    dir.close();
    
  }

}
