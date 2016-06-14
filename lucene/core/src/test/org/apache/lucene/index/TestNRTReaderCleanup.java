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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;

/** LUCENE-5574 */
@SuppressFileSystems("WindowsFS") // the bug doesn't happen on windows.
public class TestNRTReaderCleanup extends LuceneTestCase {

  public void testClosingNRTReaderDoesNotCorruptYourIndex() throws IOException {

    // Windows disallows deleting & overwriting files still
    // open for reading:
    assumeFalse("this test can't run on Windows", Constants.WINDOWS);

    MockDirectoryWrapper dir = newMockDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    LogMergePolicy lmp = new LogDocMergePolicy();
    lmp.setMergeFactor(2);
    iwc.setMergePolicy(lmp);

    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("a", "foo", Field.Store.NO));
    w.addDocument(doc);
    w.commit();
    w.addDocument(doc);

    // Get a new reader, but this also sets off a merge:
    IndexReader r = w.getReader();
    w.close();

    // Blow away index and make a new writer:
    for(String name : dir.listAll()) {
      dir.deleteFile(name);
    }

    w = new RandomIndexWriter(random(), dir);
    w.addDocument(doc);
    w.close();
    r.close();
    dir.close();
  }
}
