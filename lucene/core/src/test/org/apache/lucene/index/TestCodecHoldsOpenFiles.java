package org.apache.lucene.index;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestCodecHoldsOpenFiles extends LuceneTestCase {
  public void test() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    int numDocs = atLeast(100);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(newField("foo", "bar", TextField.TYPE_NOT_STORED));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    for(String fileName : d.listAll()) {
      try {
        d.deleteFile(fileName);
      } catch (IOException ioe) {
        // ignore: this means codec (correctly) is holding
        // the file open
      }
    }

    for(AtomicReaderContext cxt : r.leaves()) {
      _TestUtil.checkReader(cxt.reader());
    }

    r.close();
    d.close();
  }
}
