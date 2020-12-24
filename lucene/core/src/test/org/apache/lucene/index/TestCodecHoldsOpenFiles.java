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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestCodecHoldsOpenFiles extends LuceneTestCase {
  public void test() throws Exception {
    BaseDirectoryWrapper d = newDirectory();
    d.setCheckIndexOnClose(false);
    // we nuke files, but verify the reader still works
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newField("foo", "bar", TextField.TYPE_NOT_STORED));
      doc.add(new IntPoint("doc", i));
      doc.add(new IntPoint("doc2d", i, i));
      doc.add(new NumericDocValuesField("dv", i));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.commit();
    w.close();

    for (String name : d.listAll()) {
      d.deleteFile(name);
    }

    for (LeafReaderContext cxt : r.leaves()) {
      TestUtil.checkReader(cxt.reader());
    }

    r.close();
    d.close();
  }
}
