package org.apache.lucene.index;

/**
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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Back-compat test that you can seek to a lead surrogate in the term
 * dictionary. With the old lucene API, this worked, due to the fact that the
 * Term itself did not need to be converted into proper UTF-8 bytes.
 * 
 * With the new API the provided Term text must be encodeable into UTF-8.
 * 
 * @deprecated Remove this when the old API is no longer supported.
 */
@Deprecated
public class TestTermEnumSurrogate extends LuceneTestCase {
  public void testSeekSurrogate() throws Exception {
    RAMDirectory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new MockAnalyzer(),
        IndexWriter.MaxFieldLength.UNLIMITED);
    Document d = new Document();
    Field f = new Field("field", "", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f);
    f.setValue("abacadaba");
    writer.addDocument(d);
    f.setValue("𩬅");
    writer.addDocument(d);
    writer.close();
    IndexReader reader = IndexReader.open(dir, true);
    TermEnum te = reader.terms(new Term("field", "𩬅".substring(0, 1)));
    assertEquals(new Term("field", "𩬅"), te.term());
  }
}
