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

import java.io.*;
import java.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;

public class TestFlexExternalReader extends LuceneTestCase {

  public void testExternalReader() throws Exception {
    Directory d = new MockRAMDirectory();

    final int DOC_COUNT = 177;

    IndexWriter w = new IndexWriter(d, new WhitespaceAnalyzer(),
                                    IndexWriter.MaxFieldLength.UNLIMITED);
    w.setMaxBufferedDocs(7);
    Document doc = new Document();
    doc.add(new Field("field1", "this is field1", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("field2", "this is field2", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("field3", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("field4", "bbb", Field.Store.NO, Field.Index.ANALYZED));
    for(int i=0;i<DOC_COUNT;i++) {
      w.addDocument(doc);
    }

    IndexReader r = new FlexTestUtil.ForcedExternalReader(w.getReader());

    BytesRef field1Term = new BytesRef("field1");
    BytesRef field2Term = new BytesRef("field2");

    assertEquals(DOC_COUNT, r.maxDoc());
    assertEquals(DOC_COUNT, r.numDocs());
    assertEquals(DOC_COUNT, r.docFreq(new Term("field1", "field1")));
    assertEquals(DOC_COUNT, r.docFreq("field1", field1Term));

    Fields fields = r.fields();
    Terms terms = fields.terms("field1");
    TermsEnum termsEnum = terms.iterator();
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seek(field1Term));

    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, termsEnum.seek(field2Term));
    assertTrue(new BytesRef("is").bytesEquals(termsEnum.term()));

    terms = fields.terms("field2");
    termsEnum = terms.iterator();
    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, termsEnum.seek(field1Term));
    assertTrue(termsEnum.term().bytesEquals(field2Term));

    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seek(field2Term));

    termsEnum = fields.terms("field3").iterator();
    assertEquals(TermsEnum.SeekStatus.END, termsEnum.seek(new BytesRef("bbb")));

    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seek(new BytesRef("aaa")));
    assertNull(termsEnum.next());

    r.close();
    w.close();
    d.close();
  }
}
