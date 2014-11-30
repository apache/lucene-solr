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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * 
 * @lucene.experimental
 */
public class TestOmitPositions extends LuceneTestCase {

  public void testBasic() throws Exception {   
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableHighlighting("foo");
    fieldTypes.setIndexOptions("foo", IndexOptions.DOCS_AND_FREQS);

    Document doc = w.newDocument();
    doc.addLargeText("foo", "this is a test test");
    for (int i = 0; i < 100; i++) {
      w.addDocument(doc);
    }
    
    IndexReader reader = w.getReader();
    w.close();
    
    assertNull(MultiFields.getTermPositionsEnum(reader, null, "foo", new BytesRef("test")));
    
    DocsEnum de = TestUtil.docs(random(), reader, "foo", new BytesRef("test"), null, null, DocsEnum.FLAG_FREQS);
    while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(2, de.freq());
    }
    
    reader.close();
    dir.close();
  }
}
