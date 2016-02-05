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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;

/**
 * Tests that a useful exception is thrown when attempting to index a term that is 
 * too large
 *
 * @see IndexWriter#MAX_TERM_LENGTH
 */
public class TestExceedMaxTermLength extends LuceneTestCase {

  private final static int minTestTermLength = IndexWriter.MAX_TERM_LENGTH + 1;
  private final static int maxTestTermLegnth = IndexWriter.MAX_TERM_LENGTH * 2;

  Directory dir = null;

  @Before
  public void createDir() {
    dir = newDirectory();
  }
  @After
  public void destroyDir() throws IOException {
    dir.close();
    dir = null;
  }

  public void test() throws Exception {
    
    IndexWriter w = new IndexWriter
      (dir, newIndexWriterConfig(random(), new MockAnalyzer(random())));
    try {
      final FieldType ft = new FieldType();
      ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      ft.setStored(random().nextBoolean());
      ft.freeze();
      
      final Document doc = new Document();
      if (random().nextBoolean()) {
        // totally ok short field value
        doc.add(new Field(TestUtil.randomSimpleString(random(), 1, 10),
                          TestUtil.randomSimpleString(random(), 1, 10),
                          ft));
      }
      // problematic field
      final String name = TestUtil.randomSimpleString(random(), 1, 50);
      final String value = TestUtil.randomSimpleString(random(),
                                                       minTestTermLength,
                                                       maxTestTermLegnth);
      final Field f = new Field(name, value, ft);
      if (random().nextBoolean()) {
        // totally ok short field value
        doc.add(new Field(TestUtil.randomSimpleString(random(), 1, 10),
                          TestUtil.randomSimpleString(random(), 1, 10),
                          ft));
      }
      doc.add(f);
      
      try {
        w.addDocument(doc);
        fail("Did not get an exception from adding a monster term");
      } catch (IllegalArgumentException e) {
        final String maxLengthMsg = String.valueOf(IndexWriter.MAX_TERM_LENGTH);
        final String msg = e.getMessage();
        assertTrue("IllegalArgumentException didn't mention 'immense term': " + msg,
                   msg.contains("immense term"));
        assertTrue("IllegalArgumentException didn't mention max length ("+maxLengthMsg+"): " + msg,
                   msg.contains(maxLengthMsg));
        assertTrue("IllegalArgumentException didn't mention field name ("+name+"): " + msg,
                   msg.contains(name));
        assertTrue("IllegalArgumentException didn't mention original message: " + msg,
            msg.contains("bytes can be at most") && msg.contains("in length; got"));
      }
    } finally {
      w.close();
    }
  }
}
