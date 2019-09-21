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
package org.apache.lucene.analysis.miscellaneous;


import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

public class TestEmptyTokenStream extends BaseTokenStreamTestCase {

  public void testConsume() throws IOException {
    TokenStream ts = new EmptyTokenStream();
    ts.reset();
    assertFalse(ts.incrementToken());
    ts.end();
    ts.close();
    // try again with reuse:
    ts.reset();
    assertFalse(ts.incrementToken());
    ts.end();
    ts.close();
  }
  
  public void testConsume2() throws IOException {
    BaseTokenStreamTestCase.assertTokenStreamContents(new EmptyTokenStream(), new String[0]);
  }

  public void testIndexWriter_LUCENE4656() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(null));

    TokenStream ts = new EmptyTokenStream();
    assertFalse(ts.hasAttribute(TermToBytesRefAttribute.class));

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.YES));
    doc.add(new TextField("description", ts));
    
    // this should not fail because we have no TermToBytesRefAttribute
    writer.addDocument(doc);
    
    assertEquals(1, writer.getDocStats().numDocs);

    writer.close();
    directory.close();
  }

}
