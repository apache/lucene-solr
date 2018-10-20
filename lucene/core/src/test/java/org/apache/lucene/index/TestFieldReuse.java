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
import java.io.Reader;
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/** test tokenstream reuse by DefaultIndexingChain */
public class TestFieldReuse extends BaseTokenStreamTestCase {
  
  public void testStringField() throws IOException {
    StringField stringField = new StringField("foo", "bar", Field.Store.NO);
    
    // passing null
    TokenStream ts = stringField.tokenStream(null, null);
    assertTokenStreamContents(ts, 
        new String[] { "bar" },
        new int[]    { 0 },
        new int[]    { 3 }
    );
    
    // now reuse previous stream
    stringField = new StringField("foo", "baz", Field.Store.NO);
    TokenStream ts2 = stringField.tokenStream(null, ts);
    assertSame(ts, ts);
    assertTokenStreamContents(ts, 
        new String[] { "baz" },
        new int[]    { 0 },
        new int[]    { 3 }
    );
    
    // pass a bogus stream and ensure it's still ok
    stringField = new StringField("foo", "beer", Field.Store.NO);
    TokenStream bogus = new CannedTokenStream();
    ts = stringField.tokenStream(null, bogus);
    assertNotSame(ts, bogus);
    assertTokenStreamContents(ts, 
        new String[] { "beer" },
        new int[]    { 0 },
        new int[]    { 4 }
    );
  }
  
  static class MyField implements IndexableField {
    TokenStream lastSeen;
    TokenStream lastReturned;
    
    @Override
    public String name() {
      return "foo";
    }
    
    @Override
    public IndexableFieldType fieldType() {
      return StringField.TYPE_NOT_STORED;
    }
    
    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
      lastSeen = reuse;
      return lastReturned = new CannedTokenStream(new Token("unimportant", 0, 10));
    }

    @Override
    public BytesRef binaryValue() {
      return null;
    }

    @Override
    public String stringValue() {
      return null;
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public Number numericValue() {
      return null;
    } 
  }
  
  public void testIndexWriterActuallyReuses() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    final MyField field1 = new MyField();
    iw.addDocument(Collections.singletonList(field1));
    TokenStream previous = field1.lastReturned;
    assertNotNull(previous);
    
    final MyField field2 = new MyField();
    iw.addDocument(Collections.singletonList(field2));
    assertSame(previous, field2.lastSeen);
    iw.close();
    dir.close();
  }
}
