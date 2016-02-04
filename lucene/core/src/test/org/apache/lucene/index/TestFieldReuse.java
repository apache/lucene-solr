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
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.NumericTokenStream.NumericTermAttribute;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

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
    TokenStream bogus = new NumericTokenStream();
    ts = stringField.tokenStream(null, bogus);
    assertNotSame(ts, bogus);
    assertTokenStreamContents(ts, 
        new String[] { "beer" },
        new int[]    { 0 },
        new int[]    { 4 }
    );
  }
  
  public void testNumericReuse() throws IOException {
    IntField intField = new IntField("foo", 5, Field.Store.NO);
    
    // passing null
    TokenStream ts = intField.tokenStream(null, null);
    assertTrue(ts instanceof NumericTokenStream);
    assertEquals(NumericUtils.PRECISION_STEP_DEFAULT_32, ((NumericTokenStream)ts).getPrecisionStep());
    assertNumericContents(5, ts);

    // now reuse previous stream
    intField = new IntField("foo", 20, Field.Store.NO);
    TokenStream ts2 = intField.tokenStream(null, ts);
    assertSame(ts, ts2);
    assertNumericContents(20, ts);
    
    // pass a bogus stream and ensure it's still ok
    intField = new IntField("foo", 2343, Field.Store.NO);
    TokenStream bogus = new CannedTokenStream(new Token("bogus", 0, 5));
    ts = intField.tokenStream(null, bogus);
    assertNotSame(bogus, ts);
    assertNumericContents(2343, ts);
    
    // pass another bogus stream (numeric, but different precision step!)
    intField = new IntField("foo", 42, Field.Store.NO);
    assert 3 != NumericUtils.PRECISION_STEP_DEFAULT;
    bogus = new NumericTokenStream(3);
    ts = intField.tokenStream(null, bogus);
    assertNotSame(bogus, ts);
    assertNumericContents(42, ts);
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
    public float boost() {
      return 1;
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
  
  private void assertNumericContents(int value, TokenStream ts) throws IOException {
    assertTrue(ts instanceof NumericTokenStream);
    NumericTermAttribute numericAtt = ts.getAttribute(NumericTermAttribute.class);
    ts.reset();
    boolean seen = false;
    while (ts.incrementToken()) {
      if (numericAtt.getShift() == 0) {
        assertEquals(value, numericAtt.getRawValue());
        seen = true;
      }
    }
    ts.end();
    ts.close();
    assertTrue(seen);
  }
}
