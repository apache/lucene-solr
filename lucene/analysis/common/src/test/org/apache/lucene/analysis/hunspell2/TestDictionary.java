package org.apache.lucene.analysis.hunspell2;

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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestDictionary extends LuceneTestCase {

  public void testSimpleDictionary() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("simple.aff");
    InputStream dictStream = getClass().getResourceAsStream("simple.dic");

    Dictionary dictionary = new Dictionary(affixStream, dictStream);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).size());
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).size());
    char flags[] = dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3, new BytesRef());
    assertNotNull(flags);
    assertEquals(1, flags.length);
    assertEquals("Wrong number of flags for lucen", 1, dictionary.lookupWord(new char[]{'l', 'u', 'c', 'e', 'n'}, 0, 5, new BytesRef()).length);

    affixStream.close();
    dictStream.close();
  }

  public void testCompressedDictionary() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("compressed.aff");
    InputStream dictStream = getClass().getResourceAsStream("compressed.dic");

    Dictionary dictionary = new Dictionary(affixStream, dictStream);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).size());
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).size());
    assertEquals(1, dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3, new BytesRef()).length);
    
    affixStream.close();
    dictStream.close();
  }

  // malformed rule causes ParseException
  public void testInvalidData() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("broken.aff");
    InputStream dictStream = getClass().getResourceAsStream("simple.dic");
    
    try {
      new Dictionary(affixStream, dictStream);
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      assertEquals("The affix file contains a rule with less than five elements", expected.getMessage());
      assertEquals(23, expected.getErrorOffset());
    }
    
    affixStream.close();
    dictStream.close();
  }
  
  private class CloseCheckInputStream extends FilterInputStream {
    private boolean closed = false;

    public CloseCheckInputStream(InputStream delegate) {
      super(delegate);
    }

    @Override
    public void close() throws IOException {
      this.closed = true;
      super.close();
    }
    
    public boolean isClosed() {
      return this.closed;
    }
  }
  
  public void testResourceCleanup() throws Exception {
    CloseCheckInputStream affixStream = new CloseCheckInputStream(getClass().getResourceAsStream("compressed.aff"));
    CloseCheckInputStream dictStream = new CloseCheckInputStream(getClass().getResourceAsStream("compressed.dic"));
    
    new Dictionary(affixStream, dictStream);
    
    assertFalse(affixStream.isClosed());
    assertFalse(dictStream.isClosed());
    
    affixStream.close();
    dictStream.close();
    
    assertTrue(affixStream.isClosed());
    assertTrue(dictStream.isClosed());
  }
}
