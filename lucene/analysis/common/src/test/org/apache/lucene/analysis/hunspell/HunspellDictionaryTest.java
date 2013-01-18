package org.apache.lucene.analysis.hunspell;

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
import java.io.InputStream;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;

public class HunspellDictionaryTest extends LuceneTestCase {
  
  private class CloseCheckInputStream extends InputStream {
    private InputStream delegate;
    
    private boolean closed = false;

    public CloseCheckInputStream(InputStream delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return delegate.read(b);
    }

    @Override
    public boolean equals(Object obj) {
      return delegate.equals(obj);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      return delegate.skip(n);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public void close() throws IOException {
      this.closed = true;
      delegate.close();
    }

    @Override
    public void mark(int readlimit) {
      delegate.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
      delegate.reset();
    }

    @Override
    public boolean markSupported() {
      return delegate.markSupported();
    }
    
    public boolean isClosed() {
      return this.closed;
    }
    
  }

  @Test
  public void testResourceCleanup() throws IOException, ParseException {
    CloseCheckInputStream affixStream = new CloseCheckInputStream(getClass().getResourceAsStream("testCompressed.aff"));
    CloseCheckInputStream dictStream = new CloseCheckInputStream(getClass().getResourceAsStream("testCompressed.dic"));
    
    new HunspellDictionary(affixStream, dictStream, TEST_VERSION_CURRENT);
    
    assertFalse(affixStream.isClosed());
    assertFalse(dictStream.isClosed());
    
    affixStream.close();
    dictStream.close();
    
    assertTrue(affixStream.isClosed());
    assertTrue(dictStream.isClosed());
  }

  @Test
  public void testHunspellDictionary_loadDicAff() throws IOException, ParseException {
    InputStream affixStream = getClass().getResourceAsStream("test.aff");
    InputStream dictStream = getClass().getResourceAsStream("test.dic");

    HunspellDictionary dictionary = new HunspellDictionary(affixStream, dictStream, TEST_VERSION_CURRENT);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).size());
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).size());
    assertEquals(1, dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3).size());
    
    affixStream.close();
    dictStream.close();
  }

  @Test
  public void testCompressedHunspellDictionary_loadDicAff() throws IOException, ParseException {
    InputStream affixStream = getClass().getResourceAsStream("testCompressed.aff");
    InputStream dictStream = getClass().getResourceAsStream("testCompressed.dic");

    HunspellDictionary dictionary = new HunspellDictionary(affixStream, dictStream, TEST_VERSION_CURRENT);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).size());
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).size());
    assertEquals(1, dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3).size());
    
    affixStream.close();
    dictStream.close();
  }

  @Test
  public void testHunspellDictionary_loadDicWrongAff() throws IOException, ParseException {
    InputStream affixStream = getClass().getResourceAsStream("testWrongAffixRule.aff");
    InputStream dictStream = getClass().getResourceAsStream("test.dic");

    HunspellDictionary dictionary = new HunspellDictionary(affixStream, Arrays.asList(dictStream), TEST_VERSION_CURRENT, false, false);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).size());
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).size());
    assertEquals(1, dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3).size());
    //strict parsing disabled: malformed rule is not loaded
    assertNull(dictionary.lookupPrefix(new char[]{'a'}, 0, 1));    
    affixStream.close();
    dictStream.close();

    affixStream = getClass().getResourceAsStream("testWrongAffixRule.aff");
    dictStream = getClass().getResourceAsStream("test.dic");
    //strict parsing enabled: malformed rule causes ParseException
    try {
      dictionary = new HunspellDictionary(affixStream, Arrays.asList(dictStream), TEST_VERSION_CURRENT, false, true);
      Assert.fail();
    } catch(ParseException e) {
      Assert.assertEquals("The affix file contains a rule with less than five elements", e.getMessage());
      Assert.assertEquals(23, e.getErrorOffset());
    }
    
    affixStream.close();
    dictStream.close();
  }
}
