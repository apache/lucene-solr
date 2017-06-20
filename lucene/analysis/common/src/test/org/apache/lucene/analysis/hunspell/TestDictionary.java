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
package org.apache.lucene.analysis.hunspell;


import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

public class TestDictionary extends LuceneTestCase {

  public void testSimpleDictionary() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("simple.aff");
    InputStream dictStream = getClass().getResourceAsStream("simple.dic");
    Directory tempDir = getDirectory();

    Dictionary dictionary = new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).length);
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).length);
    IntsRef ordList = dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3);
    assertNotNull(ordList);
    assertEquals(1, ordList.length);
    
    BytesRef ref = new BytesRef();
    dictionary.flagLookup.get(ordList.ints[0], ref);
    char flags[] = Dictionary.decodeFlags(ref);
    assertEquals(1, flags.length);
    
    ordList = dictionary.lookupWord(new char[]{'l', 'u', 'c', 'e', 'n'}, 0, 5);
    assertNotNull(ordList);
    assertEquals(1, ordList.length);
    dictionary.flagLookup.get(ordList.ints[0], ref);
    flags = Dictionary.decodeFlags(ref);
    assertEquals(1, flags.length);
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
  }

  public void testCompressedDictionary() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("compressed.aff");
    InputStream dictStream = getClass().getResourceAsStream("compressed.dic");

    Directory tempDir = getDirectory();
    Dictionary dictionary = new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).length);
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).length);
    IntsRef ordList = dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3);
    BytesRef ref = new BytesRef();
    dictionary.flagLookup.get(ordList.ints[0], ref);
    char flags[] = Dictionary.decodeFlags(ref);
    assertEquals(1, flags.length);
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
  }
  
  public void testCompressedBeforeSetDictionary() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("compressed-before-set.aff");
    InputStream dictStream = getClass().getResourceAsStream("compressed.dic");
    Directory tempDir = getDirectory();

    Dictionary dictionary = new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).length);
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).length);
    IntsRef ordList = dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3);
    BytesRef ref = new BytesRef();
    dictionary.flagLookup.get(ordList.ints[0], ref);
    char flags[] = Dictionary.decodeFlags(ref);
    assertEquals(1, flags.length);
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
  }
  
  public void testCompressedEmptyAliasDictionary() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("compressed-empty-alias.aff");
    InputStream dictStream = getClass().getResourceAsStream("compressed.dic");
    Directory tempDir = getDirectory();

    Dictionary dictionary = new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    assertEquals(3, dictionary.lookupSuffix(new char[]{'e'}, 0, 1).length);
    assertEquals(1, dictionary.lookupPrefix(new char[]{'s'}, 0, 1).length);
    IntsRef ordList = dictionary.lookupWord(new char[]{'o', 'l', 'r'}, 0, 3);
    BytesRef ref = new BytesRef();
    dictionary.flagLookup.get(ordList.ints[0], ref);
    char flags[] = Dictionary.decodeFlags(ref);
    assertEquals(1, flags.length);
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
  }

  // malformed rule causes ParseException
  public void testInvalidData() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("broken.aff");
    InputStream dictStream = getClass().getResourceAsStream("simple.dic");
    Directory tempDir = getDirectory();
    
    ParseException expected = expectThrows(ParseException.class, () -> {
      new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    });
    assertTrue(expected.getMessage().startsWith("The affix file contains a rule with less than four elements"));
    assertEquals(24, expected.getErrorOffset());
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
  }
  
  // malformed flags causes ParseException
  public void testInvalidFlags() throws Exception {
    InputStream affixStream = getClass().getResourceAsStream("broken-flags.aff");
    InputStream dictStream = getClass().getResourceAsStream("simple.dic");
    Directory tempDir = getDirectory();
    
    Exception expected = expectThrows(Exception.class, () -> {
      new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    });
    assertTrue(expected.getMessage().startsWith("expected only one flag"));
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
  }
  
  private static class CloseCheckInputStream extends FilterInputStream {
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
    Directory tempDir = getDirectory();
    
    new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    
    assertFalse(affixStream.isClosed());
    assertFalse(dictStream.isClosed());
    
    affixStream.close();
    dictStream.close();
    tempDir.close();
    
    assertTrue(affixStream.isClosed());
    assertTrue(dictStream.isClosed());
  }
  
  
  public void testReplacements() throws Exception {
    Outputs<CharsRef> outputs = CharSequenceOutputs.getSingleton();
    Builder<CharsRef> builder = new Builder<>(FST.INPUT_TYPE.BYTE2, outputs);
    IntsRefBuilder scratchInts = new IntsRefBuilder();
    
    // a -> b
    Util.toUTF16("a", scratchInts);
    builder.add(scratchInts.get(), new CharsRef("b"));
    
    // ab -> c
    Util.toUTF16("ab", scratchInts);
    builder.add(scratchInts.get(), new CharsRef("c"));
    
    // c -> de
    Util.toUTF16("c", scratchInts);
    builder.add(scratchInts.get(), new CharsRef("de"));
    
    // def -> gh
    Util.toUTF16("def", scratchInts);
    builder.add(scratchInts.get(), new CharsRef("gh"));
    
    FST<CharsRef> fst = builder.finish();
    
    StringBuilder sb = new StringBuilder("atestanother");
    Dictionary.applyMappings(fst, sb);
    assertEquals("btestbnother", sb.toString());
    
    sb = new StringBuilder("abtestanother");
    Dictionary.applyMappings(fst, sb);
    assertEquals("ctestbnother", sb.toString());
    
    sb = new StringBuilder("atestabnother");
    Dictionary.applyMappings(fst, sb);
    assertEquals("btestcnother", sb.toString());
    
    sb = new StringBuilder("abtestabnother");
    Dictionary.applyMappings(fst, sb);
    assertEquals("ctestcnother", sb.toString());
    
    sb = new StringBuilder("abtestabcnother");
    Dictionary.applyMappings(fst, sb);
    assertEquals("ctestcdenother", sb.toString());
    
    sb = new StringBuilder("defdefdefc");
    Dictionary.applyMappings(fst, sb);
    assertEquals("ghghghde", sb.toString());
  }
  
  public void testSetWithCrazyWhitespaceAndBOMs() throws Exception {
    assertEquals("UTF-8", Dictionary.getDictionaryEncoding(new ByteArrayInputStream("SET\tUTF-8\n".getBytes(StandardCharsets.UTF_8))));
    assertEquals("UTF-8", Dictionary.getDictionaryEncoding(new ByteArrayInputStream("SET\t UTF-8\n".getBytes(StandardCharsets.UTF_8))));
    assertEquals("UTF-8", Dictionary.getDictionaryEncoding(new ByteArrayInputStream("\uFEFFSET\tUTF-8\n".getBytes(StandardCharsets.UTF_8))));
    assertEquals("UTF-8", Dictionary.getDictionaryEncoding(new ByteArrayInputStream("\uFEFFSET\tUTF-8\r\n".getBytes(StandardCharsets.UTF_8))));
  }
  
  public void testFlagWithCrazyWhitespace() throws Exception {
    assertNotNull(Dictionary.getFlagParsingStrategy("FLAG\tUTF-8"));
    assertNotNull(Dictionary.getFlagParsingStrategy("FLAG    UTF-8"));
  }

  private Directory getDirectory() {
    return newDirectory();
  }
}
