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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;
import org.junit.Test;

public class TestDictionary extends LuceneTestCase {

  public void testSimpleDictionary() throws Exception {
    Dictionary dictionary = loadDictionary("simple.aff", "simple.dic");
    assertEquals(3, dictionary.lookupSuffix(new char[] {'e'}).length);
    assertEquals(1, dictionary.lookupPrefix(new char[] {'s'}).length);
    IntsRef ordList = dictionary.lookupWord(new char[] {'o', 'l', 'r'}, 0, 3);
    assertNotNull(ordList);
    assertEquals(1, ordList.length);

    assertEquals('B', assertSingleFlag(dictionary, ordList));

    int offset = random().nextInt(10);
    ordList = dictionary.lookupWord((" ".repeat(offset) + "lucen").toCharArray(), offset, 5);
    assertNotNull(ordList);
    assertEquals(1, ordList.length);
    assertEquals('A', assertSingleFlag(dictionary, ordList));
  }

  private static char assertSingleFlag(Dictionary dictionary, IntsRef ordList) {
    int entryId = ordList.ints[0];
    char[] flags = dictionary.flagLookup.getFlags(entryId);
    assertEquals(1, flags.length);
    return flags[0];
  }

  public void testCompressedDictionary() throws Exception {
    Dictionary dictionary = loadDictionary("compressed.aff", "compressed.dic");
    assertEquals(3, dictionary.lookupSuffix(new char[] {'e'}).length);
    assertEquals(1, dictionary.lookupPrefix(new char[] {'s'}).length);
    IntsRef ordList = dictionary.lookupWord(new char[] {'o', 'l', 'r'}, 0, 3);
    assertSingleFlag(dictionary, ordList);
  }

  public void testCompressedBeforeSetDictionary() throws Exception {
    Dictionary dictionary = loadDictionary("compressed-before-set.aff", "compressed.dic");
    assertEquals(3, dictionary.lookupSuffix(new char[] {'e'}).length);
    assertEquals(1, dictionary.lookupPrefix(new char[] {'s'}).length);
    IntsRef ordList = dictionary.lookupWord(new char[] {'o', 'l', 'r'}, 0, 3);
    assertSingleFlag(dictionary, ordList);
  }

  public void testCompressedEmptyAliasDictionary() throws Exception {
    Dictionary dictionary = loadDictionary("compressed-empty-alias.aff", "compressed.dic");
    assertEquals(3, dictionary.lookupSuffix(new char[] {'e'}).length);
    assertEquals(1, dictionary.lookupPrefix(new char[] {'s'}).length);
    IntsRef ordList = dictionary.lookupWord(new char[] {'o', 'l', 'r'}, 0, 3);
    assertSingleFlag(dictionary, ordList);
  }

  // malformed rule causes ParseException
  public void testInvalidData() {
    ParseException expected =
        expectThrows(ParseException.class, () -> loadDictionary("broken.aff", "simple.dic"));
    assertTrue(expected.getMessage().startsWith("Invalid syntax"));
    assertEquals(24, expected.getErrorOffset());
  }

  public void testUsingFlagsBeforeFlagDirective() throws IOException, ParseException {
    byte[] aff = "KEEPCASE 42\nFLAG num".getBytes(StandardCharsets.UTF_8);
    byte[] dic = "1\nfoo/42".getBytes(StandardCharsets.UTF_8);

    Dictionary dictionary =
        new Dictionary(
            new ByteBuffersDirectory(),
            "",
            new ByteArrayInputStream(aff),
            new ByteArrayInputStream(dic));

    assertEquals(42, dictionary.keepcase);
  }

  public void testForgivableErrors() throws Exception {
    Dictionary dictionary = loadDictionary("forgivable-errors.aff", "forgivable-errors.dic");
    assertEquals(1, dictionary.repTable.size());
    assertEquals(2, dictionary.compoundMax);

    loadDictionary("forgivable-errors-long.aff", "single-word.dic");
    loadDictionary("forgivable-errors-num.aff", "single-word.dic");
  }

  private Dictionary loadDictionary(String aff, String dic) throws IOException, ParseException {
    try (InputStream affixStream = getClass().getResourceAsStream(aff);
        InputStream dicStream = getClass().getResourceAsStream(dic);
        Directory tempDir = getDirectory()) {
      return new Dictionary(tempDir, "dictionary", affixStream, dicStream);
    }
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
    CloseCheckInputStream affixStream =
        new CloseCheckInputStream(getClass().getResourceAsStream("compressed.aff"));
    CloseCheckInputStream dictStream =
        new CloseCheckInputStream(getClass().getResourceAsStream("compressed.dic"));
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
    FSTCompiler<CharsRef> fstCompiler = new FSTCompiler<>(FST.INPUT_TYPE.BYTE2, outputs);
    IntsRefBuilder scratchInts = new IntsRefBuilder();

    // a -> b
    Util.toUTF16("a", scratchInts);
    fstCompiler.add(scratchInts.get(), new CharsRef("b"));

    // ab -> c
    Util.toUTF16("ab", scratchInts);
    fstCompiler.add(scratchInts.get(), new CharsRef("c"));

    // c -> de
    Util.toUTF16("c", scratchInts);
    fstCompiler.add(scratchInts.get(), new CharsRef("de"));

    // def -> gh
    Util.toUTF16("def", scratchInts);
    fstCompiler.add(scratchInts.get(), new CharsRef("gh"));

    FST<CharsRef> fst = fstCompiler.compile();

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
    assertEquals("UTF-8", getDictionaryEncoding("SET\tUTF-8\n"));
    assertEquals("UTF-8", getDictionaryEncoding("SET\t UTF-8\n"));
    assertEquals("UTF-8", getDictionaryEncoding("\uFEFFSET\tUTF-8\n"));
    assertEquals("UTF-8", getDictionaryEncoding("\uFEFFSET\tUTF-8\r\n"));
    assertEquals(Dictionary.DEFAULT_CHARSET.name(), getDictionaryEncoding(""));
  }

  private static String getDictionaryEncoding(String affFile) throws IOException, ParseException {
    Dictionary dictionary =
        new Dictionary(
            new ByteBuffersDirectory(),
            "",
            new ByteArrayInputStream(affFile.getBytes(StandardCharsets.UTF_8)),
            new ByteArrayInputStream("1\nmock".getBytes(StandardCharsets.UTF_8)));
    return dictionary.decoder.charset().name();
  }

  public void testFlagWithCrazyWhitespace() {
    assertNotNull(Dictionary.getFlagParsingStrategy("FLAG\tUTF-8", StandardCharsets.UTF_8));
    assertNotNull(Dictionary.getFlagParsingStrategy("FLAG    UTF-8", StandardCharsets.UTF_8));
  }

  @Test
  public void testUtf8Flag() {
    Dictionary.FlagParsingStrategy strategy =
        Dictionary.getFlagParsingStrategy("FLAG\tUTF-8", Dictionary.DEFAULT_CHARSET);

    String src = "привет";
    String asAscii = new String(src.getBytes(StandardCharsets.UTF_8), Dictionary.DEFAULT_CHARSET);
    assertNotEquals(src, asAscii);
    assertEquals(src, new String(strategy.parseFlags(asAscii)));
  }

  private Directory getDirectory() {
    return newDirectory();
  }
}
