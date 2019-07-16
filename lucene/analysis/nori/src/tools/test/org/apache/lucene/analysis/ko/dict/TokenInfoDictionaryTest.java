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
package org.apache.lucene.analysis.ko.dict;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.analysis.ko.util.TokenInfoDictionaryBuilder;
import org.apache.lucene.analysis.ko.util.TokenInfoDictionaryWriter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

import static java.io.File.separatorChar;
import static org.apache.lucene.analysis.ko.dict.BinaryDictionary.ResourceScheme;

/**
 * Tests of TokenInfoDictionary build tools; run using ant test-tools
 */
public class TokenInfoDictionaryTest extends LuceneTestCase {

  public void testPut() throws Exception {
    TokenInfoDictionary dict = newDictionary("명사,1,1,2,NNG,*,*,*,*,*,*,*",
        // "large" id
        "일반,5000,5000,3,NNG,*,*,*,*,*,*,*");
    IntsRef wordIdRef = new IntsRefBuilder().get();

    dict.lookupWordIds(0, wordIdRef);
    int wordId = wordIdRef.ints[wordIdRef.offset];
    assertEquals(1, dict.getLeftId(wordId));
    assertEquals(1, dict.getRightId(wordId));
    assertEquals(2, dict.getWordCost(wordId));

    dict.lookupWordIds(1, wordIdRef);
    wordId = wordIdRef.ints[wordIdRef.offset];
    assertEquals(5000, dict.getLeftId(wordId));
    assertEquals(5000, dict.getRightId(wordId));
    assertEquals(3, dict.getWordCost(wordId));
  }

  private TokenInfoDictionary newDictionary(String... entries) throws Exception {
    Path dir = createTempDir();
    try (OutputStream out = Files.newOutputStream(dir.resolve("test.csv"));
         PrintWriter printer = new PrintWriter(new OutputStreamWriter(out, "utf-8"))) {
      for (String entry : entries) {
        printer.println(entry);
      }
    }
    TokenInfoDictionaryBuilder builder = new TokenInfoDictionaryBuilder("utf-8", true);
    TokenInfoDictionaryWriter writer = builder.build(dir.toString());
    writer.write(dir.toString());
    String dictionaryPath = TokenInfoDictionary.class.getName().replace('.', separatorChar);
    // We must also load the other files (in BinaryDictionary) from the correct path
    return new TokenInfoDictionary(ResourceScheme.FILE, dir.resolve(dictionaryPath).toString());
  }

  public void testPutException() throws Exception {
    // too few columns
    expectThrows(IllegalArgumentException.class, () -> newDictionary("HANGUL,1,1,1,NNG,*,*,*,*,*"));
    // id too large
    expectThrows(IllegalArgumentException.class, () -> newDictionary("HANGUL,8192,8192,1,NNG,*,*,*,*,*,*,*"));
  }
}
