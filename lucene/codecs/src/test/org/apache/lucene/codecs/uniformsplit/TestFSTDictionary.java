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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests {@link FSTDictionary}.
 */
public class TestFSTDictionary extends LuceneTestCase {

  public void testEmptyTermSupported() {
    FSTDictionary indexDictionary = createFSTDictionary(Collections.singletonList(new BytesRef()), new int[]{588});
    assertEquals(588, indexDictionary.browser().seekBlock(new BytesRef()));
  }

  public void testRepeatedTermNotAllowed() {
    for (BytesRef term : new BytesRef[] {new BytesRef(), new BytesRef("a")}) {
      try {
        createFSTDictionary(Arrays.asList(term, term), new int[]{0, 1});
        fail("Expected exception not thrown");
      } catch (Exception e) {
        assertSame(UnsupportedOperationException.class, e.getClass());
      }
    }
  }

  public void testRepeatedOutputAllowed() {
    BytesRef[] terms = {new BytesRef("a"), new BytesRef("b")};
    FSTDictionary indexDictionary = createFSTDictionary(Arrays.asList(terms), new int[]{588, 588});
    assertEquals(588, indexDictionary.browser().seekBlock(new BytesRef("a")));
    assertEquals(588, indexDictionary.browser().seekBlock(new BytesRef("b")));
  }

  public void testSerialization() throws IOException {
    List<String> vocab = Arrays.asList(
        "aswoon",
        "asyl",
        "asyla",
        "asyllabic");

    for (boolean shouldEncode : new boolean[] {false, true}) {

      FSTDictionary srcDictionary = createFSTDictionary(vocab);
      FSTDictionary fstDictionary = serializeAndReadDictionary(srcDictionary, shouldEncode);
      assertNotSame(srcDictionary, fstDictionary);
      assertEquals(-1L, fstDictionary.browser().seekBlock(new BytesRef()));
      assertNotSame(-1L, fstDictionary.browser().seekBlock(new BytesRef("aswoon")));
      assertNotSame(-1L, fstDictionary.browser().seekBlock(new BytesRef("z")));
    }
  }
  public void testSerializationEmptyTerm() throws IOException {
    for (boolean shouldEncode : new boolean[] {false, true}) {

      FSTDictionary srcDictionary = createFSTDictionary(Collections.singletonList(new BytesRef()), new int[1]);
      FSTDictionary fstDictionary = serializeAndReadDictionary(srcDictionary, shouldEncode);
      assertNotSame(srcDictionary, fstDictionary);
      assertEquals(0, fstDictionary.browser().seekBlock(new BytesRef()));
    }
  }

  public void testCommonPrefixes() {
    List<String> vocab = new ArrayList<>();
    vocab.add("aswoon");
    vocab.add("asyl");
    vocab.add("asyla");
    vocab.add("asyllabic");
    vocab.add("asylum");
    vocab.add("asylums");
    vocab.add("asymmetric");
    vocab.add("asymmetrical");
    vocab.add("asymmetrically");
    vocab.add("asymmetries");
    vocab.add("asymmetry");
    vocab.add("asymptomatic");
    vocab.add("asymptomatically");
    vocab.add("asymptote");
    vocab.add("asymptotes");
    vocab.add("asymptotic");
    vocab.add("asymptotical");
    vocab.add("asymptotically");
    vocab.add("asynapses");
    vocab.add("asynapsis");

    int[] blockFPs = new int[vocab.size()];
    for (int i = 0; i < blockFPs.length; i++) {
      blockFPs[i] = i;
    }
    List<BytesRef> blockKeys = vocab.stream().map(BytesRef::new).collect(Collectors.toList());
    FSTDictionary indexDictionary = createFSTDictionary(blockKeys, blockFPs);
    IndexDictionary.Browser browser = indexDictionary.browser();
    for (int i = 0; i < vocab.size(); i++) {
      assertEquals(blockFPs[i], browser.seekBlock(blockKeys.get(i)));
    }
    assertEquals(blockFPs[vocab.size() - 1], browser.seekBlock(new BytesRef("zoo")));
    assertEquals(-1, browser.seekBlock(new BytesRef("A")));
    assertEquals(blockFPs[9], browser.seekBlock(new BytesRef("asymmetriesz")));
  }

  private static FSTDictionary createFSTDictionary(List<BytesRef> blockKeys, int[] blockFPs) {
    FSTDictionary.Builder builder = new FSTDictionary.Builder();
    for (int i = 0; i < blockKeys.size(); i++) {
      builder.add(blockKeys.get(i), blockFPs[i]);
    }
    return builder.build();
  }

  private static FSTDictionary createFSTDictionary(List<String> vocab) {
    FSTDictionary.Builder builder = new FSTDictionary.Builder();
    for (int i = 0; i < vocab.size(); i++) {
      builder.add(new BytesRef(vocab.get(i)), i);
    }
    return builder.build();
  }

  private static FSTDictionary serializeAndReadDictionary(FSTDictionary srcDictionary, boolean shouldEncrypt) throws IOException {
    ByteBuffersDataOutput output = ByteBuffersDataOutput.newResettableInstance();
    srcDictionary.write(output, shouldEncrypt ? Rot13CypherTestUtil.getBlockEncoder() : null);
    return FSTDictionary.read(output.toDataInput(), shouldEncrypt ? Rot13CypherTestUtil.getBlockDecoder() : null);
  }
}
