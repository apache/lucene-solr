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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests {@link FSTDictionary}.
 */
public class TestFSTDictionary extends LuceneTestCase {

  public void testEmptyTerm() {
    FSTDictionary indexDictionary = createFSTDictionary(new int[]{588}, Collections.singletonList(new BytesRef()));
    assertEquals(588, indexDictionary.browser().seekBlock(new BytesRef()));
  }

  public void testEmptyTermRepeated() {
    BytesRef[] terms = {new BytesRef(), new BytesRef()};
    try {
      createFSTDictionary(new int[]{588, 2045}, Arrays.asList(terms));
      fail("Expected exception not thrown");
    } catch (Exception e) {
      assertSame(UnsupportedOperationException.class, e.getClass());
    }
  }

  public void testNonEmptyTermRepeated() {
    BytesRef[] terms = {new BytesRef("a"), new BytesRef("a")};
    try {
      createFSTDictionary(new int[]{588, 2045}, Arrays.asList(terms));
      fail("Expected exception not thrown");
    } catch (Exception e) {
      assertSame(UnsupportedOperationException.class, e.getClass());
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
    FSTDictionary indexDictionary = createFSTDictionary(blockFPs, blockKeys);
    IndexDictionary.Browser browser = indexDictionary.browser();
    for (int i = 0; i < vocab.size(); i++) {
      assertEquals(blockFPs[i], browser.seekBlock(blockKeys.get(i)));
    }
    assertEquals(blockFPs[vocab.size() - 1], browser.seekBlock(new BytesRef("zoo")));

    assertEquals(-1, browser.seekBlock(new BytesRef("A")));

    assertEquals(blockFPs[9], browser.seekBlock(new BytesRef("asymmetriesz")));
  }

  private static FSTDictionary createFSTDictionary(int[] blockFPs, List<BytesRef> blockKeys) {
    FSTDictionary.Builder builder = new FSTDictionary.Builder();
    for (int i = 0; i < blockKeys.size(); i++) {
      builder.add(blockKeys.get(i), blockFPs[i]);
    }
    return builder.build();
  }
}
