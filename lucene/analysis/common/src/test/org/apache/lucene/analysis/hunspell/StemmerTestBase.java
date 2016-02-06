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


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;

/** base class for hunspell stemmer tests */
public abstract class StemmerTestBase extends LuceneTestCase {
  private static Stemmer stemmer;
  
  @AfterClass
  public static void afterClass() {
    stemmer = null;
  }
  
  static void init(String affix, String dictionary) throws IOException, ParseException {
    init(false, affix, dictionary);
  }

  static void init(boolean ignoreCase, String affix, String... dictionaries) throws IOException, ParseException {
    if (dictionaries.length == 0) {
      throw new IllegalArgumentException("there must be at least one dictionary");
    }
    
    InputStream affixStream = StemmerTestBase.class.getResourceAsStream(affix);
    if (affixStream == null) {
      throw new FileNotFoundException("file not found: " + affix);
    }
    
    InputStream dictStreams[] = new InputStream[dictionaries.length];
    for (int i = 0; i < dictionaries.length; i++) {
      dictStreams[i] = StemmerTestBase.class.getResourceAsStream(dictionaries[i]);
      if (dictStreams[i] == null) {
        throw new FileNotFoundException("file not found: " + dictStreams[i]);
      }
    }
    
    try {
      Dictionary dictionary = new Dictionary(new RAMDirectory(), "dictionary", affixStream, Arrays.asList(dictStreams), ignoreCase);
      stemmer = new Stemmer(dictionary);
    } finally {
      IOUtils.closeWhileHandlingException(affixStream);
      IOUtils.closeWhileHandlingException(dictStreams);
    }
  }
  
  static void assertStemsTo(String s, String... expected) {
    assertNotNull(stemmer);
    Arrays.sort(expected);
    
    List<CharsRef> stems = stemmer.stem(s);
    String actual[] = new String[stems.size()];
    for (int i = 0; i < actual.length; i++) {
      actual[i] = stems.get(i).toString();
    }
    Arrays.sort(actual);
    
    assertArrayEquals("expected=" + Arrays.toString(expected) + ",actual=" + Arrays.toString(actual), expected, actual);
  }
}
