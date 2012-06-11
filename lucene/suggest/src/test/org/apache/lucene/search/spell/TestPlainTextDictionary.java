package org.apache.lucene.search.spell;

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
import java.io.StringReader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test case for PlainTextDictionary
 *
 */
public class TestPlainTextDictionary extends LuceneTestCase {

  public void testBuild() throws IOException {
    final String LF = System.getProperty("line.separator");
    String input = "oneword" + LF + "twoword" + LF + "threeword";
    PlainTextDictionary ptd = new PlainTextDictionary(new StringReader(input));
    Directory ramDir = newDirectory();
    SpellChecker spellChecker = new SpellChecker(ramDir);
    spellChecker.indexDictionary(ptd, newIndexWriterConfig(TEST_VERSION_CURRENT, null), false);
    String[] similar = spellChecker.suggestSimilar("treeword", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "threeword");
    assertEquals(similar[1], "oneword");
    spellChecker.close();
    ramDir.close();
  }

}
