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


import java.io.BufferedWriter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;

/** Tests that &gt; 64k affixes actually works and doesnt overflow some internal int */
public class Test64kAffixes extends LuceneTestCase {
  
  public void test() throws Exception {
    Path tempDir = createTempDir("64kaffixes");
    Path affix = tempDir.resolve("64kaffixes.aff");
    Path dict = tempDir.resolve("64kaffixes.dic");
    
    BufferedWriter affixWriter = Files.newBufferedWriter(affix, StandardCharsets.UTF_8);
    
    // 65k affixes with flag 1, then an affix with flag 2
    affixWriter.write("SET UTF-8\nFLAG num\nSFX 1 Y 65536\n");
    for (int i = 0; i < 65536; i++) {
      affixWriter.write("SFX 1 0 " + Integer.toHexString(i) + " .\n");
    }
    affixWriter.write("SFX 2 Y 1\nSFX 2 0 s\n");
    affixWriter.close();
    
    BufferedWriter dictWriter = Files.newBufferedWriter(dict, StandardCharsets.UTF_8);
    
    // drink signed with affix 2 (takes -s)
    dictWriter.write("1\ndrink/2\n");
    dictWriter.close();
    
    try (InputStream affStream = Files.newInputStream(affix); InputStream dictStream = Files.newInputStream(dict); Directory tempDir2 = newDirectory()) {
      Dictionary dictionary = new Dictionary(tempDir2, "dictionary", affStream, dictStream);
      Stemmer stemmer = new Stemmer(dictionary);
      // drinks should still stem to drink
      List<CharsRef> stems = stemmer.stem("drinks");
      assertEquals(1, stems.size());
      assertEquals("drink", stems.get(0).toString());
    }
  }
}
