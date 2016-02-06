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

package org.apache.lucene.facet.taxonomy.writercache;

import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.junit.Test;

public class TestCompactLabelToOrdinal extends FacetTestCase {

  @Test
  public void testL2O() throws Exception {
    LabelToOrdinal map = new LabelToOrdinalMap();

    CompactLabelToOrdinal compact = new CompactLabelToOrdinal(2000000, 0.15f, 3);

    final int n = atLeast(10 * 1000);
    final int numUniqueValues = 50 * 1000;

    String[] uniqueValues = new String[numUniqueValues];
    byte[] buffer = new byte[50];

    Random random = random();
    for (int i = 0; i < numUniqueValues;) {
      random.nextBytes(buffer);
      int size = 1 + random.nextInt(buffer.length);

      // This test is turning random bytes into a string,
      // this is asking for trouble.
      CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
          .onUnmappableCharacter(CodingErrorAction.REPLACE)
          .onMalformedInput(CodingErrorAction.REPLACE);
      uniqueValues[i] = decoder.decode(ByteBuffer.wrap(buffer, 0, size)).toString();
      // we cannot have empty path components, so eliminate all prefix as well
      // as middle consecutive delimiter chars.
      uniqueValues[i] = uniqueValues[i].replaceAll("/+", "/");
      if (uniqueValues[i].startsWith("/")) {
        uniqueValues[i] = uniqueValues[i].substring(1);
      }
      if (uniqueValues[i].indexOf(CompactLabelToOrdinal.TERMINATOR_CHAR) == -1) {
        i++;
      }
    }

    Path tmpDir = createTempDir("testLableToOrdinal");
    Path f = tmpDir.resolve("CompactLabelToOrdinalTest.tmp");
    int flushInterval = 10;

    for (int i = 0; i < n; i++) {
      if (i > 0 && i % flushInterval == 0) {
        compact.flush(f);    
        compact = CompactLabelToOrdinal.open(f, 0.15f, 3);
        Files.delete(f);
        if (flushInterval < (n / 10)) {
          flushInterval *= 10;
        }
      }

      int index = random.nextInt(numUniqueValues);
      FacetLabel label;
      String s = uniqueValues[index];
      if (s.length() == 0) {
        label = new FacetLabel();
      } else {
        label = new FacetLabel(s.split("/"));
      }

      int ord1 = map.getOrdinal(label);
      int ord2 = compact.getOrdinal(label);

      assertEquals(ord1, ord2);

      if (ord1 == LabelToOrdinal.INVALID_ORDINAL) {
        ord1 = compact.getNextOrdinal();
        map.addLabel(label, ord1);
        compact.addLabel(label, ord1);
      }
    }

    for (int i = 0; i < numUniqueValues; i++) {
      FacetLabel label;
      String s = uniqueValues[i];
      if (s.length() == 0) {
        label = new FacetLabel();
      } else {
        label = new FacetLabel(s.split("/"));
      }
      int ord1 = map.getOrdinal(label);
      int ord2 = compact.getOrdinal(label);
      assertEquals(ord1, ord2);
    }
  }

  private static class LabelToOrdinalMap extends LabelToOrdinal {
    private Map<FacetLabel, Integer> map = new HashMap<>();

    LabelToOrdinalMap() { }
    
    @Override
    public void addLabel(FacetLabel label, int ordinal) {
      map.put(label, ordinal);
    }

    @Override
    public int getOrdinal(FacetLabel label) {
      Integer value = map.get(label);
      return (value != null) ? value.intValue() : LabelToOrdinal.INVALID_ORDINAL;
    }

  }

}
