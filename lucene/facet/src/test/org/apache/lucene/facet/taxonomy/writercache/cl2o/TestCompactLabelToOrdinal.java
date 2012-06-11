package org.apache.lucene.facet.taxonomy.writercache.cl2o;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.writercache.cl2o.CompactLabelToOrdinal;
import org.apache.lucene.facet.taxonomy.writercache.cl2o.LabelToOrdinal;

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

public class TestCompactLabelToOrdinal extends LuceneTestCase {

  @Test
  public void testL2O() throws Exception {
    LabelToOrdinal map = new LabelToOrdinalMap();

    CompactLabelToOrdinal compact = new CompactLabelToOrdinal(2000000, 0.15f, 3);

    final int n = atLeast(10 * 1000);
    final int numUniqueValues = 50 * 1000;

    String[] uniqueValues = new String[numUniqueValues];
    byte[] buffer = new byte[50];

    for (int i = 0; i < numUniqueValues;) {
      random().nextBytes(buffer);
      int size = 1 + random().nextInt(50);

      uniqueValues[i] = new String(buffer, 0, size);
      if (uniqueValues[i].indexOf(CompactLabelToOrdinal.TerminatorChar) == -1) {
        i++;
      }
    }

    TEMP_DIR.mkdirs();
    File f = new File(TEMP_DIR, "CompactLabelToOrdinalTest.tmp");
    int flushInterval = 10;

    for (int i = 0; i < n * 10; i++) {
      if (i > 0 && i % flushInterval == 0) {
        compact.flush(f);    
        compact = CompactLabelToOrdinal.open(f, 0.15f, 3);
        assertTrue(f.delete());
        if (flushInterval < (n / 10)) {
          flushInterval *= 10;
        }
      }

      int index = random().nextInt(numUniqueValues);
      CategoryPath label = new CategoryPath(uniqueValues[index], '/');

      int ord1 = map.getOrdinal(label);
      int ord2 = compact.getOrdinal(label);

      //System.err.println(ord1+" "+ord2);

      assertEquals(ord1, ord2);

      if (ord1 == LabelToOrdinal.InvalidOrdinal) {
        ord1 = compact.getNextOrdinal();

        map.addLabel(label, ord1);
        compact.addLabel(label, ord1);
      }
    }

    for (int i = 0; i < numUniqueValues; i++) {
      CategoryPath label = new CategoryPath(uniqueValues[i], '/');
      int ord1 = map.getOrdinal(label);
      int ord2 = compact.getOrdinal(label);
      assertEquals(ord1, ord2);
    }
  }

  private static class LabelToOrdinalMap extends LabelToOrdinal {
    private Map<CategoryPath, Integer> map = new HashMap<CategoryPath, Integer>();

    LabelToOrdinalMap() { }
    
    @Override
    public void addLabel(CategoryPath label, int ordinal) {
      map.put(new CategoryPath(label), ordinal);
    }

    @Override
    public void addLabel(CategoryPath label, int prefixLen, int ordinal) {
      map.put(new CategoryPath(label, prefixLen), ordinal);
    }

    @Override
    public int getOrdinal(CategoryPath label) {
      Integer value = map.get(label);
      return (value != null) ? value.intValue() : LabelToOrdinal.InvalidOrdinal;
    }

    @Override
    public int getOrdinal(CategoryPath label, int prefixLen) {
      Integer value = map.get(new CategoryPath(label, prefixLen));
      return (value != null) ? value.intValue() : LabelToOrdinal.InvalidOrdinal;
    }

  }
}
