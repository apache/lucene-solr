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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.util.TestUtil;

public class TestUTF8TaxonomyWriterCache extends FacetTestCase {
  public void testPageOverflow() throws Exception {
    UTF8TaxonomyWriterCache cache = new UTF8TaxonomyWriterCache();
    for (int ord = 0; ord < 65536 * 2; ord++) {
      cache.put(new FacetLabel("foo:", Integer.toString(ord)), ord);
    }

    for (int ord = 0; ord < 65536 * 2; ord++) {
      assertEquals(ord, cache.get(new FacetLabel("foo:", Integer.toString(ord))));
    }
  }

  public void testRandom() throws Exception {
    LabelToOrdinal map = new LabelToOrdinalMap();

    UTF8TaxonomyWriterCache cache = new UTF8TaxonomyWriterCache();

    final int n = atLeast(10 * 1000);
    final int numUniqueValues = 50 * 1000;

    Random random = random();
    Set<String> uniqueValuesSet = new HashSet<>();
    while (uniqueValuesSet.size() < numUniqueValues) {
      int numParts = TestUtil.nextInt(random(), 1, 5);
      StringBuilder b = new StringBuilder();
      for (int i=0;i<numParts;i++) {
        String part = null;
        while (true) {
          part = TestUtil.randomRealisticUnicodeString(random(), 16);
          part = part.replace("/", "");
          if (part.length() > 0) {
            break;
          }
        }

        if (i > 0) {
          b.append('/');
        }
        b.append(part);
      }
      uniqueValuesSet.add(b.toString());
    }
    String[] uniqueValues = uniqueValuesSet.toArray(new String[0]);

    int ordUpto = 0;
    for (int i = 0; i < n; i++) {

      int index = random.nextInt(numUniqueValues);
      FacetLabel label;
      String s = uniqueValues[index];
      if (s.length() == 0) {
        label = new FacetLabel();
      } else {
        label = new FacetLabel(s.split("/"));
      }

      int ord1 = map.getOrdinal(label);
      int ord2 = cache.get(label);

      assertEquals(ord1, ord2);

      if (ord1 == LabelToOrdinal.INVALID_ORDINAL) {
        ord1 = ordUpto++;
        map.addLabel(label, ord1);
        cache.put(label, ord1);
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
      int ord2 = cache.get(label);
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
