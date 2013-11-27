package org.apache.lucene.facet;

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

import java.util.Arrays;

import org.apache.lucene.util._TestUtil;

public class TestFacetsConfig extends FacetTestCase {
  public void testPathToStringAndBack() throws Exception {
    int iters = atLeast(1000);
    for(int i=0;i<iters;i++) {
      int numParts = _TestUtil.nextInt(random(), 1, 6);
      String[] parts = new String[numParts];
      for(int j=0;j<numParts;j++) {
        String s;
        while (true) {
          s = _TestUtil.randomUnicodeString(random());
          if (s.length() > 0) {
            break;
          }
        }
        parts[j] = s;
      }

      String s = FacetsConfig.pathToString(parts);
      String[] parts2 = FacetsConfig.stringToPath(s);
      assertTrue(Arrays.equals(parts, parts2));
    }
  }
}
