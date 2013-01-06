package org.apache.lucene.facet.index.categorypolicy;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

public class PathPolicyTest extends LuceneTestCase {

  @Test
  public void testDefaultPathPolicy() {
    // check path policy
    CategoryPath cp = CategoryPath.EMPTY;
    PathPolicy pathPolicy = PathPolicy.ALL_CATEGORIES;
    assertFalse("default path policy should not accept root", pathPolicy.shouldAdd(cp));
    for (int i = 0; i < 300; i++) {
      int nComponents = 1 + random().nextInt(10);
      String[] components = new String[nComponents];
      for (int j = 0; j < components.length; j++) {
        components[j] = (Integer.valueOf(random().nextInt(30))).toString();
      }
      cp = new CategoryPath(components);
      assertTrue("default path policy should accept " + cp.toString('/'), pathPolicy.shouldAdd(cp));
    }
  }

  @Test
  public void testNonTopLevelPathPolicy() throws Exception {
    Directory dir = newDirectory();
    TaxonomyWriter taxonomy = null;
    taxonomy = new DirectoryTaxonomyWriter(dir);

    CategoryPath[] topLevelPaths = new CategoryPath[10];
    String[] topLevelStrings = new String[10];
    for (int i = 0; i < 10; i++) {
      topLevelStrings[i] = Integer.valueOf(random().nextInt(30)).toString();

      topLevelPaths[i] = new CategoryPath(topLevelStrings[i]);
      taxonomy.addCategory(topLevelPaths[i]);
    }
    CategoryPath[] nonTopLevelPaths = new CategoryPath[300];
    for (int i = 0; i < 300; i++) {
      int nComponents = 2 + random().nextInt(10);
      String[] components = new String[nComponents];
      components[0] = topLevelStrings[i % 10];
      for (int j = 1; j < components.length; j++) {
        components[j] = (Integer.valueOf(random().nextInt(30))).toString();
      }
      nonTopLevelPaths[i] = new CategoryPath(components);
      taxonomy.addCategory(nonTopLevelPaths[i]);
    }
    // check ordinal policy
    PathPolicy pathPolicy = new NonTopLevelPathPolicy();
    assertFalse("top level path policy should not match root",
        pathPolicy.shouldAdd(CategoryPath.EMPTY));
    for (int i = 0; i < 10; i++) {
      assertFalse("top level path policy should not match "
          + topLevelPaths[i],
          pathPolicy.shouldAdd(topLevelPaths[i]));
    }
    for (int i = 0; i < 300; i++) {
      assertTrue("top level path policy should match "
          + nonTopLevelPaths[i],
          pathPolicy.shouldAdd(nonTopLevelPaths[i]));
    }
    taxonomy.close();
    dir.close();
  }
}
