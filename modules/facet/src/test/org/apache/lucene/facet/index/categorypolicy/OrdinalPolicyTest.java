package org.apache.lucene.facet.index.categorypolicy;

import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.categorypolicy.DefaultOrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.NonTopLevelOrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyWriter;

/**
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

public class OrdinalPolicyTest extends LuceneTestCase {

  @Test
  public void testDefaultOrdinalPolicy() {
    // check ordinal policy
    OrdinalPolicy ordinalPolicy = new DefaultOrdinalPolicy();
    assertFalse("default ordinal policy should not match root", ordinalPolicy
        .shouldAdd(TaxonomyReader.ROOT_ORDINAL));
    for (int i = 0; i < 300; i++) {
      int ordinal = 1 + random.nextInt(Integer.MAX_VALUE - 1);
      assertTrue("default ordinal policy should match " + ordinal,
          ordinalPolicy.shouldAdd(ordinal));
    }
  }

  @Test
  public void testNonTopLevelOrdinalPolicy() throws Exception {
    Directory dir = newDirectory();
    TaxonomyWriter taxonomy = null;
    taxonomy = new LuceneTaxonomyWriter(dir);

    int[] topLevelOrdinals = new int[10];
    String[] topLevelStrings = new String[10];
    for (int i = 0; i < 10; i++) {
      topLevelStrings[i] = Integer.valueOf(random.nextInt(30)).toString();
      topLevelOrdinals[i] = taxonomy.addCategory(new CategoryPath(
          topLevelStrings[i]));
    }
    int[] nonTopLevelOrdinals = new int[300];
    for (int i = 0; i < 300; i++) {
      int nComponents = 2 + random.nextInt(10);
      String[] components = new String[nComponents];
      components[0] = topLevelStrings[i % 10];
      for (int j = 1; j < components.length; j++) {
        components[j] = (Integer.valueOf(random.nextInt(30))).toString();
      }
      nonTopLevelOrdinals[i] = taxonomy.addCategory(new CategoryPath(
          components));
    }
    // check ordinal policy
    OrdinalPolicy ordinalPolicy = new NonTopLevelOrdinalPolicy();
    ordinalPolicy.init(taxonomy);
    assertFalse("top level ordinal policy should not match root", ordinalPolicy
        .shouldAdd(TaxonomyReader.ROOT_ORDINAL));
    for (int i = 0; i < 10; i++) {
      assertFalse("top level ordinal policy should not match "
          + topLevelOrdinals[i],
          ordinalPolicy.shouldAdd(topLevelOrdinals[i]));
    }
    for (int i = 0; i < 300; i++) {
      assertTrue("top level ordinal policy should match "
          + nonTopLevelOrdinals[i],
          ordinalPolicy.shouldAdd(nonTopLevelOrdinals[i]));
    }

    // check illegal ordinal
    assertFalse("Should not add illegal ordinal", ordinalPolicy.shouldAdd(100000));
    taxonomy.close();
    dir.close();
  }

}
