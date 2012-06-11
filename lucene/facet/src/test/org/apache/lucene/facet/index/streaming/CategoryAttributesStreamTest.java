package org.apache.lucene.facet.index.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.lucene.facet.index.CategoryContainerTestBase;
import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryAttributeImpl;
import org.apache.lucene.facet.index.streaming.CategoryAttributesStream;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

public class CategoryAttributesStreamTest extends CategoryContainerTestBase {

  /**
   * Verifies that a {@link CategoryAttributesStream} accepts
   * {@link CategoryAttribute} and passes them on as tokens.
   * 
   * @throws IOException
   */
  @Test
  public void testStream() throws IOException {
    ArrayList<CategoryAttribute> attributesList = new ArrayList<CategoryAttribute>();
    for (int i = 0; i < initialCatgeories.length; i++) {
      attributesList.add(new CategoryAttributeImpl(initialCatgeories[i]));
    }

    // test number of tokens
    CategoryAttributesStream stream = new CategoryAttributesStream(
        attributesList);
    int nTokens = 0;
    while (stream.incrementToken()) {
      nTokens++;
    }
    assertEquals("Wrong number of tokens", 3, nTokens);

    // test reset
    stream.reset();
    nTokens = 0;
    while (stream.incrementToken()) {
      nTokens++;
    }
    assertEquals("Wrong number of tokens", 3, nTokens);

    // test reset and contents
    Set<CategoryPath> pathsSet = new HashSet<CategoryPath>();
    for (int i = 0; i < initialCatgeories.length; i++) {
      pathsSet.add(initialCatgeories[i]);
    }
    stream.reset();
    while (stream.incrementToken()) {
      CategoryAttribute fromStream = stream
          .getAttribute(CategoryAttribute.class);
      if (!pathsSet.remove(fromStream.getCategoryPath())) {
        fail("Unexpected category path: "
            + fromStream.getCategoryPath().toString(':'));
      }
    }
    assertTrue("all category paths should have been found", pathsSet
        .isEmpty());
  }
}
