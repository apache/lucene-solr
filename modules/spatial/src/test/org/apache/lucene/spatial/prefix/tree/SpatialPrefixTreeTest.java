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

package org.apache.lucene.spatial.prefix.tree;

import com.spatial4j.core.context.simple.SimpleSpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;
import org.junit.Test;

public class SpatialPrefixTreeTest extends LuceneTestCase {

  //TODO plug in others and test them
  private SimpleSpatialContext ctx;
  private SpatialPrefixTree trie;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    ctx = SimpleSpatialContext.GEO_KM;
    trie = new GeohashPrefixTree(ctx,4);
  }

  @Test
  public void testNodeTraverse() {
    Node prevN = null;
    Node n = trie.getWorldNode();
    assertEquals(0,n.getLevel());
    assertEquals(ctx.getWorldBounds(),n.getShape());
    while(n.getLevel() < trie.getMaxLevels()) {
      prevN = n;
      n = n.getSubCells().iterator().next();//TODO random which one?
      
      assertEquals(prevN.getLevel()+1,n.getLevel());
      Rectangle prevNShape = (Rectangle) prevN.getShape();
      Shape s = n.getShape();
      Rectangle sbox = s.getBoundingBox();
      assertTrue(prevNShape.getWidth() > sbox.getWidth());
      assertTrue(prevNShape.getHeight() > sbox.getHeight());
    }
  }
}
