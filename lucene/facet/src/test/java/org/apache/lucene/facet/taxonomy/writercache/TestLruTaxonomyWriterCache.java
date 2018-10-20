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

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.junit.Test;

public class TestLruTaxonomyWriterCache extends FacetTestCase {

  @Test
  public void testDefaultLRUTypeIsCollisionSafe() {
    // These labels are clearly different, but have identical longHashCodes.
    // Note that these labels are clearly contrived. We did encounter
    // collisions in actual production data, but we aren't allowed to publish
    // those.
    final FacetLabel a = new FacetLabel("\0", "\u0003\uFFE2");
    final FacetLabel b = new FacetLabel("\1", "\0");
    // If this fails, then the longHashCode implementation has changed. This
    // cannot prevent collisions. (All hashes must allow for collisions.) It
    // will however stop the rest of this test from making sense. To fix, find
    // new colliding labels, or make a subclass of FacetLabel that produces
    // collisions.
    assertEquals(a.longHashCode(), b.longHashCode());
    // Make a cache with capacity > 2 so both our labels will fit. Don't
    // specify an LRUType, since we want to check if the default is
    // collision-safe.
    final LruTaxonomyWriterCache cache = new LruTaxonomyWriterCache(10);
    cache.put(a, 0);
    cache.put(b, 1);
    assertEquals(cache.get(a), 0);
    assertEquals(cache.get(b), 1);
  }

}
