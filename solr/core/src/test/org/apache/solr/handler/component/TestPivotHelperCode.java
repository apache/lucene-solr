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
package org.apache.solr.handler.component;


import org.apache.solr.SolrTestCaseJ4;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.util.SuppressForbidden;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

/**
 * A light weight test of various helper methods used in pivot faceting
 *
 **/
public class TestPivotHelperCode extends SolrTestCaseJ4{

  /**
   * test refinement encoding/decoding matches specific expected encoded values 
   * @see PivotFacetHelper#encodeRefinementValuePath
   * @see PivotFacetHelper#decodeRefinementValuePath
   */
  public void testRefinementStringEncodingWhiteBox() {
    // trivial example with some basci escaping of an embedded comma
    assertBiDirectionalEncoding(strs("foo,bar","yak","zat"), "~foo\\,bar,~yak,~zat");

    // simple single valued case
    assertBiDirectionalEncoding( strs("foo"), "~foo");

    // special case: empty list
    assertBiDirectionalEncoding(strs(), "");

    // special case: single element list containing empty string
    assertBiDirectionalEncoding(strs(""), "~");

    // special case: single element list containing null
    assertBiDirectionalEncoding(strs((String)null), "^");

    // mix of empty strings & null with other values
    assertBiDirectionalEncoding(strs("", "foo", "", "", null, "bar"),
                                "~,~foo,~,~,^,~bar");
  }

  /**
   * test refinement encoding/decoding of random sets of values can be round tripped, 
   * w/o worrying about what the actual encoding looks like
   *
   * @see PivotFacetHelper#encodeRefinementValuePath
   * @see PivotFacetHelper#decodeRefinementValuePath
   */
  public void testRefinementStringEncodingBlockBoxRoundTrip() {
    // random data: we should be able to round trip any set of random strings
    final int numIters = atLeast(100);
    for (int i = 0; i < numIters; i++) {
      final int numStrs = atLeast(1);
      List<String> data = new ArrayList<String>(numStrs);
      for (int j = 0; j < numStrs; j++) {
        // :TODO: mix in nulls
        data.add(TestUtil.randomUnicodeString(random()));
      }
      String encoded = PivotFacetHelper.encodeRefinementValuePath(data);
      List<String> decoded = PivotFacetHelper.decodeRefinementValuePath(encoded);
      assertEquals(data, decoded);
    }

  }

  private void assertBiDirectionalEncoding(List<String> data, String encoded) {
    assertEquals(data, PivotFacetHelper.decodeRefinementValuePath(encoded));
    assertEquals(encoded, PivotFacetHelper.encodeRefinementValuePath(data));
  }


  @SuppressForbidden(reason = "Checking object equality for Long instance")
  @SuppressWarnings("BoxedPrimitiveConstructor")
  public void testCompareWithNullLast() throws Exception {
    Long a = random().nextLong();
    Long b = random().nextLong();

    assertEquals(a.compareTo(b), PivotFacetFieldValueCollection.compareWithNullLast(a, b));
    assertEquals(b.compareTo(a), PivotFacetFieldValueCollection.compareWithNullLast(b, a));

    Long bb = new Long(b.longValue());
    assertEquals(0, PivotFacetFieldValueCollection.compareWithNullLast(b, bb));

    assertEquals(0, PivotFacetFieldValueCollection.compareWithNullLast(null, null));

    assertTrue( PivotFacetFieldValueCollection.compareWithNullLast(a, null) < 0 );
    assertTrue( PivotFacetFieldValueCollection.compareWithNullLast(b, null) < 0 );

    assertTrue( 0 < PivotFacetFieldValueCollection.compareWithNullLast(null, a) );
    assertTrue( 0 < PivotFacetFieldValueCollection.compareWithNullLast(null, b) );

  }


  private List<String> strs(String... strs) {
    return Arrays.<String>asList(strs);
  }

}
