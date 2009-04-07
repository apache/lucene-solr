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

package org.apache.solr.handler.admin;

import junit.framework.TestCase;

import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * :TODO: currently only tests some of the utilities in the LukeRequestHandler
 */
public class LukeRequestHandlerTest extends TestCase {
  
  /** tests some simple edge cases */
  public void testHistogramPowerOfTwoBucket() {
    assertHistoBucket(1,  1);
    assertHistoBucket(2,  2);
    assertHistoBucket(4,  3);
    assertHistoBucket(4,  4);
    assertHistoBucket(8,  5);
    assertHistoBucket(8,  6);
    assertHistoBucket(8,  7);
    assertHistoBucket(8,  8);
    assertHistoBucket(16, 9);

    final int MAX_VALID = ((Integer.MAX_VALUE/2)+1)/2;
    
    assertHistoBucket(MAX_VALID,   MAX_VALID-1 );
    assertHistoBucket(MAX_VALID,   MAX_VALID   );
    assertHistoBucket(MAX_VALID*2, MAX_VALID+1 );
    
  }
  private void assertHistoBucket(int expected, int in) {
    assertEquals("histobucket: " + in, expected,
                 LukeRequestHandler.TermHistogram.getPowerOfTwoBucket( in ));
  }
}
