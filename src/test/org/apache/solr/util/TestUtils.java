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

package org.apache.solr.util;

import junit.framework.TestCase;

import java.util.List;

/**
 * @author yonik
 * @version $Id$
 */
public class TestUtils extends TestCase {
  public static void testSplitEscaping() {
    List<String> arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", true);
    assertEquals(2,arr.size());
    assertEquals("\r\n",arr.get(0));
    assertEquals("\t\f\b",arr.get(1));

    arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", false);
    assertEquals(2,arr.size());
    assertEquals("\\r\\n",arr.get(0));
    assertEquals("\\t\\f\\b",arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", true);
    assertEquals(2,arr.size());
    assertEquals("\r\n",arr.get(0));
    assertEquals("\t\f\b",arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", false);
    assertEquals(2,arr.size());
    assertEquals("\\r\\n",arr.get(0));
    assertEquals("\\t\\f\\b",arr.get(1));

    arr = StrUtils.splitSmart("\\:foo\\::\\:bar\\:", ":", true);
    assertEquals(2,arr.size());
    assertEquals(":foo:",arr.get(0));
    assertEquals(":bar:",arr.get(1));

    arr = StrUtils.splitWS("\\ foo\\  \\ bar\\ ", true);
    assertEquals(2,arr.size());
    assertEquals(" foo ",arr.get(0));
    assertEquals(" bar ",arr.get(1));
  }



}
