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
package org.apache.lucene.document;

import org.apache.lucene.util.LuceneTestCase;

/**
 * Simple tests for {@link LatLonPoint}
 * TODO: move this lone test and remove class?
 * */
public class TestLatLonPoint extends LuceneTestCase {

  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals("LatLonPoint <field:18.313693958334625,-65.22744401358068>",(new LatLonPoint("field", 18.313694, -65.227444)).toString());
    
    // looks crazy due to lossiness
    assertEquals("field:[18.000000016763806 TO 18.999999999068677],[-65.9999999217689 TO -65.00000006519258]", LatLonPoint.newBoxQuery("field", 18, 19, -66, -65).toString());
    
    // distance query does not quantize inputs
    assertEquals("field:18.0,19.0 +/- 25.0 meters", LatLonPoint.newDistanceQuery("field", 18, 19, 25).toString());
  }
}
