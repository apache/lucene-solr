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

package org.apache.lucene.search.grouping;

import org.apache.lucene.util.LuceneTestCase;

public class TestDoubleRangeFactory extends LuceneTestCase {

  public void test() {

    DoubleRangeFactory factory = new DoubleRangeFactory(10, 10, 50);
    DoubleRange scratch = new DoubleRange(0, 0);

    assertEquals(new DoubleRange(Double.MIN_VALUE, 10), factory.getRange(4, scratch));
    assertEquals(new DoubleRange(10, 20), factory.getRange(10, scratch));
    assertEquals(new DoubleRange(20, 30), factory.getRange(20, scratch));
    assertEquals(new DoubleRange(10, 20), factory.getRange(15, scratch));
    assertEquals(new DoubleRange(30, 40), factory.getRange(35, scratch));
    assertEquals(new DoubleRange(50, Double.MAX_VALUE), factory.getRange(50, scratch));
    assertEquals(new DoubleRange(50, Double.MAX_VALUE), factory.getRange(500, scratch));

  }

}
