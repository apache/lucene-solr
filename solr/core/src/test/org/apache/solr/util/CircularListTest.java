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
package org.apache.solr.util;

import java.io.IOException;

import org.apache.solr.SolrTestCase;
import org.apache.solr.logging.CircularList;
import org.junit.Test;

/** 
 * Test circular list
 */
public class CircularListTest  extends SolrTestCase {

  @Test
  public void testCircularList() throws IOException {
    CircularList<Integer> list = new CircularList<>(10);
    for(int i=0;i<10; i++) {
      list.add(i);
    }
    assertEquals("within list", Integer.valueOf(0), list.get(0));
    for(int i=10;i<20; i++) {
      list.add(i);
      assertEquals("within list", Integer.valueOf(i - 9), list.get(0));
    }
    
    // now try the resize
    list.resize(5);
    assertEquals(Integer.valueOf(15), list.get(0));
    list.resize(10);
    assertEquals(Integer.valueOf(15), list.get(0));
  }
}
