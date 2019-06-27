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
package org.apache.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;


public class TestDocumentBuilder extends SolrTestCase {

  @Test
  public void testDeepCopy() throws IOException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("field1", "value1");
    doc.addField("field2", "value1");
    doc.addField("field3", "value2");
    doc.addField("field4", 15);
    List<Integer> list = new ArrayList<>();
    list.add(45);
    list.add(33);
    list.add(20);
    doc.addField("field5", list);
    
    SolrInputDocument clone = doc.deepCopy();
    
    System.out.println("doc1: "+ doc);
    System.out.println("clone: "+ clone);
    
    assertNotSame(doc, clone);
    
    Collection<String> fieldNames = doc.getFieldNames();
    for (String name : fieldNames) {
      Collection<Object> values = doc.getFieldValues(name);
      Collection<Object> cloneValues = clone.getFieldValues(name);
      
      assertEquals(values.size(), cloneValues.size());
      assertNotSame(values, cloneValues);
      
      Iterator<Object> cloneIt = cloneValues.iterator();
      for (Object value : values) {
        Object cloneValue = cloneIt.next();
        assertSame(value, cloneValue);
      }
    }
  }

}
