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
package org.apache.solr.client.solrj.io.stream.eval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.FieldValueEvaluator;
import org.junit.Test;

import junit.framework.Assert;

public class FieldValueEvaluatorTest extends SolrTestCase {

  Map<String, Object> values;
  
  public FieldValueEvaluatorTest() {
    super();
    
    values = new HashMap<String,Object>();
  }
    
  @SuppressWarnings("serial")
  @Test
  public void listTypes() throws Exception{
    values.clear();
    values.put("a", new ArrayList<Boolean>(){{ add(true); add(false); }});
    values.put("b", new ArrayList<Double>(){{ add(0.0); add(1.1); }});
    values.put("c", new ArrayList<Integer>(){{ add(0); add(1); }});
    values.put("d", new ArrayList<Long>(){{ add(0L); add(1L); }});
    values.put("e", new ArrayList<String>(){{ add("first"); add("second"); }});
    
    Tuple tuple = new Tuple(values);
    
    for(String fieldName : new String[]{ "a", "b", "c", "d", "e" }){
      Assert.assertTrue(new FieldValueEvaluator(fieldName).evaluate(tuple) instanceof Collection);
      Assert.assertEquals(2, ((Collection<?>)new FieldValueEvaluator(fieldName).evaluate(tuple)).size());
    }
    
    Assert.assertEquals(false, ((Collection<?>)new FieldValueEvaluator("a").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1.1, ((Collection<?>)new FieldValueEvaluator("b").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1, ((Collection<?>)new FieldValueEvaluator("c").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1L, ((Collection<?>)new FieldValueEvaluator("d").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals("second", ((Collection<?>)new FieldValueEvaluator("e").evaluate(tuple)).toArray()[1]);
  }
  
  @Test
  public void arrayTypes() throws Exception{
    values.clear();
    values.put("a", new Boolean[]{ true, false });
    values.put("b", new Double[]{ 0.0, 1.1 });
    values.put("c", new Integer[]{ 0, 1 });
    values.put("d", new Long[]{ 0L, 1L });
    values.put("e", new String[]{ "first", "second" });
    
    Tuple tuple = new Tuple(values);
    
    for(String fieldName : new String[]{ "a", "b", "c", "d", "e" }){
      Assert.assertTrue(new FieldValueEvaluator(fieldName).evaluate(tuple) instanceof Collection);
      Assert.assertEquals(2, ((Collection<?>)new FieldValueEvaluator(fieldName).evaluate(tuple)).size());
    }
    
    Assert.assertEquals(false, ((Collection<?>)new FieldValueEvaluator("a").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1.1, ((Collection<?>)new FieldValueEvaluator("b").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1, ((Collection<?>)new FieldValueEvaluator("c").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1L, ((Collection<?>)new FieldValueEvaluator("d").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals("second", ((Collection<?>)new FieldValueEvaluator("e").evaluate(tuple)).toArray()[1]);
  }
  
  @SuppressWarnings("serial")
  @Test
  public void iterableTypes() throws Exception{
    values.clear();
    
    values.put("a", new PriorityQueue<Boolean>(){{ add(true); add(false); }});
    values.put("b", new PriorityQueue<Double>(){{ add(0.0); add(1.1); }});
    values.put("c", new PriorityQueue<Integer>(){{ add(0); add(1); }});
    values.put("d", new PriorityQueue<Long>(){{ add(0L); add(1L); }});
    values.put("e", new PriorityQueue<String>(){{ add("first"); add("second"); }});
    
    Tuple tuple = new Tuple(values);
    
    for(String fieldName : new String[]{ "a", "b", "c", "d", "e" }){
      Assert.assertTrue(new FieldValueEvaluator(fieldName).evaluate(tuple) instanceof Collection);
      Assert.assertEquals(2, ((Collection<?>)new FieldValueEvaluator(fieldName).evaluate(tuple)).size());
    }
    
    // the priority queue is doing natural ordering, so false is first
    Assert.assertEquals(true, ((Collection<?>)new FieldValueEvaluator("a").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1.1, ((Collection<?>)new FieldValueEvaluator("b").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1, ((Collection<?>)new FieldValueEvaluator("c").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals(1L, ((Collection<?>)new FieldValueEvaluator("d").evaluate(tuple)).toArray()[1]);
    Assert.assertEquals("second", ((Collection<?>)new FieldValueEvaluator("e").evaluate(tuple)).toArray()[1]);
  }
}
