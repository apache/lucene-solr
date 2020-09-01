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
package org.apache.solr.client.solrj.io.stream.ops;

import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

/**
 **/

public class OperationsTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public OperationsTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("replace", ReplaceOperation.class);
    values = new HashedMap<>();
  }
    
  @Test
  public void replaceValueNullWithString() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withValue=foo)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("foo", tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }

  @Test
  public void replaceValueNullWithInt() throws Exception{
    Tuple tuple;
    StreamOperation operation;
    
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withValue=123)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", (long)123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals((long)123, tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }
  
  @Test
  public void replaceValueNullWithFloat() throws Exception{
    Tuple tuple;
    StreamOperation operation;
    
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withValue=123.45678)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals(123.45678, tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }
  
  @Test
  public void replaceValueNullWithDouble() throws Exception{
    Tuple tuple;
    StreamOperation operation;
    
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withValue=123.45678912345)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals(123.45678912345, tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }

  @Test
  public void replaceFieldNullWithString() throws Exception{
    Tuple tuple;
    StreamOperation operation;
    
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withField=fieldB)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }

  @Test
  public void replaceFieldNullWithInt() throws Exception{
    Tuple tuple;
    StreamOperation operation;
    
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withField=fieldC)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals(123, tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }
  
  @Test
  public void replaceFieldNullWithNonExistantField() throws Exception{
    Tuple tuple;
    StreamOperation operation;
    
    operation = new ReplaceOperation("fieldA", StreamExpressionParser.parse("replace(null, withField=fieldD)"), factory);
    
    // replace
    values.clear();
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNull(tuple.get("fieldA"));
    
    // don't replace
    values.clear();
    values.put("fieldA", "exists");
    values.put("fieldB", "bar");
    values.put("fieldC", 123);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("exists", tuple.get("fieldA"));
  }  
  
}
