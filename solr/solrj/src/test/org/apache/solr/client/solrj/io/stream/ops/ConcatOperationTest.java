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
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

/**
 **/

public class ConcatOperationTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public ConcatOperationTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("concat", ConcatOperation.class);
    values = new HashedMap<>();
  }
    
  @Test
  public void concatSingleField() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(new String[]{"fieldA"}, "fieldAConcat", "-");
    
    values.clear();
    values.put("fieldA", "bar");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldAConcat"));
    Assert.assertEquals("bar", tuple.get("fieldAConcat"));
  }

  @Test
  public void concatMultipleFields() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(new String[]{"fieldA","fieldB"}, "fieldABConcat", "-");
    values.clear();
    values.put("fieldA", "bar");
    values.put("fieldB", "baz");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("bar-baz", tuple.get("fieldABConcat"));
    
    // do the same in oposite order
    operation = new ConcatOperation(new String[]{"fieldB","fieldA"}, "fieldABConcat", "-");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("baz-bar", tuple.get("fieldABConcat"));
  }
  
  @Test
  public void concatMultipleFieldsWithIgnoredFields() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(new String[]{"fieldA","fieldB"}, "fieldABConcat", "-");
    values.clear();
    values.put("fieldA", "bar");
    values.put("fieldB", "baz");
    values.put("fieldC", "bab");
    values.put("fieldD", "bat");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldC"));
    Assert.assertEquals("bab", tuple.get("fieldC"));
    
    Assert.assertNotNull(tuple.get("fieldD"));
    Assert.assertEquals("bat", tuple.get("fieldD"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("bar-baz", tuple.get("fieldABConcat"));
    
    // do the same in oposite order
    operation = new ConcatOperation(new String[]{"fieldB","fieldA"}, "fieldABConcat", "-");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("baz-bar", tuple.get("fieldABConcat"));
  }

  @Test
  public void concatWithNullValues() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(new String[]{"fieldA","fieldB"}, "fieldABConcat", "-");
    values.clear();
    values.put("fieldA", "bar");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNull(tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("bar-null", tuple.get("fieldABConcat"));
    
  }
  
///////////////////////////
  @Test
  public void concatSingleFieldExpression() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(StreamExpressionParser.parse("concat(fields=\"fieldA\", as=\"fieldAConcat\", delim=\"-\")"), factory);
    
    values.clear();
    values.put("fieldA", "bar");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldAConcat"));
    Assert.assertEquals("bar", tuple.get("fieldAConcat"));
  }

  @Test
  public void concatMultipleFieldsExpression() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(StreamExpressionParser.parse("concat(fields=\"fieldA,fieldB\", as=\"fieldABConcat\", delim=\"-\")"), factory);
    values.clear();
    values.put("fieldA", "bar");
    values.put("fieldB", "baz");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("bar-baz", tuple.get("fieldABConcat"));
    
    // do the same in oposite order
    operation = new ConcatOperation(StreamExpressionParser.parse("concat(fields=\"fieldB,fieldA\", as=\"fieldABConcat\", delim=\"-\")"), factory);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("baz-bar", tuple.get("fieldABConcat"));
  }
  
  @Test
  public void concatMultipleFieldsWithIgnoredFieldsExpression() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(StreamExpressionParser.parse("concat(fields=\"fieldA,fieldB\", as=\"fieldABConcat\", delim=\"-\")"), factory);
    values.clear();
    values.put("fieldA", "bar");
    values.put("fieldB", "baz");
    values.put("fieldC", "bab");
    values.put("fieldD", "bat");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldC"));
    Assert.assertEquals("bab", tuple.get("fieldC"));
    
    Assert.assertNotNull(tuple.get("fieldD"));
    Assert.assertEquals("bat", tuple.get("fieldD"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("bar-baz", tuple.get("fieldABConcat"));
    
    // do the same in oposite order
    operation = new ConcatOperation(StreamExpressionParser.parse("concat(fields=\"fieldB,fieldA\", as=\"fieldABConcat\", delim=\"-\")"), factory);
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNotNull(tuple.get("fieldB"));
    Assert.assertEquals("baz", tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("baz-bar", tuple.get("fieldABConcat"));
  }

  @Test
  public void concatWithNullValuesExpression() throws Exception{
    Tuple tuple;
    StreamOperation operation;
        
    operation = new ConcatOperation(StreamExpressionParser.parse("concat(fields=\"fieldA,fieldB\", as=\"fieldABConcat\", delim=\"-\")"), factory);
    values.clear();
    values.put("fieldA", "bar");
    tuple = new Tuple(values);
    operation.operate(tuple);
    
    Assert.assertNotNull(tuple.get("fieldA"));
    Assert.assertEquals("bar", tuple.get("fieldA"));
    
    Assert.assertNull(tuple.get("fieldB"));
    
    Assert.assertNotNull(tuple.get("fieldABConcat"));
    Assert.assertEquals("bar-null", tuple.get("fieldABConcat"));
    
  }
  
}
