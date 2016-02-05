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
package org.apache.lucene.queries.function;

import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.RangeMapFloatFunction;
import org.apache.lucene.search.BaseExplanationTestCase;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;

public class TestFunctionQueryExplanations extends BaseExplanationTestCase {
  
  public void testSimple() throws Exception {
    Query q = new FunctionQuery(new ConstValueSource(5));
    qtest(q, new int[] { 0,1,2,3 });
  }

  public void testBoost() throws Exception {
    Query q = new BoostQuery(new FunctionQuery(new ConstValueSource(5)), 2);
    qtest(q, new int[] { 0,1,2,3 });
  }

  public void testMapFunction() throws Exception {
    ValueSource rff = new RangeMapFloatFunction(new ConstValueSource(3), 0, 1, 2, new Float(4));
    Query q = new FunctionQuery(rff);
    qtest(q, new int[] { 0,1,2,3 });
    assertEquals("map(const(3.0),0.0,1.0,const(2.0),const(4.0))", rff.description());
    assertEquals("map(const(3.0),min=0.0,max=1.0,target=const(2.0),defaultVal=const(4.0))", rff.getValues(null, null).toString(123));

    // DefaultValue is null -> defaults to source value
    rff = new RangeMapFloatFunction(new ConstValueSource(3), 0, 1, 2, null);
    assertEquals("map(const(3.0),0.0,1.0,const(2.0),null)", rff.description());
    assertEquals("map(const(3.0),min=0.0,max=1.0,target=const(2.0),defaultVal=null)", rff.getValues(null, null).toString(123));
  }
}
