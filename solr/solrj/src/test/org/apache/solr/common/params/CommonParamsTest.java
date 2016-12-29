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
package org.apache.solr.common.params;

import org.apache.lucene.util.LuceneTestCase;

/**
 * Unit test for {@link CommonParams}
 *
 * This class tests backwards compatibility of CommonParams parameter constants.
 * If someone accidentally changes those constants then this test will flag that up.
 */
public class CommonParamsTest extends LuceneTestCase
{
  public void testStart() { assertEquals("start", CommonParams.START); }
  public void testStartDefault() { assertEquals(0, CommonParams.START_DEFAULT); }

  public void testRows() { assertEquals("rows", CommonParams.ROWS); }
  public void testRowsDefault() { assertEquals(10, CommonParams.ROWS_DEFAULT); }

  public void testPreferLocalShards() { assertEquals("preferLocalShards", CommonParams.PREFER_LOCAL_SHARDS); }
}
