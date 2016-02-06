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
 * This class tests backwards compatibility of {@link ShardParams} parameter constants.
 * If someone accidentally changes those constants then this test will flag that up. 
 */
public class ShardParamsTest extends LuceneTestCase
{
  public void testShards() { assertEquals(ShardParams.SHARDS, "shards"); }

  public void testShardsRows() { assertEquals(ShardParams.SHARDS_ROWS, "shards.rows"); }
  public void testShardsStart() { assertEquals(ShardParams.SHARDS_START, "shards.start"); }

  public void testIds() { assertEquals(ShardParams.IDS, "ids"); }
  
  public void testIsShard() { assertEquals(ShardParams.IS_SHARD, "isShard"); }
  
  public void testShardUrl() { assertEquals(ShardParams.SHARD_URL, "shard.url"); }
  
  public void testShardsQt() { assertEquals(ShardParams.SHARDS_QT, "shards.qt"); }
  
  public void testShardsInfo() { assertEquals(ShardParams.SHARDS_INFO, "shards.info"); }
  
  public void testShardsTolerant() { assertEquals(ShardParams.SHARDS_TOLERANT, "shards.tolerant"); }
  
  public void testShardsPurpose() { assertEquals(ShardParams.SHARDS_PURPOSE, "shards.purpose"); }
  
  public void testRoute() { assertEquals(ShardParams._ROUTE_, "_route_"); }
  
  public void testDistribSinglePass() { assertEquals(ShardParams.DISTRIB_SINGLE_PASS, "distrib.singlePass"); }
}
