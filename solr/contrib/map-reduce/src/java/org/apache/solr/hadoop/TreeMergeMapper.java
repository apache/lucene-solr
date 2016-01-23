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
package org.apache.solr.hadoop;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For the meat see {@link TreeMergeOutputFormat}.
 */
public class TreeMergeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String MAX_SEGMENTS_ON_TREE_MERGE = "maxSegmentsOnTreeMerge";

  public static final String SOLR_SHARD_NUMBER = "_solrShardNumber";

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    LOGGER.trace("map key: {}, value: {}", key, value);
    context.write(value, NullWritable.get());
  }
  
}