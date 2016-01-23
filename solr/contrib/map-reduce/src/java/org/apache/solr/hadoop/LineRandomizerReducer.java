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
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MR Reducer that randomizing a list of URLs.
 * 
 * Reducer input is (randomPosition, URL) pairs. Each such pair indicates a file
 * to index.
 * 
 * Reducer output is a list of URLs, each URL in a random position.
 */
public class LineRandomizerReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (Text value : values) {
      LOGGER.debug("reduce key: {}, value: {}", key, value);
      context.write(value, NullWritable.get());
    }
  }
}