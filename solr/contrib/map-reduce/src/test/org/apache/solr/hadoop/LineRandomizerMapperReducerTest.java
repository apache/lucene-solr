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
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LineRandomizerMapperReducerTest extends Assert {

  private MapReduceDriver<LongWritable, Text, LongWritable, Text, Text, NullWritable> mapReduceDriver;

  @Before
  public void setUp() {
    LineRandomizerMapper mapper = new LineRandomizerMapper();
    LineRandomizerReducer reducer = new LineRandomizerReducer();
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapReduce1Item() throws IOException {
    mapReduceDriver.withInput(new LongWritable(0), new Text("hello"));
    mapReduceDriver.withOutput(new Text("hello"), NullWritable.get());
    mapReduceDriver.runTest();
  }
  
  @Test
  public void testMapReduce2Items() throws IOException {
    mapReduceDriver.withAll(Arrays.asList(
        new Pair<LongWritable, Text>(new LongWritable(0), new Text("hello")),
        new Pair<LongWritable, Text>(new LongWritable(1), new Text("world"))
        ));
    mapReduceDriver.withAllOutput(Arrays.asList(
        new Pair<Text, NullWritable>(new Text("world"), NullWritable.get()),
        new Pair<Text, NullWritable>(new Text("hello"), NullWritable.get())
        ));
    mapReduceDriver.runTest();
  }
  
  @Test
  public void testMapReduce3Items() throws IOException {
    mapReduceDriver.withAll(Arrays.asList(
        new Pair<LongWritable, Text>(new LongWritable(0), new Text("hello")),
        new Pair<LongWritable, Text>(new LongWritable(1), new Text("world")),
        new Pair<LongWritable, Text>(new LongWritable(2), new Text("nadja"))
        ));
    mapReduceDriver.withAllOutput(Arrays.asList(
        new Pair<Text, NullWritable>(new Text("nadja"), NullWritable.get()),
        new Pair<Text, NullWritable>(new Text("world"), NullWritable.get()),
        new Pair<Text, NullWritable>(new Text("hello"), NullWritable.get())
        ));
    mapReduceDriver.runTest();
  }
  
  @Test
  public void testMapReduce4Items() throws IOException {
    mapReduceDriver.withAll(Arrays.asList(
        new Pair<LongWritable, Text>(new LongWritable(0), new Text("hello")),
        new Pair<LongWritable, Text>(new LongWritable(1), new Text("world")),
        new Pair<LongWritable, Text>(new LongWritable(2), new Text("nadja")),
        new Pair<LongWritable, Text>(new LongWritable(3), new Text("basti"))
        ));
    mapReduceDriver.withAllOutput(Arrays.asList(
        new Pair<Text, NullWritable>(new Text("nadja"), NullWritable.get()),
        new Pair<Text, NullWritable>(new Text("world"), NullWritable.get()),
        new Pair<Text, NullWritable>(new Text("basti"), NullWritable.get()),
        new Pair<Text, NullWritable>(new Text("hello"), NullWritable.get())
        ));
    mapReduceDriver.runTest();
  }
  
}