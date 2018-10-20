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
package org.apache.lucene.queryparser.flexible.standard;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.util.LuceneTestCase;

/** Simple test for point field integration into the flexible QP */
public class TestPointQueryParser extends LuceneTestCase {
  
  public void testIntegers() throws Exception {
    StandardQueryParser parser = new StandardQueryParser();
    Map<String,PointsConfig> pointsConfig = new HashMap<>();
    pointsConfig.put("intField", new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class));
    parser.setPointsConfigMap(pointsConfig);
    
    assertEquals(IntPoint.newRangeQuery("intField", 1, 3),
                 parser.parse("intField:[1 TO 3]", "body"));
    assertEquals(IntPoint.newRangeQuery("intField", 1, 1),
                 parser.parse("intField:1", "body"));
  }
  
  public void testLongs() throws Exception {
    StandardQueryParser parser = new StandardQueryParser();
    Map<String,PointsConfig> pointsConfig = new HashMap<>();
    pointsConfig.put("longField", new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Long.class));
    parser.setPointsConfigMap(pointsConfig);
    
    assertEquals(LongPoint.newRangeQuery("longField", 1, 3),
                 parser.parse("longField:[1 TO 3]", "body"));
    assertEquals(LongPoint.newRangeQuery("longField", 1, 1),
                 parser.parse("longField:1", "body"));
  }
  
  public void testFloats() throws Exception {
    StandardQueryParser parser = new StandardQueryParser();
    Map<String,PointsConfig> pointsConfig = new HashMap<>();
    pointsConfig.put("floatField", new PointsConfig(NumberFormat.getNumberInstance(Locale.ROOT), Float.class));
    parser.setPointsConfigMap(pointsConfig);
    
    assertEquals(FloatPoint.newRangeQuery("floatField", 1.5F, 3.6F),
                 parser.parse("floatField:[1.5 TO 3.6]", "body"));
    assertEquals(FloatPoint.newRangeQuery("floatField", 1.5F, 1.5F),
                 parser.parse("floatField:1.5", "body"));
  }
  
  public void testDoubles() throws Exception {
    StandardQueryParser parser = new StandardQueryParser();
    Map<String,PointsConfig> pointsConfig = new HashMap<>();
    pointsConfig.put("doubleField", new PointsConfig(NumberFormat.getNumberInstance(Locale.ROOT), Double.class));
    parser.setPointsConfigMap(pointsConfig);
    
    assertEquals(DoublePoint.newRangeQuery("doubleField", 1.5D, 3.6D),
                 parser.parse("doubleField:[1.5 TO 3.6]", "body"));
    assertEquals(DoublePoint.newRangeQuery("doubleField", 1.5D, 1.5D),
                 parser.parse("doubleField:1.5", "body"));
  }

}
