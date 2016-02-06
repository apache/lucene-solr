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
package org.apache.solr.internal.csv;

import java.io.IOException;

import junit.framework.TestCase;

/**
 * CSVUtilsTest
 */
public class CSVUtilsTest extends TestCase {
  
  // ======================================================
  //   static parser tests
  // ======================================================
  public void testParse1() throws IOException {
      String[][] data = CSVUtils.parse("abc\ndef");
      assertEquals(2, data.length);
      assertEquals(1, data[0].length);
      assertEquals(1, data[1].length);
      assertEquals("abc", data[0][0]);
      assertEquals("def", data[1][0]);
    }

    public void testParse2() throws IOException {
      String[][] data = CSVUtils.parse("abc,def,\"ghi,jkl\"\ndef");
      assertEquals(2, data.length);
      assertEquals(3, data[0].length);
      assertEquals(1, data[1].length);
      assertEquals("abc", data[0][0]);
      assertEquals("def", data[0][1]);
      assertEquals("ghi,jkl", data[0][2]);
      assertEquals("def", data[1][0]);
    }

    public void testParse3() throws IOException {
      String[][] data = CSVUtils.parse("abc,\"def\nghi\"\njkl");
      assertEquals(2, data.length);
      assertEquals(2, data[0].length);
      assertEquals(1, data[1].length);
      assertEquals("abc", data[0][0]);
      assertEquals("def\nghi", data[0][1]);
      assertEquals("jkl", data[1][0]);
    }

    public void testParse4() throws IOException {
      String[][] data = CSVUtils.parse("abc,\"def\\\\nghi\"\njkl");
      assertEquals(2, data.length);
      assertEquals(2, data[0].length);
      assertEquals(1, data[1].length);
      assertEquals("abc", data[0][0]);
      // an escape char in quotes only escapes a delimiter, not itself
      assertEquals("def\\\\nghi", data[0][1]);
      assertEquals("jkl", data[1][0]);
    }

    public void testParse5() throws IOException {
      String[][] data = CSVUtils.parse("abc,def\\nghi\njkl");
      assertEquals(2, data.length);
      assertEquals(2, data[0].length);
      assertEquals(1, data[1].length);
      assertEquals("abc", data[0][0]);
      assertEquals("def\\nghi", data[0][1]);
      assertEquals("jkl", data[1][0]);
    }
    
    public void testParse6() throws IOException {
      String[][] data = CSVUtils.parse("");
      // default strategy is CSV, which ignores empty lines
      assertEquals(0, data.length);
    }
    
    public void testParse7() throws IOException {
      boolean io = false;
      try {
        CSVUtils.parse(null);
      } catch (IllegalArgumentException e) {
        io = true;
      }
      assertTrue(io);
    }
    
    public void testParseLine1() throws IOException {
      String[] data = CSVUtils.parseLine("abc,def,ghi");
      assertEquals(3, data.length);
      assertEquals("abc", data[0]);
      assertEquals("def", data[1]);
      assertEquals("ghi", data[2]);
    }

    public void testParseLine2() throws IOException {
      String[] data = CSVUtils.parseLine("abc,def,ghi\n");
      assertEquals(3, data.length);
      assertEquals("abc", data[0]);
      assertEquals("def", data[1]);
      assertEquals("ghi", data[2]);
    }

    public void testParseLine3() throws IOException {
      String[] data = CSVUtils.parseLine("abc,\"def,ghi\"");
      assertEquals(2, data.length);
      assertEquals("abc", data[0]);
      assertEquals("def,ghi", data[1]);
    }

    public void testParseLine4() throws IOException {
      String[] data = CSVUtils.parseLine("abc,\"def\nghi\"");
      assertEquals(2, data.length);
      assertEquals("abc", data[0]);
      assertEquals("def\nghi", data[1]);
    }
    
    public void testParseLine5() throws IOException {
      String[] data = CSVUtils.parseLine("");
      assertEquals(0, data.length);
      // assertEquals("", data[0]);
    }
    
    public void testParseLine6() throws IOException {
      boolean io = false;
      try {
        CSVUtils.parseLine(null);
      } catch (IllegalArgumentException e) {
        io = true;
      }
      assertTrue(io);
    }
    
    public void testParseLine7() throws IOException {
      String[] res = CSVUtils.parseLine("");
      assertNotNull(res);
      assertEquals(0, res.length);  
    }
      
}
