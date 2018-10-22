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

import java.io.StringReader;
import java.util.Arrays;

import junit.framework.TestCase;

/**
 * ExtendedBufferedReaderTest
 *
 */
public class ExtendedBufferedReaderTest extends TestCase {

  // ======================================================
  //   the test cases
  // ======================================================
 
  public void testConstructors() {
    ExtendedBufferedReader br = new ExtendedBufferedReader(new StringReader(""));
    br = new ExtendedBufferedReader(new StringReader(""), 10); 
  }
  
  public void testReadLookahead1() throws Exception {
   
    assertEquals(ExtendedBufferedReader.END_OF_STREAM, getEBR("").read());
    ExtendedBufferedReader br = getEBR("1\n2\r3\n");
    assertEquals('1', br.lookAhead());
    assertEquals(ExtendedBufferedReader.UNDEFINED, br.readAgain());
    assertEquals('1', br.read());
    assertEquals('1', br.readAgain());

    assertEquals(0, br.getLineNumber());
    assertEquals('\n', br.lookAhead());
    assertEquals(0, br.getLineNumber());
    assertEquals('1', br.readAgain());    
    assertEquals('\n', br.read());
    assertEquals(1, br.getLineNumber());
    assertEquals('\n', br.readAgain());
    assertEquals(1, br.getLineNumber());
    
    assertEquals('2', br.lookAhead());
    assertEquals(1, br.getLineNumber());
    assertEquals('\n', br.readAgain());
    assertEquals(1, br.getLineNumber());
    assertEquals('2', br.read());
    assertEquals('2', br.readAgain());
    
    assertEquals('\r', br.lookAhead());
    assertEquals('2', br.readAgain());
    assertEquals('\r', br.read());
    assertEquals('\r', br.readAgain());
    
    assertEquals('3', br.lookAhead());
    assertEquals('\r', br.readAgain());
    assertEquals('3', br.read());
    assertEquals('3', br.readAgain());
    
    assertEquals('\n', br.lookAhead());
    assertEquals(1, br.getLineNumber());
    assertEquals('3', br.readAgain());
    assertEquals('\n', br.read());
    assertEquals(2, br.getLineNumber());
    assertEquals('\n', br.readAgain());
    assertEquals(2, br.getLineNumber());
    
    assertEquals(ExtendedBufferedReader.END_OF_STREAM, br.lookAhead());
    assertEquals('\n', br.readAgain());
    assertEquals(ExtendedBufferedReader.END_OF_STREAM, br.read());
    assertEquals(ExtendedBufferedReader.END_OF_STREAM, br.readAgain());
    assertEquals(ExtendedBufferedReader.END_OF_STREAM, br.read());
    assertEquals(ExtendedBufferedReader.END_OF_STREAM, br.lookAhead());
 
  }
  

  public void testReadLookahead2() throws Exception {
    char[] ref = new char[5];
    char[] res = new char[5];  
    
    ExtendedBufferedReader br = getEBR("");
    assertEquals(0, br.read(res, 0, 0));
    assertTrue(Arrays.equals(res, ref)); 
    
    br = getEBR("abcdefg");
    ref[0] = 'a';
    ref[1] = 'b';
    ref[2] = 'c';
    assertEquals(3, br.read(res, 0, 3));
    assertTrue(Arrays.equals(res, ref));
    assertEquals('c', br.readAgain());
    
    assertEquals('d', br.lookAhead());
    ref[4] = 'd';
    assertEquals(1, br.read(res, 4, 1));
    assertTrue(Arrays.equals(res, ref));
    assertEquals('d', br.readAgain());
 
  }
  
  public void testMarkSupported() {
    assertFalse(getEBR("foo").markSupported());
  }
  
  public void testReadLine() throws Exception {
    ExtendedBufferedReader br = getEBR("");
    assertTrue(br.readLine() == null);
    
    br = getEBR("\n");
    assertTrue(br.readLine().equals(""));
    assertTrue(br.readLine() == null);
    
    br = getEBR("foo\n\nhello");
    assertEquals(0, br.getLineNumber());
    assertTrue(br.readLine().equals("foo"));
    assertEquals(1, br.getLineNumber());
    assertTrue(br.readLine().equals(""));
    assertEquals(2, br.getLineNumber());
    assertTrue(br.readLine().equals("hello"));
    assertEquals(3, br.getLineNumber());
    assertTrue(br.readLine() == null);
    assertEquals(3, br.getLineNumber());
    
    br = getEBR("foo\n\nhello");
    assertEquals('f', br.read());
    assertEquals('o', br.lookAhead());
    assertTrue(br.readLine().equals("oo"));
    assertEquals(1, br.getLineNumber());
    assertEquals('\n', br.lookAhead());
    assertTrue(br.readLine().equals(""));
    assertEquals(2, br.getLineNumber());
    assertEquals('h', br.lookAhead());
    assertTrue(br.readLine().equals("hello"));
    assertTrue(br.readLine() == null);
    assertEquals(3, br.getLineNumber());
    
 
    br = getEBR("foo\rbaar\r\nfoo");
    assertTrue(br.readLine().equals("foo"));
    assertEquals('b', br.lookAhead());
    assertTrue(br.readLine().equals("baar"));
    assertEquals('f', br.lookAhead());
    assertTrue(br.readLine().equals("foo"));
    assertTrue(br.readLine() == null);
  }
  
  public void testSkip0() throws Exception {
    
    ExtendedBufferedReader br = getEBR("");
    assertEquals(0, br.skip(0));
    assertEquals(0, br.skip(1));
    
    br = getEBR("");
    assertEquals(0, br.skip(1));
    
    br = getEBR("abcdefg");
    assertEquals(0, br.skip(0));
    assertEquals('a', br.lookAhead());
    assertEquals(0, br.skip(0));
    assertEquals('a', br.lookAhead());
    assertEquals(1, br.skip(1));
    assertEquals('b', br.lookAhead());
    assertEquals('b', br.read());
    assertEquals(3, br.skip(3));
    assertEquals('f', br.lookAhead());
    assertEquals(2, br.skip(5));
    assertTrue(br.readLine() == null);
    
    br = getEBR("12345");
    assertEquals(5, br.skip(5));
    assertTrue (br.lookAhead() == ExtendedBufferedReader.END_OF_STREAM);
  }
  
  public void testSkipUntil() throws Exception {   
    ExtendedBufferedReader br = getEBR("");
    assertEquals(0, br.skipUntil(';'));
    br = getEBR("ABCDEF,GHL,,MN");
    assertEquals(6, br.skipUntil(','));
    assertEquals(0, br.skipUntil(','));
    br.skip(1);
    assertEquals(3, br.skipUntil(','));
    br.skip(1);
    assertEquals(0, br.skipUntil(','));
    br.skip(1);
    assertEquals(2, br.skipUntil(','));
  }
  
  public void testReadUntil() throws Exception {
    ExtendedBufferedReader br = getEBR("");
    assertTrue(br.readUntil(';').equals(""));
    br = getEBR("ABCDEF;GHL;;MN");
    assertTrue(br.readUntil(';').equals("ABCDEF"));
    assertTrue(br.readUntil(';').length() == 0);
    br.skip(1);
    assertTrue(br.readUntil(';').equals("GHL"));
    br.skip(1);
    assertTrue(br.readUntil(';').equals(""));
    br.skip(1);
    assertTrue(br.readUntil(',').equals("MN"));
  }
  
  private ExtendedBufferedReader getEBR(String s) {
    return new ExtendedBufferedReader(new StringReader(s));
  }
}
