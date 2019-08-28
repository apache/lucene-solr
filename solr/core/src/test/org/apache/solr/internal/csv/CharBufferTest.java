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

import junit.framework.TestCase;
import org.apache.solr.SolrTestCaseJ4;

public class CharBufferTest extends TestCase {
    public void testCreate() {
        CharBuffer cb = new CharBuffer();
        assertEquals(0, cb.length());
        SolrTestCaseJ4.expectThrows(IllegalArgumentException.class, () -> new CharBuffer(0));
        cb = new CharBuffer(128);
        assertEquals(0, cb.length());
    }
    
    public void testAppendChar() {
        CharBuffer cb = new CharBuffer(1);
        String expected = "";
        for (char c = 'a'; c < 'z'; c++) {
            cb.append(c);
            expected += c;
            assertEquals(expected, cb.toString());
            assertEquals(expected.length(), cb.length());
        }
    }
    
    public void testAppendCharArray() {
        CharBuffer cb = new CharBuffer(1);
        char[] abcd = "abcd".toCharArray();
        String expected = "";
        for (int i=0; i<10; i++) {
            cb.append(abcd);
            expected += "abcd";
            assertEquals(expected, cb.toString());
            assertEquals(4*(i+1), cb.length());
        }
    }
    
    public void testAppendString() {
        CharBuffer cb = new CharBuffer(1);
        String abcd = "abcd";
        String expected = "";
        for (int i=0; i<10; i++) {
            cb.append(abcd);
            expected += abcd;
            assertEquals(expected, cb.toString());
            assertEquals(4*(i+1), cb.length());
        }
    }
    
    public void testAppendStringBuffer() {
        CharBuffer cb = new CharBuffer(1);
        StringBuffer abcd = new StringBuffer("abcd");
        String expected = "";
        for (int i=0; i<10; i++) {
            cb.append(abcd);
            expected += "abcd";
            assertEquals(expected, cb.toString());
            assertEquals(4*(i+1), cb.length());
        }
    }
    
    public void testAppendCharBuffer() {
        CharBuffer cb = new CharBuffer(1);
        CharBuffer abcd = new CharBuffer(17);
        abcd.append("abcd");
        String expected = "";
        for (int i=0; i<10; i++) {
            cb.append(abcd);
            expected += "abcd";
            assertEquals(expected, cb.toString());
            assertEquals(4*(i+1), cb.length());
        }
    }
    
    public void testShrink() {
        String data = "123456789012345678901234567890";
        
        CharBuffer cb = new CharBuffer(data.length() + 100);
        assertEquals(data.length() + 100, cb.capacity());
        cb.append(data);
        assertEquals(data.length() + 100, cb.capacity());
        assertEquals(data.length(), cb.length());
        cb.shrink();
        assertEquals(data.length(), cb.capacity());
        assertEquals(data.length(), cb.length());
        assertEquals(data, cb.toString());
    }
    
    //-- the following test cases have been adapted from the HttpComponents project
    //-- written by Oleg Kalnichevski
    
    public void testSimpleAppend() throws Exception {
        CharBuffer buffer = new CharBuffer(16);
        assertEquals(16, buffer.capacity()); 
        assertEquals(0, buffer.length());
        char[] b1 = buffer.getCharacters();
        assertNotNull(b1);
        assertEquals(0, b1.length);
        assertEquals(0, buffer.length());
        
        char[] tmp = new char[] { '1', '2', '3', '4'};
        buffer.append(tmp);
        assertEquals(16, buffer.capacity()); 
        assertEquals(4, buffer.length());
        
        char[] b2 = buffer.getCharacters();
        assertNotNull(b2);
        assertEquals(4, b2.length);
        for (int i = 0; i < tmp.length; i++) {
            assertEquals(tmp[i], b2[i]);
        }
        assertEquals("1234", buffer.toString());
        
        buffer.clear();
        assertEquals(16, buffer.capacity()); 
        assertEquals(0, buffer.length());
    }
    
    public void testAppendString2() throws Exception {
        CharBuffer buffer = new CharBuffer(8);
        buffer.append("stuff");
        buffer.append(" and more stuff");
        assertEquals("stuff and more stuff", buffer.toString());
    }
    
    public void testAppendNull() throws Exception {
        CharBuffer buffer = new CharBuffer(8);
        
        buffer.append((StringBuffer)null);
        assertEquals("", buffer.toString());
        
        buffer.append((String)null);
        assertEquals("", buffer.toString());

        buffer.append((CharBuffer)null);
        assertEquals("", buffer.toString());

        buffer.append((char[])null);
        assertEquals("", buffer.toString());
    }
    
    public void testAppendCharArrayBuffer() throws Exception {
        CharBuffer buffer1 = new CharBuffer(8);
        buffer1.append(" and more stuff");
        CharBuffer buffer2 = new CharBuffer(8);
        buffer2.append("stuff");
        buffer2.append(buffer1);
        assertEquals("stuff and more stuff", buffer2.toString());
    }
    
    public void testAppendSingleChar() throws Exception {
        CharBuffer buffer = new CharBuffer(4);
        buffer.append('1');
        buffer.append('2');
        buffer.append('3');
        buffer.append('4');
        buffer.append('5');
        buffer.append('6');
        assertEquals("123456", buffer.toString());
    }
    
    public void testProvideCapacity() throws Exception {
        CharBuffer buffer = new CharBuffer(4);
        buffer.provideCapacity(2);
        assertEquals(4, buffer.capacity());
        buffer.provideCapacity(8);
        assertTrue(buffer.capacity() >= 8);
    }
}
