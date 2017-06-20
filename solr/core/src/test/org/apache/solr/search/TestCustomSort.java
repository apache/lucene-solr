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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

import java.nio.ByteBuffer;


/**
 * Test SortField.CUSTOM sorts
 */
public class TestCustomSort extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-custom-field.xml");
  }
  
  public void testSortableBinary() throws Exception {
    clearIndex();
    assertU(adoc(sdoc("id", "1", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x12, 0x62, 0x15 }))));                    //  2
    assertU(adoc(sdoc("id", "2", "text", "b", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x16 }))));                    //  5
    assertU(adoc(sdoc("id", "3", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x32, 0x58 }))));                    //  8
    assertU(adoc(sdoc("id", "4", "text", "b", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x15 }))));                    //  4
    assertU(adoc(sdoc("id", "5", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x35, 0x10, 0x00 }))));              //  9
    assertU(adoc(sdoc("id", "6", "text", "c", "payload", ByteBuffer.wrap(new byte[] { 0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03 }))));  //  3
    assertU(adoc(sdoc("id", "7", "text", "c", "payload", ByteBuffer.wrap(new byte[] { 0x00, 0x3c, 0x73 }))));                    //  1
    assertU(adoc(sdoc("id", "8", "text", "c", "payload", ByteBuffer.wrap(new byte[] { 0x59, 0x2d, 0x4d }))));                    // 11
    assertU(adoc(sdoc("id", "9", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x39, 0x79, 0x7a }))));                    // 10
    assertU(adoc(sdoc("id", "10", "text", "b", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c }))));                   //  6
    assertU(adoc(sdoc("id", "11", "text", "d", "payload", ByteBuffer.wrap(new byte[] { (byte)0xff, (byte)0xaf, (byte)0x9c })))); // 13
    assertU(adoc(sdoc("id", "12", "text", "d", "payload", ByteBuffer.wrap(new byte[] { 0x34, (byte)0xdd, 0x4d }))));             //  7
    assertU(adoc(sdoc("id", "13", "text", "d", "payload", ByteBuffer.wrap(new byte[] { (byte)0x80, 0x11, 0x33 }))));             // 12
    assertU(commit());
    
    assertQ(req("q", "*:*", "fl", "id_i", "sort", "payload asc", "rows", "20")
        , "//result[@numFound='13']"                   // <result name="response" numFound="13" start="0">
        , "//result/doc[int='7'  and position()=1]"    //   <doc><int name="id">7</int></doc>   00 3c 73
        , "//result/doc[int='1'  and position()=2]"    //   <doc><int name="id">1</int></doc>   12 62 15
        , "//result/doc[int='6'  and position()=3]"    //   <doc><int name="id">6</int></doc>   1a 2b 3c 00 00 03
        , "//result/doc[int='4'  and position()=4]"    //   <doc><int name="id">4</int></doc>   25 21 15
        , "//result/doc[int='2'  and position()=5]"    //   <doc><int name="id">2</int></doc>   25 21 16
        , "//result/doc[int='10' and position()=6]"    //   <doc><int name="id">10</int></doc>  31 39 7c
        , "//result/doc[int='12' and position()=7]"    //   <doc><int name="id">12</int></doc>  34 dd 4d
        , "//result/doc[int='3'  and position()=8]"    //   <doc><int name="id">3</int></doc>   35 32 58
        , "//result/doc[int='5'  and position()=9]"    //   <doc><int name="id">5</int></doc>   35 35 10 00
        , "//result/doc[int='9'  and position()=10]"   //   <doc><int name="id">9</int></doc>   39 79 7a
        , "//result/doc[int='8'  and position()=11]"   //   <doc><int name="id">8</int></doc>   59 2d 4d      
        , "//result/doc[int='13' and position()=12]"   //   <doc><int name="id">13</int></doc>  80 11 33       
        , "//result/doc[int='11' and position()=13]"); //   <doc><int name="id">11</int></doc>  ff af 9c
    assertQ(req("q", "*:*", "fl", "id_i", "sort", "payload desc", "rows", "20")
        , "//result[@numFound='13']"                   // <result name="response" numFound="13" start="0">
        , "//result/doc[int='11' and position()=1]"    //   <doc><int name="id">11</int></doc>  ff af 9c            
        , "//result/doc[int='13' and position()=2]"    //   <doc><int name="id">13</int></doc>  80 11 33                   
        , "//result/doc[int='8'  and position()=3]"    //   <doc><int name="id">8</int></doc>   59 2d 4d                  
        , "//result/doc[int='9'  and position()=4]"    //   <doc><int name="id">9</int></doc>   39 79 7a            
        , "//result/doc[int='5'  and position()=5]"    //   <doc><int name="id">5</int></doc>   35 35 10 00         
        , "//result/doc[int='3'  and position()=6]"    //   <doc><int name="id">3</int></doc>   35 32 58            
        , "//result/doc[int='12' and position()=7]"    //   <doc><int name="id">12</int></doc>  34 dd 4d            
        , "//result/doc[int='10' and position()=8]"    //   <doc><int name="id">10</int></doc>  31 39 7c            
        , "//result/doc[int='2'  and position()=9]"    //   <doc><int name="id">2</int></doc>   25 21 16            
        , "//result/doc[int='4'  and position()=10]"   //   <doc><int name="id">4</int></doc>   25 21 15            
        , "//result/doc[int='6'  and position()=11]"   //   <doc><int name="id">6</int></doc>   1a 2b 3c 00 00 03   
        , "//result/doc[int='1'  and position()=12]"   //   <doc><int name="id">1</int></doc>   12 62 15            
        , "//result/doc[int='7'  and position()=13]"); //   <doc><int name="id">7</int></doc>   00 3c 73            
    assertQ(req("q", "text:a", "fl", "id_i", "sort", "payload asc", "rows", "20")
        , "//result[@numFound='4']"                    // <result name="response" numFound="4" start="0">
        , "//result/doc[int='1'  and position()=1]"    //   <doc><int name="id">1</int></doc>   12 62 15    
        , "//result/doc[int='3'  and position()=2]"    //   <doc><int name="id">3</int></doc>   35 32 58    
        , "//result/doc[int='5'  and position()=3]"    //   <doc><int name="id">5</int></doc>   35 35 10 00 
        , "//result/doc[int='9'  and position()=4]");  //   <doc><int name="id">9</int></doc>   39 79 7a    
    assertQ(req("q", "text:a", "fl", "id_i", "sort", "payload desc", "rows", "20")
        , "//result[@numFound='4']"                    // <result name="response" numFound="4" start="0">
        , "//result/doc[int='9'  and position()=1]"    //   <doc><int name="id">9</int></doc>   39 79 7a    
        , "//result/doc[int='5'  and position()=2]"    //   <doc><int name="id">5</int></doc>   35 35 10 00 
        , "//result/doc[int='3'  and position()=3]"    //   <doc><int name="id">3</int></doc>   35 32 58    
        , "//result/doc[int='1'  and position()=4]");  //   <doc><int name="id">1</int></doc>   12 62 15    
    assertQ(req("q", "text:b", "fl", "id_i", "sort", "payload asc", "rows", "20")
        , "//result[@numFound='3']"                    // <result name="response" numFound="3" start="0">
        , "//result/doc[int='4'  and position()=1]"    //   <doc><int name="id">4</int></doc>   25 21 15
        , "//result/doc[int='2'  and position()=2]"    //   <doc><int name="id">2</int></doc>   25 21 16
        , "//result/doc[int='10' and position()=3]");  //   <doc><int name="id">10</int></doc>  31 39 7c
    assertQ(req("q", "text:b", "fl", "id_i", "sort", "payload desc", "rows", "20")
        , "//result[@numFound='3']"                    // <result name="response" numFound="3" start="0">
        , "//result/doc[int='10' and position()=1]"    //   <doc><int name="id">10</int></doc>  31 39 7c
        , "//result/doc[int='2'  and position()=2]"    //   <doc><int name="id">2</int></doc>   25 21 16
        , "//result/doc[int='4'  and position()=3]");  //   <doc><int name="id">4</int></doc>   25 21 15
    assertQ(req("q", "text:c", "fl", "id_i", "sort", "payload asc", "rows", "20")
        , "//result[@numFound='3']"                   // <result name="response" numFound="3" start="0">
        , "//result/doc[int='7'  and position()=1]"   //   <doc><int name="id">7</int></doc>    00 3c 73         
        , "//result/doc[int='6'  and position()=2]"   //   <doc><int name="id">6</int></doc>    1a 2b 3c 00 00 03
        , "//result/doc[int='8'  and position()=3]"); //   <doc><int name="id">8</int></doc>    59 2d 4d              
    assertQ(req("q", "text:c", "fl", "id_i", "sort", "payload desc", "rows", "20")
        , "//result[@numFound='3']"                   // <result name="response" numFound="3" start="0">
        , "//result/doc[int='8'  and position()=1]"   //   <doc><int name="id">8</int></doc>    59 2d 4d              
        , "//result/doc[int='6'  and position()=2]"   //   <doc><int name="id">6</int></doc>    1a 2b 3c 00 00 03
        , "//result/doc[int='7'  and position()=3]"); //   <doc><int name="id">7</int></doc>    00 3c 73         
    assertQ(req("q", "text:d", "fl", "id_i", "sort", "payload asc", "rows", "20")
        , "//result[@numFound='3']"                   // <result name="response" numFound="3" start="0">
        , "//result/doc[int='12' and position()=1]"   //   <doc><int name="id">12</int></doc>   34 dd 4d
        , "//result/doc[int='13' and position()=2]"   //   <doc><int name="id">13</int></doc>   80 11 33      
        , "//result/doc[int='11' and position()=3]"); //   <doc><int name="id">11</int></doc>   ff af 9c
    assertQ(req("q", "text:d", "fl", "id_i", "sort", "payload desc", "rows", "20")
        , "//result[@numFound='3']"                   // <result name="response" numFound="3" start="0">
        , "//result/doc[int='11' and position()=1]"   //   <doc><int name="id">11</int></doc>   ff af 9c
        , "//result/doc[int='13' and position()=2]"   //   <doc><int name="id">13</int></doc>   80 11 33      
        , "//result/doc[int='12' and position()=3]"); //   <doc><int name="id">12</int></doc>   34 dd 4d
  }
}
