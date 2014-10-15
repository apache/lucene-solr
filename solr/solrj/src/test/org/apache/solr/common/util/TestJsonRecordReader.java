package org.apache.solr.common.util;

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

import org.apache.solr.SolrTestCaseJ4;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class TestJsonRecordReader  extends SolrTestCaseJ4 {
  public void testOneLevelSplit() throws IOException {
    String json ="{\n" +
        " \"a\":\"A\" ,\n" +
        " \"b\":[\n" +
        "     {\"c\":\"C\",\"d\":\"D\" ,\"e\": {\n" +
        "                         \"s\":\"S\",\n" +
        "                         \"t\":3}},\n" +
        "     {\"c\":\"C1\",\"d\":\"D1\"},\n" +
        "     {\"c\":\"C2\",\"d\":\"D2\"}\n" +
        " ]\n" +
        "}";
//    System.out.println(json);
//    All parameters are mapped with field name
    JsonRecordReader streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a_s:/a",
        "c_s:/b/c",
        "d_s:/b/d",
        "e_s:/b/e/s",
        "e_i:/b/e/t"
    ));

    List<Map<String, Object>> records = streamer.getAllRecords(new StringReader(json));
    assertEquals(3, records.size());
    assertEquals( 3l, ((Map)records.get(0)).get("e_i") );
    assertEquals( "D2", ((Map)records.get(2)).get("d_s") );
    assertNull( ((Map)records.get(1)).get("e_s") );
    assertNull( ((Map)records.get(2)).get("e_s") );
    assertNull( ((Map)records.get(1)).get("e_i") );
    assertNull( ((Map)records.get(2)).get("e_i") );

    //    All parameters but /b/c is omitted
    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a:/a",
        "d:/b/d",
        "s:/b/e/s",
        "t:/b/e/t"
    ));
    records = streamer.getAllRecords(new StringReader(json));
    for (Map<String, Object> record : records) {
      assertNull( record.get("c") );

    }

    //one nested /b/e/* object is completely ignored
    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a:/a",
        "c:/b/c",
        "d:/b/d"
    ));
    records = streamer.getAllRecords(new StringReader(json));
    for (Map<String, Object> record : records) {
      assertNull( record.get("s") );
      assertNull( record.get("t") );
    }

    //nested /b/e/* object is completely ignored even though /b/e is mapped
    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a_s:/a",
        "c_s:/b/c",
        "d_s:/b/d",
        "e:/b/e"

    ));
    records = streamer.getAllRecords(new StringReader(json));
    for (Map<String, Object> record : records) {
      assertNull( record.get("s") );
      assertNull( record.get("t") );
      assertNull( record.get("e") );
    }



    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a_s:/a",
        "c_s:/b/c",
        "d_s:/b/d",
        "/b/e/*"
    ));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(3, records.size());
    assertEquals( 3l, ((Map)records.get(0)).get("t") );
    assertEquals( "S", ((Map)records.get(0)).get("s") );
    assertNull( ((Map)records.get(1)).get("s") );
    assertNull( ((Map)records.get(2)).get("s") );




  }

  public void testRecursiveWildCard() throws IOException {
    String json ="{\n" +
        " \"a\":\"A\" ,\n" +
        " \"b\":[\n" +
        "     {\"c\":\"C\",\"d\":\"D\" ,\"e\": {\n" +
        "                         \"s\":\"S\",\n" +
        "                         \"t\":3 ,\"u\":{\"v\":3.1234,\"w\":false}}},\n" +
        "     {\"c\":\"C1\",\"d\":\"D1\"},\n" +
        "     {\"c\":\"C2\",\"d\":\"D2\"}\n" +
        " ]\n" +
        "}";
    JsonRecordReader streamer;
    List<Map<String, Object>> records;

    streamer = JsonRecordReader.getInst("/b", Collections.singletonList("/b/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(3, records.size());
    assertEquals("records "+records,  3l, ((Map)records.get(0)).get("t") );
    assertEquals( "records "+records,"S", ((Map)records.get(0)).get("s") );
    assertEquals( "records "+records,3.1234, ((Map)records.get(0)).get("v") );
    assertEquals( "records "+records,false, ((Map)records.get(0)).get("w") );
    for (Map<String, Object> record : records) {
      assertNotNull("records "+records,record.get("c"));
      assertNotNull("records "+records,record.get("d"));
    }

    streamer = JsonRecordReader.getInst("/", Collections.singletonList("/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(1, records.size());
    assertEquals(3, ((List)((Map)records.get(0)).get("c")).size() );
    assertEquals(3, ((List)((Map)records.get(0)).get("d")).size() );
    assertEquals("records "+records,  3l, ((Map)records.get(0)).get("t") );
    assertEquals( "records "+records,"S", ((Map)records.get(0)).get("s") );
    assertEquals( "records "+records,"A", ((Map)records.get(0)).get("a") );
    assertEquals( "records "+records,false, ((Map)records.get(0)).get("w") );

  }

  public void testRecursiveWildcard2() throws Exception{
    String json = "{\n" +
        "  \"first\": \"John\",\n" +
        "  \"last\": \"Doe\",\n" +
        "  \"grade\": 8,\n" +
        "  \"exams\": [\n" +
        "      {\n" +
        "        \"subject\": \"Maths\",\n" +
        "        \"test\"   : \"term1\",\n" +
        "        \"marks\":90},\n" +
        "        {\n" +
        "         \"subject\": \"Biology\",\n" +
        "         \"test\"   : \"term1\",\n" +
        "         \"marks\":86}\n" +
        "      ]\n" +
        "}";

    JsonRecordReader streamer;
    List<Map<String, Object>> records;

    streamer = JsonRecordReader.getInst("/exams", Collections.singletonList("/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(2, records.size());
    for (Map<String, Object> record : records) {
      assertEquals(6,record.size());
      assertTrue(record.containsKey("subject"));
      assertTrue(record.containsKey("test"));
      assertTrue(record.containsKey("marks"));
    }

    streamer = JsonRecordReader.getInst("/exams", Collections.singletonList("$FQN:/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(2, records.size());
    for (Map<String, Object> record : records) {
      assertEquals(6,record.size());
      assertTrue(record.containsKey("exams.subject"));
      assertTrue(record.containsKey("exams.test"));
      assertTrue(record.containsKey("exams.marks"));
    }

    streamer = JsonRecordReader.getInst("/", Collections.singletonList("txt:/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(1, records.size());
    assertEquals(9, ((List)records.get(0).get("txt")).size() );

  }

}
