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
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.StringReader;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RecordingJSONParser;


public class TestJsonRecordReader extends SolrTestCaseJ4 {

  public void testOneLevelSplit() throws IOException {
    String json = "{\n" +
        " \"a\":\"A\" ,\n" +
        " \"b\":[\n" +
        "     {\"c\":\"C\",\"d\":\"D\" ,\"e\": {\n" +
        "                         \"s\":\"S\",\n" +
        "                         \"t\":3}},\n" +
        "     {\"c\":\"C1\",\"d\":\"D1\"},\n" +
        "     {\"c\":\"C2\",\"d\":\"D2\"}\n" +
        " ]\n" +
        "}";
    JsonRecordReader streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a_s:/a",
        "c_s:/b/c",
        "d_s:/b/d",
        "e_s:/b/e/s",
        "e_i:/b/e/t"
    ));

    List<Map<String, Object>> records = streamer.getAllRecords(new StringReader(json));
    assertEquals(3, records.size());
    assertEquals(3l, ((Map) records.get(0)).get("e_i"));
    assertEquals("D2", ((Map) records.get(2)).get("d_s"));
    assertNull(((Map) records.get(1)).get("e_s"));
    assertNull(((Map) records.get(2)).get("e_s"));
    assertNull(((Map) records.get(1)).get("e_i"));
    assertNull(((Map) records.get(2)).get("e_i"));

    //    All parameters but /b/c is omitted
    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a:/a",
        "d:/b/d",
        "s:/b/e/s",
        "t:/b/e/t"
    ));
    records = streamer.getAllRecords(new StringReader(json));
    for (Map<String, Object> record : records) {
      assertNull(record.get("c"));

    }

    //one nested /b/e/* object is completely ignored
    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a:/a",
        "c:/b/c",
        "d:/b/d"
    ));
    records = streamer.getAllRecords(new StringReader(json));
    for (Map<String, Object> record : records) {
      assertNull(record.get("s"));
      assertNull(record.get("t"));
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
      assertNull(record.get("s"));
      assertNull(record.get("t"));
      assertNull(record.get("e"));
    }


    streamer = JsonRecordReader.getInst("/b", Arrays.asList(
        "a_s:/a",
        "c_s:/b/c",
        "d_s:/b/d",
        "/b/e/*"
    ));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(3, records.size());
    assertEquals(3l, ((Map) records.get(0)).get("t"));
    assertEquals("S", ((Map) records.get(0)).get("s"));
    assertNull(((Map) records.get(1)).get("s"));
    assertNull(((Map) records.get(2)).get("s"));


  }

  public void testSrcField() throws Exception {
    String json = "{\n" +
        "  \"id\" : \"123\",\n" +
        "  \"description\": \"Testing /json/docs srcField 1\",\n" +
        "\n" +
        "  \"nested_data\" : {\n" +
        "    \"nested_inside\" : \"check check check 1\"\n" +
        "  }\n" +
        "}";

    String json2 =
        " {\n" +
            "  \"id\" : \"345\",\n" +
            "  \"payload\": \""+ StringUtils.repeat("0123456789", 819) +
            "\",\n" +
            "  \"description\": \"Testing /json/docs srcField 2\",\n" +
            "\n" +
            "  \"nested_data\" : {\n" +
            "    \"nested_inside\" : \"check check check 2\"\n" +
            "  }\n" +
            "}";

    String json3 =
        " {\n" +
            "  \"id\" : \"678\",\n" +
            "  \"description\": \"Testing /json/docs srcField 3\",\n" +
            "\n" +
            "  \"nested_data\" : {\n" +
            "    \"nested_inside\" : \"check check check 3\"\n" +
            "  }\n" +
            "}";


    JsonRecordReader streamer = JsonRecordReader.getInst("/", Arrays.asList("id:/id"));
    RecordingJSONParser parser = new RecordingJSONParser(new StringReader(json + json2 + json3));

    streamer.streamRecords(parser, new JsonRecordReader.Handler() {
      int count = 0;

      @Override
      public void handle(Map<String, Object> record, String path) {
        count++;
        String buf = parser.getBuf();
        parser.resetBuf();

        @SuppressWarnings({"rawtypes"})
        Map m = (Map) Utils.fromJSONString(buf);
        if (count == 1) {
          assertEquals(m.get("id"), "123");
          assertEquals(m.get("description"), "Testing /json/docs srcField 1");
          assertEquals(((Map) m.get("nested_data")).get("nested_inside"), "check check check 1");
        }
        if (count++ == 2) {
          assertEquals(m.get("id"), "345");
          assertEquals(m.get("description"), "Testing /json/docs srcField 2");
          assertEquals(((Map) m.get("nested_data")).get("nested_inside"), "check check check 2");
        }
        if (count++ == 3) {
          assertEquals(m.get("id"), "678");
          assertEquals(m.get("description"), "Testing /json/docs srcField 3");
          assertEquals(((Map) m.get("nested_data")).get("nested_inside"), "check check check 3");
        }

      }
    });

  }

  public void testRecursiveWildCard() throws IOException {
    String json = "{\n" +
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
    assertEquals("records " + records, 3l, ((Map) records.get(0)).get("t"));
    assertEquals("records " + records, "S", ((Map) records.get(0)).get("s"));
    assertEquals("records " + records, 3.1234, ((Map) records.get(0)).get("v"));
    assertEquals("records " + records, false, ((Map) records.get(0)).get("w"));
    for (Map<String, Object> record : records) {
      assertNotNull("records " + records, record.get("c"));
      assertNotNull("records " + records, record.get("d"));
    }

    streamer = JsonRecordReader.getInst("/", Collections.singletonList("/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(1, records.size());
    assertEquals(3, ((List) ((Map) records.get(0)).get("c")).size());
    assertEquals(3, ((List) ((Map) records.get(0)).get("d")).size());
    assertEquals("records " + records, 3l, ((Map) records.get(0)).get("t"));
    assertEquals("records " + records, "S", ((Map) records.get(0)).get("s"));
    assertEquals("records " + records, "A", ((Map) records.get(0)).get("a"));
    assertEquals("records " + records, false, ((Map) records.get(0)).get("w"));

  }

  public void testRecursiveWildcard2() throws Exception {
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
      assertEquals(6, record.size());
      assertTrue(record.containsKey("subject"));
      assertTrue(record.containsKey("test"));
      assertTrue(record.containsKey("marks"));
    }

    streamer = JsonRecordReader.getInst("/exams", Collections.singletonList("$FQN:/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(2, records.size());
    for (Map<String, Object> record : records) {
      assertEquals(6, record.size());
      assertTrue(record.containsKey("exams.subject"));
      assertTrue(record.containsKey("exams.test"));
      assertTrue(record.containsKey("exams.marks"));
    }

    streamer = JsonRecordReader.getInst("/", Collections.singletonList("txt:/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(1, records.size());
    assertEquals(9, ((List) records.get(0).get("txt")).size());

  }

  public void testNestedDocs() throws Exception {
    String json = "{a:{" +
        "b:{c:d}," +
        "x:y" +
        "}}";
    JsonRecordReader streamer = JsonRecordReader.getInst("/|/a/b", Arrays.asList("/a/x", "/a/b/*"));
    streamer.streamRecords(new StringReader(json), (record, path) -> {
      assertEquals(record.get("x"), "y");
      assertEquals(((Map) record.get("b")).get("c"), "d");
    });
    json = "{a:{" +
        "b:[{c:c1, e:e1},{c:c2, e :e2, d:{p:q}}]," +
        "x:y" +
        "}}";
    streamer.streamRecords(new StringReader(json), (record, path) -> {
      assertEquals(record.get("x"), "y");
      @SuppressWarnings({"rawtypes"})
      List l = (List) record.get("b");
      @SuppressWarnings({"rawtypes"})
      Map m = (Map) l.get(0);
      assertEquals(m.get("c"), "c1");
      assertEquals(m.get("e"), "e1");
      m = (Map) l.get(1);
      assertEquals(m.get("c"), "c2");
      assertEquals(m.get("e"), "e2");
    });
    streamer = JsonRecordReader.getInst("/|/a/b", Arrays.asList("$FQN:/**"));
    streamer.streamRecords(new StringReader(json), (record, path) -> {
      assertEquals(record.get("a.x"), "y");
      @SuppressWarnings({"rawtypes"})
      List l = (List) record.get("b");
      @SuppressWarnings({"rawtypes"})
      Map m = (Map) l.get(0);
      assertEquals(m.get("c"), "c1");
      assertEquals(m.get("e"), "e1");
      m = (Map) l.get(1);
      assertEquals(m.get("c"), "c2");
      assertEquals(m.get("e"), "e2");
      assertEquals(m.get("d.p"), "q");
    });
  }

  public void testNestedJsonWithFloats() throws Exception {

    String json = "{\n" +
        "        \"a_string\" : \"abc\",\n" +
        "        \"a_num\" : 2.0,\n" +
        "        \"a\" : {\n" +
        "                        \"b\" : [\n" +
        "                                {\"id\":\"1\", \"title\" : \"test1\"},\n" +
        "                                {\"id\":\"2\", \"title\" : \"test2\"}\n" +
        "                        ]\n" +
        "                }\n" +
        "}\n";

    JsonRecordReader streamer;
    List<Map<String, Object>> records;

    streamer = JsonRecordReader.getInst("/a/b", Collections.singletonList("title_s:/a/b/title"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(2, records.size());
  }

  public void testClearPreviousRecordFields() throws Exception {
    String json = "{\n" +
        "'first': 'John',\n" +
        "'exams': [\n" +
        "{'subject': 'Maths', 'test'   : 'term1', 'marks':90},\n" +
        "{'subject': 'Biology', 'test'   : 'term1', 'marks':86}\n" +
        "]\n" +
        "}\n" +
        "{\n" +
        "'first': 'Bob',\n" +
        "'exams': [\n" +
        "{'subject': 'Maths', 'test': 'term1', 'marks': 95\n" +
        "}\n" +
        ",\n" +
        "{\n" +
        "'subject': 'Biology', 'test'   : 'term1', 'marks': 92}\n" +
        "]\n" +
        "}";


    JsonRecordReader streamer;
    List<Map<String, Object>> records;

    streamer = JsonRecordReader.getInst("/exams", Collections.singletonList("/**"));
    records = streamer.getAllRecords(new StringReader(json));
    assertEquals(4, records.size());

    for (Map<String, Object> record : records) {
      for (Map.Entry<String, Object> e : record.entrySet()) {
        assertFalse(e.getValue() instanceof List);
      }
    }
  }

  public void testArrayOfRootObjects() throws Exception {
    String json = "[{'fieldA':'A1'}, {'fieldB':'B2'}]";
    JsonRecordReader streamer;
    List<Map<String, Object>> records;

    final AtomicReference<WeakReference<String>> ref = new AtomicReference<>();
    streamer = JsonRecordReader.getInst("/", Collections.singletonList("$FQN:/**"));
    streamer.streamRecords(new StringReader(json), (record, path) -> {
      System.gc();
      if (ref.get() != null) {
        assertNull("This reference is still intact :" + ref.get().get(), ref.get().get());
      }
      String fName = record.keySet().iterator().next();
      ref.set(new WeakReference<>(fName));
    });


  }

  public void testAIOOBE() throws IOException {
  String json = "[   {\n" +
      "      \"taxon_group\" : {\n" +
      "         \"identifiers\" : {\n" +
      "            \"bioentry_id\" : 1876284,\n" +
      "            \"namespace\" : \"NCBI\",\n" +
      "            \"primary_id\" : \"AAAAA_ID_19303\",\n" +
      "            \"version\" : null,\n" +
      "            \"name\" : \"AAAAA_ID_19303\",\n" +
      "            \"description\" : \"Taxon group for NCBI taxon 1286265\",\n" +
      "            \"accession\" : \"AAAAA_ID_19303\"\n" +
      "         },\n" +
      "         \"source\" : [\n" +
      "            {\n" +
      "               \"ID\" : \"AAAAA_ID_19303_0\",\n" +
      "               \"segment_group_serotype\" : \"H3\",\n" +
      "               \"primary_key\" : 11877892,\n" +
      "               \"name\" : \"segment_group\",\n" +
      "               \"segment_group_NA_subtype\" : \"\",\n" +
      "               \"segmentgroup_sequence_count\" : \"1\",\n" +
      "               \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "               \"segment_group_submitter_lab\" : \"Microbiology, Faculty of Medicine Sebelas Maret University, Jl. Ir. Sutami 36A, Surakarta, Jawa Tengah 57126, Indonesia\",\n" +
      "               \"strand\" : \"1\",\n" +
      "               \"segment_group_submission_date\" : \"22-JAN-2013\",\n" +
      "               \"segment_group_HA_subtype\" : \"H3\",\n" +
      "               \"start-end\" : \"1..260\"\n" +
      "            },\n" +
      "            {\n" +
      "               \"taxon_group_serotype\" : \"H3\",\n" +
      "               \"taxon_group_sequence_count\" : \"1\",\n" +
      "               \"ID\" : \"AAAAA_ID_19303\",\n" +
      "               \"primary_key\" : 11877893,\n" +
      "               \"name\" : \"source\",\n" +
      "               \"db_xref\" : [\n" +
      "                  \"taxon:1286265\"\n" +
      "               ],\n" +
      "               \"taxon_group_HA_subtype\" : \"H3\",\n" +
      "               \"taxon_group_segment_group_count\" : \"1\",\n" +
      "               \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "               \"strand\" : \"1\",\n" +
      "               \"taxon_group_NA_subtype\" : \"\",\n" +
      "               \"start-end\" : \"1..260\"\n" +
      "            }\n" +
      "         ],\n" +
      "         \"segment_groups\" : [\n" +
      "            {\n" +
      "               \"identifiers\" : {\n" +
      "                  \"bioentry_id\" : 1876283,\n" +
      "                  \"namespace\" : \"NCBI\",\n" +
      "                  \"primary_id\" : \"AAAAA_ID_19303_0\",\n" +
      "                  \"version\" : null,\n" +
      "                  \"name\" : \"AAAAA_ID_19303_0\",\n" +
      "                  \"description\" : \"Segment group 0 for AAAAA_ID_19303 (NCBI taxon 1286265)\",\n" +
      "                  \"accession\" : \"AAAAA_ID_19303_0\"\n" +
      "               },\n" +
      "               \"source\" : [\n" +
      "                  {\n" +
      "                     \"ID\" : \"KC513508\",\n" +
      "                     \"primary_key\" : 22483564,\n" +
      "                     \"nucleotide_gi\" : \"451898947\",\n" +
      "                     \"name\" : \"gene\",\n" +
      "                     \"HA_subtype\" : \"H3\",\n" +
      "                     \"segment\" : \"4\",\n" +
      "                     \"collection_date\" : \"22-Mar-2010\",\n" +
      "                     \"NA_subtype\" : \"N2\",\n" +
      "                     \"subtype\" : \"H3\",\n" +
      "                     \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "                     \"standardized_collection_date\" : \"2010-03-22T00:00:00Z\",\n" +
      "                     \"segment_name\" : \"HA\",\n" +
      "                     \"strand\" : \"1\",\n" +
      "                     \"serotype\" : \"H3N2\",\n" +
      "                     \"ncbi_accession\" : \"KC513508\",\n" +
      "                     \"start-end\" : \"1..260\"\n" +
      "                  },\n" +
      "                  {\n" +
      "                     \"mol_type\" : \"viral cRNA\",\n" +
      "                     \"taxonomy_strain\" : \"A/Surakarta/1/2010\",\n" +
      "                     \"identified_by\" : \"Afiono Agung Prasetyo\",\n" +
      "                     \"HA_subtype\" : \"H3\",\n" +
      "                     \"collection_date\" : \"22-Mar-2010\",\n" +
      "                     \"standardized_collection_date\" : \"2010-03-22T00:00:00Z\",\n" +
      "                     \"strand\" : \"1\",\n" +
      "                     \"serotype\" : \"H3N2\",\n" +
      "                     \"organism\" : \"Influenza A virus (A/Surakarta/1/2010(H3N2))\",\n" +
      "                     \"country\" : \"Indonesia\",\n" +
      "                     \"primary_key\" : 22483566,\n" +
      "                     \"isolation_source\" : \"nasal and throat swab\",\n" +
      "                     \"collected_by\" : \"Afiono Agung Prasetyo\",\n" +
      "                     \"name\" : \"source\",\n" +
      "                     \"flu_type\" : \"A\",\n" +
      "                     \"host\" : \"Homo sapiens\",\n" +
      "                     \"db_xref\" : [\n" +
      "                        \"taxon:1286265\"\n" +
      "                     ],\n" +
      "                     \"NA_subtype\" : \"N2\",\n" +
      "                     \"strain\" : \"A/Surakarta/1/2010\",\n" +
      "                     \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "                     \"start-end\" : \"1..260\"\n" +
      "                  },\n" +
      "                  {\n" +
      "                     \"primary_key\" : 22483572,\n" +
      "                     \"name\" : \"standard_host\",\n" +
      "                     \"Host_NCBI_taxon_id\" : \"9605\",\n" +
      "                     \"curation_status_code\" : \"150\",\n" +
      "                     \"curation_date\" : \"2015-01-12\",\n" +
      "                     \"program_version\" : \"v1.1.7\",\n" +
      "                     \"source_tag\" : \"parse_host_v1\",\n" +
      "                     \"strand\" : \"1\",\n" +
      "                     \"curation_status_message\" : \"success, archive and add on next major program revision\",\n" +
      "                     \"start-end\" : \"1..260\",\n" +
      "                     \"curation_status\" : \"true\"\n" +
      "                  },\n" +
      "                  {\n" +
      "                     \"primary_key\" : 22483576,\n" +
      "                     \"Location_Lat_Long\" : \"-0.7892749906,113.9213256836\",\n" +
      "                     \"name\" : \"standardized_location\",\n" +
      "                     \"Location_Country_Alpha2\" : \"ID\",\n" +
      "                     \"curation_status_code\" : \"150\",\n" +
      "                     \"curation_date\" : \"2015-01-15\",\n" +
      "                     \"program_version\" : \"v0.1\",\n" +
      "                     \"source_tag\" : \"xxxxx_parse_location_v0\",\n" +
      "                     \"strand\" : \"1\",\n" +
      "                     \"curation_status_message\" : \"success, archive and add on next major program revision\",\n" +
      "                     \"start-end\" : 1,\n" +
      "                     \"curation_status\" : \"true\"\n" +
      "                  }\n" +
      "               ],\n" +
      "               \"sequence\" : {\n" +
      "                  \"string\" : \"CCCTTATGATGTGCCGGATTATGCCTCCCTTAGGTCACTAGTTGCCTCATCCGGCACACTGGAGTTTAACAGTGAAAGCTTCAATTGGACTGGAGTCACTCAAAACGGAACAAGCTCTGCTTGCATAAGGAGATCTAATAATAGTTTCTTTAGTAGATTGAATTGGTTGACCCACTTAAACTTCAAATACCCAGCATTGAACGTGACTATGCCAAACAATGAACAATTTGACAAATTGTACATTTGGGGGGTTCACCACC\"\n" +
      "               },\n" +
      "               \"references\" : [\n" +
      "                  {\n" +
      "                     \"authors\" : \"Prasetyo,A.A.\",\n" +
      "                     \"location\" : \"Unpublished\",\n" +
      "                     \"title\" : \"Molecular Epidemiology Study of Human Respiratory Virus in Surakarta Indonesia\"\n" +
      "                  },\n" +
      "                  {\n" +
      "                     \"authors\" : \"Prasetyo,A.A.\",\n" +
      "                     \"location\" : \"Submitted (22-JAN-2013) Microbiology, Faculty of Medicine Sebelas Maret University, Jl. Ir. Sutami 36A, Surakarta, Jawa Tengah 57126, Indonesia\",\n" +
      "                     \"title\" : \"Direct Submission\"\n" +
      "                  }\n" +
      "               ],\n" +
      "               \"name\" : \"AAAAA_ID_19303_0\",\n" +
      "               \"segments\" : [\n" +
      "                  {\n" +
      "                     \"identifiers\" : {\n" +
      "                        \"bioentry_id\" : 1588885,\n" +
      "                        \"namespace\" : \"NCBI\",\n" +
      "                        \"primary_id\" : \"KC513508\",\n" +
      "                        \"version\" : 1,\n" +
      "                        \"name\" : \"KC513508\",\n" +
      "                        \"description\" : \"Influenza A virus (A/Surakarta/1/2010(H3N2)) segment 4 hemagglutinin (HA) gene, partial cds.\",\n" +
      "                        \"accession\" : \"KC513508\"\n" +
      "                     },\n" +
      "                     \"source\" : [\n" +
      "                        {\n" +
      "                           \"mol_type\" : \"viral cRNA\",\n" +
      "                           \"identified_by\" : \"Afiono Agung Prasetyo\",\n" +
      "                           \"HA_subtype\" : \"H3\",\n" +
      "                           \"segment\" : \"4\",\n" +
      "                           \"collection_date\" : \"22-Mar-2010\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"serotype\" : \"H3N2\",\n" +
      "                           \"organism\" : \"Influenza A virus (A/Surakarta/1/2010(H3N2))\",\n" +
      "                           \"country\" : \"Indonesia\",\n" +
      "                           \"primary_key\" : 22511042,\n" +
      "                           \"isolation_source\" : \"nasal and throat swab\",\n" +
      "                           \"collected_by\" : \"Afiono Agung Prasetyo\",\n" +
      "                           \"name\" : \"source\",\n" +
      "                           \"host\" : \"Homo sapiens\",\n" +
      "                           \"NA_subtype\" : \"N2\",\n" +
      "                           \"db_xref\" : [\n" +
      "                              \"taxon:1286265\"\n" +
      "                           ],\n" +
      "                           \"strain\" : \"A/Surakarta/1/2010\",\n" +
      "                           \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "                           \"start-end\" : \"1..260\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511045,\n" +
      "                           \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "                           \"gene\" : \"HA\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"name\" : \"gene\",\n" +
      "                           \"start-end\" : \"1..260\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511046,\n" +
      "                           \"protein_id\" : \"AGF80141.1\",\n" +
      "                           \"gene\" : \"HA\",\n" +
      "                           \"name\" : \"CDS\",\n" +
      "                           \"db_xref\" : [\n" +
      "                              \"GI:451898948\"\n" +
      "                           ],\n" +
      "                           \"codon_start\" : \"2\",\n" +
      "                           \"source_tag\" : \"EMBL/GenBank/SwissProt\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"translation\" : \"PYDVPDYASLRSLVASSGTLEFNSESFNWTGVTQNGTSSACIRRSNNSFFSRLNWLTHLNFKYPALNVTMPNNEQFDKLYIWGVHH\",\n" +
      "                           \"product\" : \"hemagglutinin\",\n" +
      "                           \"start-end\" : \"1..260\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511053,\n" +
      "                           \"name\" : \"asdf_typing\",\n" +
      "                           \"segment\" : \"4\",\n" +
      "                           \"flu_type\" : \"A\",\n" +
      "                           \"bitscore\" : \"277.3\",\n" +
      "                           \"full_lineage\" : \"X_XX_XX_XxxxxXxxxx\",\n" +
      "                           \"lineage\" : \"AAAAAAAAAA\",\n" +
      "                           \"curation_date\" : \"2015-01-07\",\n" +
      "                           \"subtype\" : \"H3\",\n" +
      "                           \"program_version\" : \"v2.8.2\",\n" +
      "                           \"source_tag\" : \"some_text_some\",\n" +
      "                           \"Evalue\" : \"4.6e-85\",\n" +
      "                           \"segment_name\" : \"HA\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"start-end\" : \"1..260\",\n" +
      "                           \"curation_status\" : \"true\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"bitscore\" : \"464\",\n" +
      "                           \"subj_location\" : \"342..601\",\n" +
      "                           \"slen\" : \"1701\",\n" +
      "                           \"sseqid\" : \"someID_someID_some\",\n" +
      "                           \"mismatch\" : \"3\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"qcovs\" : \"100\",\n" +
      "                           \"qlen\" : \"260\",\n" +
      "                           \"qcovhsp\" : \"100\",\n" +
      "                           \"primary_key\" : 22511048,\n" +
      "                           \"pident\" : \"98.85\",\n" +
      "                           \"name\" : \"segtypeAlign\",\n" +
      "                           \"qseqid\" : \"KC513508\",\n" +
      "                           \"gaps\" : \"0\",\n" +
      "                           \"curation_date\" : \"2015-01-15\",\n" +
      "                           \"typing\" : \"A_HA_H3\",\n" +
      "                           \"length\" : \"260\",\n" +
      "                           \"evalue\" : \"2e-132\",\n" +
      "                           \"source_tag\" : \"asdf_asdf_asdfv1\",\n" +
      "                           \"program_version\" : \"v1.1.2\",\n" +
      "                           \"curation_status\" : \"true\",\n" +
      "                           \"start-end\" : \"1..260\",\n" +
      "                           \"gapopen\" : \"0\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511060,\n" +
      "                           \"name\" : \"standard_host\",\n" +
      "                           \"Host_NCBI_taxon_id\" : \"9605\",\n" +
      "                           \"curation_status_code\" : \"150\",\n" +
      "                           \"curation_date\" : \"2015-01-12\",\n" +
      "                           \"program_version\" : \"v1.1.7\",\n" +
      "                           \"source_tag\" : \"parse_host_v1\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"curation_status_message\" : \"success, archive and add on next major program revision\",\n" +
      "                           \"start-end\" : \"1..260\",\n" +
      "                           \"curation_status\" : \"true\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511052,\n" +
      "                           \"name\" : \"flu_segtype\",\n" +
      "                           \"flu_type\" : \"A\",\n" +
      "                           \"curation_date\" : \"2015-01-15\",\n" +
      "                           \"subtype\" : \"H3\",\n" +
      "                           \"program_version\" : \"v1.1.2\",\n" +
      "                           \"source_tag\" : \"asdf_asdf_asd_v1\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"segtype\" : \"HA\",\n" +
      "                           \"start-end\" : \"1..260\",\n" +
      "                           \"curation_status\" : \"true\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511056,\n" +
      "                           \"prot_coord\" : \"115..87\",\n" +
      "                           \"name\" : \"exon\",\n" +
      "                           \"curation_date\" : \"2015-01-07\",\n" +
      "                           \"source_tag\" : \"asdf_asdfas_v2\",\n" +
      "                           \"program_version\" : \"v2.8.2\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"start-end\" : \"2..259\",\n" +
      "                           \"curation_status\" : \"true\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"nstart\" : \"2\",\n" +
      "                           \"primary_key\" : 22511058,\n" +
      "                           \"pend\" : \"200\",\n" +
      "                           \"aaseq\" : \"PYDVPDYASLRSLVASSGTLEFNSESFNWTGVTQNGTSSACIRRSNNSFFSRLNWLTHLNFKYPALNVTMPNNEQFDKLYIWGVHH\",\n" +
      "                           \"p_pc_coverage\" : \"15.19\",\n" +
      "                           \"name\" : \"CDS\",\n" +
      "                           \"score\" : \"461\",\n" +
      "                           \"nend\" : \"259\",\n" +
      "                           \"curation_date\" : \"2015-01-07\",\n" +
      "                           \"program_version\" : \"v2.8.2\",\n" +
      "                           \"source_tag\" : \"asdf_asdfas_v2\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"product\" : \"A_HA_H3\",\n" +
      "                           \"ntlength\" : \"258\",\n" +
      "                           \"start-end\" : \"1..260\",\n" +
      "                           \"curation_status\" : \"true\",\n" +
      "                           \"pstart\" : \"115\"\n" +
      "                        },\n" +
      "                        {\n" +
      "                           \"primary_key\" : 22511062,\n" +
      "                           \"Location_Lat_Long\" : \"-0.7892749906,113.9213256836\",\n" +
      "                           \"name\" : \"standardized_location\",\n" +
      "                           \"Location_Country_Alpha2\" : \"ID\",\n" +
      "                           \"curation_status_code\" : \"150\",\n" +
      "                           \"curation_date\" : \"2015-01-15\",\n" +
      "                           \"program_version\" : \"v0.1\",\n" +
      "                           \"source_tag\" : \"asdf_asdf_asdf_asdfn_v0\",\n" +
      "                           \"strand\" : \"1\",\n" +
      "                           \"curation_status_message\" : \"success, archive and add on next major program revision\",\n" +
      "                           \"start-end\" : 1,\n" +
      "                           \"curation_status\" : \"true\"\n" +
      "                        }\n" +
      "                     ],\n" +
      "                     \"sequence\" : {\n" +
      "                        \"length\" : 260,\n" +
      "                        \"string\" : \"CCCTTATGATGTGCCGGATTATGCCTCCCTTAGGTCACTAGTTGCCTCATCCGGCACACTGGAGTTTAACAGTGAAAGCTTCAATTGGACTGGAGTCACTCAAAACGGAACAAGCTCTGCTTGCATAAGGAGATCTAATAATAGTTTCTTTAGTAGATTGAATTGGTTGACCCACTTAAACTTCAAATACCCAGCATTGAACGTGACTATGCCAAACAATGAACAATTTGACAAATTGTACATTTGGGGGGTTCACCACC\"\n" +
      "                     },\n" +
      "                     \"name\" : \"KC513508\",\n" +
      "                     \"annotations\" : {\n" +
      "                        \"keyword\" : \"\",\n" +
      "                        \"curation_date\" : \"2015-01-03\",\n" +
      "                        \"comment\" : [\n" +
      "                           \"##Assembly-Data-START## Assembly Method :: CLC Main Workbench v. 6.8 Sequencing Technology :: Sanger dideoxy1 sequencing ##Assembly-Data-END## \"\n" +
      "                        ],\n" +
      "                        \"date_changed\" : \"25-FEB-2013\",\n" +
      "                        \"curation_status\" : \"false\"\n" +
      "                     }\n" +
      "                  }\n" +
      "               ],\n" +
      "               \"annotations\" : {\n" +
      "                  \"keyword\" : \"\",\n" +
      "                  \"curation_date\" : \"2015-01-05\",\n" +
      "                  \"comment\" : [\n" +
      "                     \"##Assembly-Data-START## Assembly Method :: CLC Main Workbench v. 6.8 Sequencing Technology :: Sanger dideoxy sequencing ##Assembly-Data-END## \"\n" +
      "                  ]\n" +
      "               }}]}}]";

    RecordingJSONParser parser = new RecordingJSONParser(new StringReader(json));
    JsonRecordReader recordReader = JsonRecordReader.getInst("/",Collections.singletonList("/**"));
    try {
      recordReader.streamRecords(parser, (record, path) -> {
      });   /*don't care*/
    } catch (RuntimeException e) {
      parser.error("").printStackTrace();
      throw e;
    }
  }
}
