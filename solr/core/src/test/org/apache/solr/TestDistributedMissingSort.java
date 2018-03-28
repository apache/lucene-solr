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
package org.apache.solr;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

/**
 * Tests sortMissingFirst and sortMissingLast in distributed sort
 */
@Slow
public class TestDistributedMissingSort extends BaseDistributedSearchTestCase {

  public TestDistributedMissingSort() {
    schemaString = "schema-distributed-missing-sort.xml";
  }
  
  String sint1_ml = "one_i1_ml";    // int field, sortMissingLast=true, multiValued=false
  String sint1_mf = "two_i1_mf";    // int field, sortMissingFirst=true, multiValued=false
  String long1_ml = "three_l1_ml";  // long field, sortMissingLast=true, multiValued=false
  String long1_mf = "four_l1_mf";   // long field, sortMissingFirst=true, multiValued=false
  String string1_ml = "five_s1_ml"; // StringField, sortMissingLast=true, multiValued=false
  String string1_mf = "six_s1_mf";  // StringField, sortMissingFirst=true, multiValued=false

  @Test
  public void test() throws Exception {
    index();
    testSortMissingLast();
    testSortMissingFirst();
  }

  private void index() throws Exception {
    del("*:*");
    indexr(id,1, sint1_ml, 100, sint1_mf, 100, long1_ml, 100, long1_mf, 100,
        "foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d,
        string1_ml, "DE", string1_mf, "DE");
    indexr(id,2, sint1_ml, 50, sint1_mf, 50, long1_ml, 50, long1_mf, 50,
        string1_ml, "ABC", string1_mf, "ABC");
    indexr(id,3, sint1_ml, 2, sint1_mf, 2, long1_ml, 2, long1_mf, 2,
        string1_ml, "HIJK", string1_mf, "HIJK");
    indexr(id,4, sint1_ml, -100, sint1_mf, -100, long1_ml, -101, long1_mf, -101,
        string1_ml, "L  M", string1_mf, "L  M");
    indexr(id,5, sint1_ml, 500, sint1_mf, 500, long1_ml, 500, long1_mf, 500,
        string1_ml, "YB", string1_mf, "YB");
    indexr(id,6, sint1_ml, -600, sint1_mf, -600, long1_ml, -600, long1_mf, -600,
        string1_ml, "WX", string1_mf, "WX");
    indexr(id,7, sint1_ml, 123, sint1_mf, 123, long1_ml, 123, long1_mf, 123,
        string1_ml, "N", string1_mf, "N");
    indexr(id,8, sint1_ml, 876, sint1_mf, 876, long1_ml, 876, long1_mf, 876,
        string1_ml, "QRS", string1_mf, "QRS");
    indexr(id,9, sint1_ml, 7, sint1_mf, 7, long1_ml, 7, long1_mf, 7,
        string1_ml, "P", string1_mf, "P");

    commit();  // try to ensure there's more than one segment

    indexr(id,10, sint1_ml, 4321, sint1_mf, 4321, long1_ml, 4321, long1_mf, 4321,
        string1_ml, "O", string1_mf, "O");
    indexr(id,11, sint1_ml, -987, sint1_mf, -987, long1_ml, -987, long1_mf, -987,
        string1_ml, "YA", string1_mf, "YA");
    indexr(id,12, sint1_ml, 379, sint1_mf, 379, long1_ml, 379, long1_mf, 379,
        string1_ml, "TUV", string1_mf, "TUV");
    indexr(id,13, sint1_ml, 232, sint1_mf, 232, long1_ml, 232, long1_mf, 232,
        string1_ml, "F G", string1_mf, "F G");

    indexr(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i=0; i<100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);

    for (int i=100; i<150; i++) {
      indexr(id, i);
    }

    commit();

    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("_version_", SKIPVAL); // not a cloud test, but may use updateLog
  }
  
  private void testSortMissingLast() throws Exception {
    // id field values:         1     2     3     4     5     6     7     8     9    10    11    12    13
    // sint1_ml field values: 100    50     2  -100   500  -600   123   876     7  4321  -987   379   232
    // sint1_ml asc sort pos:   7     6     4     3    11     2     8    12     5    13     1    10     9
    // sint1_ml desc sort pos:  7     8    10    11     3    12     6     2     9     1    13     4     5

    QueryResponse rsp = query("q","*:*", "sort", sint1_ml + " desc", "rows", "13");
    assertFieldValues(rsp.getResults(), "id_i", 10, 8, 5, 12, 13, 7, 1, 2, 9, 3, 4, 6, 11);

    rsp = query("q","*:*", "sort", sint1_ml + " asc", "rows", "13");
    assertFieldValues(rsp.getResults(), "id_i", 11, 6, 4, 3, 9, 2, 1, 7, 13, 12, 5, 8, 10);

    rsp = query("q","*:*", "sort", sint1_ml + " desc, id_i asc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        10, 8, 5, 12, 13, 7, 1, 2, 9, 3, 4, 6, 11,
        14, 15, 16, 17,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
        140, 141, 142, 143, 144, 145, 146, 147, 148, 149);

    rsp = query("q","*:*", "sort", sint1_ml + " asc, id_i desc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        11, 6, 4, 3, 9, 2, 1, 7, 13, 12, 5, 8, 10,
        149, 148, 147, 146, 145, 144, 143, 142, 141, 140,
        139, 138, 137, 136, 135, 134, 133, 132, 131, 130,
        129, 128, 127, 126, 125, 124, 123, 122, 121, 120,
        119, 118, 117, 116, 115, 114, 113, 112, 111, 110,
        109, 108, 107, 106, 105, 104, 103, 102, 101, 100,
        17, 16, 15, 14);

    // id field values:         1     2     3     4     5     6     7     8     9    10    11    12    13
    // long1_ml field values: 100    50     2  -100   500  -600   123   876     7  4321  -987   379   232
    // long1_ml asc sort pos:   7     6     4     3    11     2     8    12     5    13     1    10     9
    // long1_ml desc sort pos:  7     8    10    11     3    12     6     2     9     1    13     4     5

    rsp = query("q","*:*", "sort", long1_ml + " desc", "rows", "13");
    assertFieldValues(rsp.getResults(), "id_i", 10, 8, 5, 12, 13, 7, 1, 2, 9, 3, 4, 6, 11);

    rsp = query("q","*:*", "sort", long1_ml + " asc", "rows", "13");
    assertFieldValues(rsp.getResults(), "id_i", 11, 6, 4, 3, 9, 2, 1, 7, 13, 12, 5, 8, 10);

    rsp = query("q","*:*", "sort", long1_ml + " desc, id_i asc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        10, 8, 5, 12, 13, 7, 1, 2, 9, 3, 4, 6, 11,
        14, 15, 16, 17,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
        140, 141, 142, 143, 144, 145, 146, 147, 148, 149);

    rsp = query("q","*:*", "sort", long1_ml + " asc, id_i desc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        11, 6, 4, 3, 9, 2, 1, 7, 13, 12, 5, 8, 10,
        149, 148, 147, 146, 145, 144, 143, 142, 141, 140,
        139, 138, 137, 136, 135, 134, 133, 132, 131, 130,
        129, 128, 127, 126, 125, 124, 123, 122, 121, 120,
        119, 118, 117, 116, 115, 114, 113, 112, 111, 110,
        109, 108, 107, 106, 105, 104, 103, 102, 101, 100,
        17, 16, 15, 14);

    
    // id field values:           1     2     3     4     5     6     7     8     9    10    11    12    13
    // string1_ml field values:  DE   ABC  HIJK  L  M    YB    WX     N   QRS     P     O    YA   TUV   F G
    // string1_ml asc sort pos:   2     1     4     5    13    11     6     9     8     7    12    10     3
    // string1_ml desc sort pos: 12    13    10     9     1     3     8     5     6     7     2     4    11

    rsp = query("q","*:*", "sort", string1_ml + " desc", "rows", "13");
    assertFieldValues(rsp.getResults(), "id_i", 5, 11, 6, 12, 8, 9, 10, 7, 4, 3, 13, 1, 2);

    rsp = query("q","*:*", "sort", string1_ml + " asc", "rows", "13");
    assertFieldValues(rsp.getResults(), "id_i", 2, 1, 13, 3, 4, 7, 10, 9, 8, 12, 6, 11, 5);

    rsp = query("q","*:*", "sort", string1_ml + " desc, id_i asc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        5, 11, 6, 12, 8, 9, 10, 7, 4, 3, 13, 1, 2,
        // missing field string1_ml="a_s1", ascending id sort
        14, 15, 16, 17,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
        140, 141, 142, 143, 144, 145, 146, 147, 148, 149);

    rsp = query("q","*:*", "sort", string1_ml + " asc, id_i desc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        2, 1, 13, 3, 4, 7, 10, 9, 8, 12, 6, 11, 5,
        // missing field string1_ml="a_s1", descending id sort
        149, 148, 147, 146, 145, 144, 143, 142, 141, 140,
        139, 138, 137, 136, 135, 134, 133, 132, 131, 130,
        129, 128, 127, 126, 125, 124, 123, 122, 121, 120,
        119, 118, 117, 116, 115, 114, 113, 112, 111, 110,
        109, 108, 107, 106, 105, 104, 103, 102, 101, 100,
        17, 16, 15, 14);
  }
  
  private void testSortMissingFirst() throws Exception {
    // id field values:         1     2     3     4     5     6     7     8     9    10    11    12    13
    // sint1_mf field values: 100    50     2  -100   500  -600   123   876     7  4321  -987   379   232
    // sint1_mf asc sort pos:   7     6     4     3    11     2     8    12     5    13     1    10     9
    // sint1_mf desc sort pos:  7     8    10    11     3    12     6     2     9     1    13     4     5

    QueryResponse rsp = query("q","*:*", "sort", sint1_mf + " desc, id_i asc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        14, 15, 16, 17,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
        140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
        10, 8, 5, 12, 13, 7, 1, 2, 9, 3, 4, 6, 11);

    rsp = query("q","*:*", "sort", sint1_mf + " asc, id_i desc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        149, 148, 147, 146, 145, 144, 143, 142, 141, 140,
        139, 138, 137, 136, 135, 134, 133, 132, 131, 130,
        129, 128, 127, 126, 125, 124, 123, 122, 121, 120,
        119, 118, 117, 116, 115, 114, 113, 112, 111, 110,
        109, 108, 107, 106, 105, 104, 103, 102, 101, 100,
        17, 16, 15, 14,
        11, 6, 4, 3, 9, 2, 1, 7, 13, 12, 5, 8, 10);


    // id field values:         1     2     3     4     5     6     7     8     9    10    11    12    13
    // long1_mf field values: 100    50     2  -100   500  -600   123   876     7  4321  -987   379   232
    // long1_mf asc sort pos:   7     6     4     3    11     2     8    12     5    13     1    10     9
    // long1_mf desc sort pos:  7     8    10    11     3    12     6     2     9     1    13     4     5

    rsp = query("q","*:*", "sort", long1_mf + " desc, id_i asc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        14, 15, 16, 17,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
        140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
        10, 8, 5, 12, 13, 7, 1, 2, 9, 3, 4, 6, 11);

    rsp = query("q","*:*", "sort", long1_mf + " asc, id_i desc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        149, 148, 147, 146, 145, 144, 143, 142, 141, 140,
        139, 138, 137, 136, 135, 134, 133, 132, 131, 130,
        129, 128, 127, 126, 125, 124, 123, 122, 121, 120,
        119, 118, 117, 116, 115, 114, 113, 112, 111, 110,
        109, 108, 107, 106, 105, 104, 103, 102, 101, 100,
        17, 16, 15, 14,
        11, 6, 4, 3, 9, 2, 1, 7, 13, 12, 5, 8, 10);

    
    // id field values:           1     2     3     4     5     6     7     8     9    10    11    12    13
    // string1_mf field values:  DE   ABC  HIJK  L  M    YB    WX     N   QRS     P     O    YA   TUV   F G
    // string1_mf asc sort pos:   2     1     4     5    13    11     6     9     8     7    12    10     3
    // string1_mf desc sort pos: 12    13    10     9     1     3     8     5     6     7     2     4    11

    rsp = query("q","*:*", "sort", string1_mf + " desc, id_i asc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        // missing field string1_mf="a_s1_mf", ascending id sort
        14, 15, 16, 17,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
        140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
        5, 11, 6, 12, 8, 9, 10, 7, 4, 3, 13, 1, 2);

    rsp = query("q","*:*", "sort", string1_mf + " asc, id_i desc", "rows", "200");
    assertFieldValues(rsp.getResults(), "id_i",
        // missing field string1_mf="a_s1_mf", descending id sort
        149, 148, 147, 146, 145, 144, 143, 142, 141, 140,
        139, 138, 137, 136, 135, 134, 133, 132, 131, 130,
        129, 128, 127, 126, 125, 124, 123, 122, 121, 120,
        119, 118, 117, 116, 115, 114, 113, 112, 111, 110,
        109, 108, 107, 106, 105, 104, 103, 102, 101, 100,
        17, 16, 15, 14,
        2, 1, 13, 3, 4, 7, 10, 9, 8, 12, 6, 11, 5);
  }
}
