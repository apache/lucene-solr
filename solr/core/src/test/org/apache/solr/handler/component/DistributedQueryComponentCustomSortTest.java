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
package org.apache.solr.handler.component;

import java.util.List;
import org.apache.solr.common.util.NamedList;

import org.apache.solr.common.SolrDocumentList;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Test for QueryComponent's distributed querying
 *
 * @see org.apache.solr.handler.component.QueryComponent
 */
public class DistributedQueryComponentCustomSortTest extends BaseDistributedSearchTestCase {

  public DistributedQueryComponentCustomSortTest() {
    stress = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-custom-field.xml");
  }

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    del("*:*");

    index(id, "1", "text", "a", "val", "25", "payload", ByteBuffer.wrap(new byte[] { 0x12, 0x62, 0x15 }),                     //  2 
          // quick check to prove "*" dynamicField hasn't been broken by somebody mucking with schema
          "asdfasdf_field_should_match_catchall_dynamic_field_adsfasdf", "value");
    index(id, "2", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x16 }));                    //  5 
    index(id, "3", "text", "a", "val", "25", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x32, 0x58 }));                    //  8 
    index(id, "4", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x15 }));                    //  4 
    index(id, "5", "text", "a", "val", "25", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x35, 0x10, 0x00 }));              //  9 
    index(id, "6", "text", "c", "val", "18", "payload", ByteBuffer.wrap(new byte[] { 0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03 }));  //  3 
    index(id, "7", "text", "c", "val", "18", "payload", ByteBuffer.wrap(new byte[] { 0x00, 0x3c, 0x73 }));                    //  1 
    index(id, "8", "text", "c", "val", "18", "payload", ByteBuffer.wrap(new byte[] { 0x59, 0x2d, 0x4d }));                    // 11 
    index(id, "9", "text", "a", "val", "25", "payload", ByteBuffer.wrap(new byte[] { 0x39, 0x79, 0x7a }));                    // 10 
    index(id, "10", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c }));                   //  6 
    index(id, "11", "text", "d", "val", "94", "payload", ByteBuffer.wrap(new byte[] { (byte)0xff, (byte)0xaf, (byte)0x9c })); // 13 
    index(id, "12", "text", "d", "val", "94", "payload", ByteBuffer.wrap(new byte[] { 0x34, (byte)0xdd, 0x4d }));             //  7 
    index(id, "13", "text", "d", "val", "94", "payload", ByteBuffer.wrap(new byte[] { (byte)0x80, 0x11, 0x33 }));             // 12 
    commit();

    QueryResponse rsp;

    rsp = query("q", "*:*", "fl", "id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11); 
    rsp = query("q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12, 10, 2, 4, 6, 1, 7);

    // SOLR-6744
    rsp = query("q", "*:*", "fl", "key:id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), "key", 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);
    rsp = query("q", "*:*", "fl", "key:id,id:text", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), "key", 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);
    
    rsp = query("q", "text:a", "fl", "id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 1, 3, 5, 9);
    rsp = query("q", "text:a", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 9, 5, 3, 1);
    
    rsp = query("q", "text:b", "fl", "id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 4, 2, 10);
    rsp = query("q", "text:b", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 10, 2, 4);

    // SOLR-6744
    rsp = query("q", "text:b", "fl", "key:id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, null, null, null);

    rsp = query("q", "text:c", "fl", "id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 7, 6, 8);
    rsp = query("q", "text:c", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 8, 6, 7);
    
    rsp = query("q", "text:d", "fl", "id", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 12, 13, 11);
    rsp = query("q", "text:d", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 12);

    // sanity check function sorting
    rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                "sort", "abs(sub(5,id)) asc, id desc");
    assertFieldValues(rsp.getResults(), id, 5 , 6,4 , 7,3 , 8,2 , 9,1 , 10 );

    // Add two more docs with same payload as in doc #4 
    index(id, "14", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x15 })); 
    index(id, "15", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x15 })); 

    // Add three more docs with same payload as in doc #10
    index(id, "16", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c })); 
    index(id, "17", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c })); 
    index(id, "18", "text", "b", "val", "10", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c }));
    
    commit();
    
    rsp = query("q", "*:*", "fl", "id", "sort", "payload asc, id desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 7, 1, 6,   15,14,4,   2,   18,17,16,10,   12, 3, 5, 9, 8, 13, 11);
    rsp = query("q", "*:*", "fl", "id", "sort", "payload desc, id asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12,   10,16,17,18,   2,   4,14,15,   6, 1, 7);

    // SOLR-6203
    {
      final String sort;
      final int[] expectedIds;
      if (random().nextBoolean()) {
        // non-function sorting
        sort = "payload desc, id asc";
        expectedIds = new int[]{ 6, 1, 2 };
      } else {
        // function sorting
        sort = "abs(sub(5,id)) asc, id desc";
        expectedIds = new int[]{ 1, 6, 2 };
      }
      rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
          "sort", sort, "group", "true", "group.field", "val", "group.sort", "id asc");

      NamedList grouped = (NamedList)rsp.getResponse().get("grouped");
      assertNotNull(grouped);
      NamedList valFieldList = (NamedList)grouped.get("val");
      assertEquals(10, valFieldList.get("matches"));

      List<NamedList> groupLists = (List)valFieldList.get("groups");
      assertEquals(expectedIds.length, groupLists.size());
      for (int ii=0; ii<expectedIds.length; ++ii) {
        NamedList groupList = (NamedList)groupLists.get(ii);
        assertNotNull(groupList);
        assertFieldValues((SolrDocumentList)groupList.get("doclist"), id, expectedIds[ii]);
      }
    }

    // function sorting, limit 1
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "sub(5,id) asc", "group", "true", "group.field", "val", "group.sort", "id asc", "group.limit", 1);
        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 2);

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);


        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 1);

        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 6);
    }


    // function sorting (group.sort desc)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "id desc");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 9);

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 8);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 10);
    }



    // function sorting (group.sort desc)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) asc, val desc", "group", "true", "group.field", "val", "group.sort", "id desc");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 9);

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 8);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 10);
    }


    // function sorting (group.limit=2)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) desc, id desc", "group", "true", "group.field", "val", "group.sort", "sum(3,id) asc", "group.limit", "2");


        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 2, 4);

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 1, 3);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 6, 7);
    }


    //  function sorting (group.limit=3)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "sum(3,id) asc", "group.limit", "3");


        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 1, 3, 5);

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 6, 7, 8);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 2, 4, 10);
    }


    //  function sorting (start=2)
    //  Pagination is on level of groups, not individual offers. 
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20", "start", "2",
                    "sort", "abs(sub(5,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "sum(3,id) asc", "group.limit", "3");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(1, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 2, 4, 10);
    }


    //  function sorting (group.offset=1)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "sum(3,id) asc", "group.limit", "3", "group.offset", "1");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 3, 5,9 );

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 7, 8);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 4, 10);
    }

    //  function sorting (group.offset=2)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "sum(3,id) asc", "group.limit", "3", "group.offset", "2");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 5, 9 );

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id,  8);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);

        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 10);
    }

    // function sorting (different sort and group.sort functions)
    {
        rsp = query("q", "id:[1 TO 10]", "fl", "id", "rows", "20",
                    "sort", "sum(val,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "id asc", "group.limit", "3");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(10, valFieldList.get("matches"));
        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(3, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);


        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 2, 4, 10);
        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 1, 3, 5);


        NamedList groupList2 = (NamedList)groupLists.get(2);
        assertNotNull(groupList2);
        assertFieldValues((SolrDocumentList)groupList2.get("doclist"), id, 6, 7, 8);

    }

    // function sorting (id:[1 TO 2])
    {
        rsp = query("q", "id:[1 TO 2]", "fl", "id", "rows", "20",
                    "sort", "abs(sub(5,id)) asc, id desc", "group", "true", "group.field", "val", "group.sort", "id asc");

        NamedList grouped = (NamedList)rsp.getResponse().get("grouped");

        assertNotNull(grouped);

        NamedList valFieldList = (NamedList)grouped.get("val");
        assertEquals(2, valFieldList.get("matches"));

        List<NamedList> groupLists = (List)valFieldList.get("groups");
        assertEquals(2, groupLists.size());

        NamedList groupList0 = (NamedList)groupLists.get(0);
        assertNotNull(groupList0);

        assertFieldValues((SolrDocumentList)groupList0.get("doclist"), id, 2);

        NamedList groupList1 = (NamedList)groupLists.get(1);
        assertNotNull(groupList1);

        assertFieldValues((SolrDocumentList)groupList1.get("doclist"), id, 1);
    }
  }
}
