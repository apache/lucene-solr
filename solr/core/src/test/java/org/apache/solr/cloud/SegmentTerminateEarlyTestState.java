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

package org.apache.solr.cloud;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;

class SegmentTerminateEarlyTestState {

  static final String KEY_FIELD = "id";

  // for historic reasons, this is refered to as a "timestamp" field, but in actuallity is just an int
  // value representing a number of "minutes" between 0-60.
  // aka: I decided not to rename a million things while refactoring this test
  public static final String TIMESTAMP_FIELD = "timestamp_i_dvo";
  public static final String ODD_FIELD = "odd_l1"; // <dynamicField name="*_l1"  type="long"   indexed="true"  stored="true" multiValued="false"/>
  public static final String QUAD_FIELD = "quad_l1"; // <dynamicField name="*_l1"  type="long"   indexed="true"  stored="true" multiValued="false"/>

  final Set<Integer> minTimestampDocKeys = new HashSet<>();
  final Set<Integer> maxTimestampDocKeys = new HashSet<>();

  Integer minTimestampMM = null;
  Integer maxTimestampMM = null;

  int numDocs = 0;
  final Random rand;

  public SegmentTerminateEarlyTestState(Random rand) {
    this.rand = rand;
  }
  
  void addDocuments(CloudSolrClient cloudSolrClient,
      int numCommits, int numDocsPerCommit, boolean optimize) throws Exception {
    for (int cc = 1; cc <= numCommits; ++cc) {
      for (int nn = 1; nn <= numDocsPerCommit; ++nn) {
        ++numDocs;
        final Integer docKey = numDocs;
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField(KEY_FIELD, ""+docKey);
        final int MM = rand.nextInt(60); // minutes
        if (minTimestampMM == null || MM <= minTimestampMM.intValue()) {
          if (minTimestampMM != null && MM < minTimestampMM.intValue()) {
            minTimestampDocKeys.clear();
          }
          minTimestampMM = MM;
          minTimestampDocKeys.add(docKey);
        }
        if (maxTimestampMM == null || maxTimestampMM.intValue() <= MM) {
          if (maxTimestampMM != null && maxTimestampMM.intValue() < MM) {
            maxTimestampDocKeys.clear();
          }
          maxTimestampMM = MM;
          maxTimestampDocKeys.add(docKey);
        }
        doc.setField(TIMESTAMP_FIELD, MM);
        doc.setField(ODD_FIELD, ""+(numDocs % 2));
        doc.setField(QUAD_FIELD, ""+(numDocs % 4)+1);
        cloudSolrClient.add(doc);
      }
      cloudSolrClient.commit();
    }
    if (optimize) {
      cloudSolrClient.optimize();
    }
  }

  void queryTimestampDescending(CloudSolrClient cloudSolrClient) throws Exception {
    TestSegmentSorting.assertFalse(maxTimestampDocKeys.isEmpty());
    TestSegmentSorting.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = (long) (maxTimestampDocKeys.iterator().next().intValue() % 2);
    final SolrQuery query = new SolrQuery(ODD_FIELD +":"+oddFieldValue);
    query.setSort(TIMESTAMP_FIELD, SolrQuery.ORDER.desc);
    query.setFields(KEY_FIELD, ODD_FIELD, TIMESTAMP_FIELD);
    query.setRows(1);
    // CommonParams.SEGMENT_TERMINATE_EARLY parameter intentionally absent
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestSegmentSorting.assertEquals("numFound", numDocs/2, rsp.getResults().getNumFound());
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      final Integer idAsInt = Integer.parseInt(solrDocument0.getFieldValue(KEY_FIELD).toString());
      TestSegmentSorting.assertTrue
        (KEY_FIELD +"="+idAsInt+" of ("+solrDocument0+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
         maxTimestampDocKeys.contains(idAsInt));
      TestSegmentSorting.assertEquals(ODD_FIELD, oddFieldValue, solrDocument0.getFieldValue(ODD_FIELD));
    }
    // check segmentTerminatedEarly flag
    TestSegmentSorting.assertNull("responseHeader.segmentTerminatedEarly present in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
  }

  void queryTimestampDescendingSegmentTerminateEarlyYes(CloudSolrClient cloudSolrClient) throws Exception {
    TestSegmentSorting.assertFalse(maxTimestampDocKeys.isEmpty());
    TestSegmentSorting.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = (long) (maxTimestampDocKeys.iterator().next().intValue() % 2);
    final SolrQuery query = new SolrQuery(ODD_FIELD +":"+oddFieldValue);
    query.setSort(TIMESTAMP_FIELD, SolrQuery.ORDER.desc);
    query.setFields(KEY_FIELD, ODD_FIELD, TIMESTAMP_FIELD);
    final int rowsWanted = 1;
    query.setRows(rowsWanted);
    final Boolean shardsInfoWanted = (rand.nextBoolean() ? null : rand.nextBoolean());
    if (shardsInfoWanted != null) {
      query.set(ShardParams.SHARDS_INFO, shardsInfoWanted.booleanValue());
    }
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, true);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestSegmentSorting.assertTrue("numFound", rowsWanted <= rsp.getResults().getNumFound());
    TestSegmentSorting.assertTrue("numFound", rsp.getResults().getNumFound() <= numDocs/2);
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      final Integer idAsInt = Integer.parseInt(solrDocument0.getFieldValue(KEY_FIELD).toString());
      TestSegmentSorting.assertTrue
        (KEY_FIELD +"="+idAsInt+" of ("+solrDocument0+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
         maxTimestampDocKeys.contains(idAsInt));
      TestSegmentSorting.assertEquals(ODD_FIELD, oddFieldValue, rsp.getResults().get(0).getFieldValue(ODD_FIELD));
    }
    // check segmentTerminatedEarly flag
    TestSegmentSorting.assertNotNull("responseHeader.segmentTerminatedEarly missing in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
    TestSegmentSorting.assertTrue("responseHeader.segmentTerminatedEarly missing/false in "+rsp.getResponseHeader(),
        Boolean.TRUE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
    // check shards info
    final Object shardsInfo = rsp.getResponse().get(ShardParams.SHARDS_INFO);
    if (!Boolean.TRUE.equals(shardsInfoWanted)) {
      TestSegmentSorting.assertNull(ShardParams.SHARDS_INFO, shardsInfo);
    } else {
      TestSegmentSorting.assertNotNull(ShardParams.SHARDS_INFO, shardsInfo);
      int segmentTerminatedEarlyShardsCount = 0;
      for (Map.Entry<String, ?> si : (SimpleOrderedMap<?>)shardsInfo) {
        if (Boolean.TRUE.equals(((SimpleOrderedMap)si.getValue()).get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY))) {
          segmentTerminatedEarlyShardsCount += 1;
        }
      }
      // check segmentTerminatedEarly flag within shards info
      TestSegmentSorting.assertTrue(segmentTerminatedEarlyShardsCount+" shards reported "+SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY,
          (0<segmentTerminatedEarlyShardsCount));
    }
  }

  void queryTimestampDescendingSegmentTerminateEarlyNo(CloudSolrClient cloudSolrClient) throws Exception {
    TestSegmentSorting.assertFalse(maxTimestampDocKeys.isEmpty());
    TestSegmentSorting.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = (long) (maxTimestampDocKeys.iterator().next().intValue() % 2);
    final SolrQuery query = new SolrQuery(ODD_FIELD +":"+oddFieldValue);
    query.setSort(TIMESTAMP_FIELD, SolrQuery.ORDER.desc);
    query.setFields(KEY_FIELD, ODD_FIELD, TIMESTAMP_FIELD);
    query.setRows(1);
    final Boolean shardsInfoWanted = (rand.nextBoolean() ? null : rand.nextBoolean());
    if (shardsInfoWanted != null) {
      query.set(ShardParams.SHARDS_INFO, shardsInfoWanted.booleanValue());
    }
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, false);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestSegmentSorting.assertEquals("numFound", numDocs/2, rsp.getResults().getNumFound());
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      final Integer idAsInt = Integer.parseInt(solrDocument0.getFieldValue(KEY_FIELD).toString());
      TestSegmentSorting.assertTrue
        (KEY_FIELD +"="+idAsInt+" of ("+solrDocument0+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
         maxTimestampDocKeys.contains(idAsInt));
      TestSegmentSorting.assertEquals(ODD_FIELD, oddFieldValue, rsp.getResults().get(0).getFieldValue(ODD_FIELD));
    }
    // check segmentTerminatedEarly flag
    TestSegmentSorting.assertNull("responseHeader.segmentTerminatedEarly present in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
    TestSegmentSorting.assertFalse("responseHeader.segmentTerminatedEarly present/true in "+rsp.getResponseHeader(),
        Boolean.TRUE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
    // check shards info
    final Object shardsInfo = rsp.getResponse().get(ShardParams.SHARDS_INFO);
    if (!Boolean.TRUE.equals(shardsInfoWanted)) {
      TestSegmentSorting.assertNull(ShardParams.SHARDS_INFO, shardsInfo);
    } else {
      TestSegmentSorting.assertNotNull(ShardParams.SHARDS_INFO, shardsInfo);
      int segmentTerminatedEarlyShardsCount = 0;
      for (Map.Entry<String, ?> si : (SimpleOrderedMap<?>)shardsInfo) {
        if (Boolean.TRUE.equals(((SimpleOrderedMap)si.getValue()).get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY))) {
          segmentTerminatedEarlyShardsCount += 1;
        }
      }
      TestSegmentSorting.assertEquals("shards reporting "+SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY,
          0, segmentTerminatedEarlyShardsCount);
    }
  }

  void queryTimestampDescendingSegmentTerminateEarlyYesGrouped(CloudSolrClient cloudSolrClient) throws Exception {
    TestSegmentSorting.assertFalse(maxTimestampDocKeys.isEmpty());
    TestSegmentSorting.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = (long) (maxTimestampDocKeys.iterator().next().intValue() % 2);
    final SolrQuery query = new SolrQuery(ODD_FIELD +":"+oddFieldValue);
    query.setSort(TIMESTAMP_FIELD, SolrQuery.ORDER.desc);
    query.setFields(KEY_FIELD, ODD_FIELD, TIMESTAMP_FIELD);
    query.setRows(1);
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, true);
    TestSegmentSorting.assertTrue("numDocs="+numDocs+" is not quad-able", (numDocs%4)==0);
    query.add("group.field", QUAD_FIELD);
    query.set("group", true);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestSegmentSorting.assertEquals("matches", numDocs/2, rsp.getGroupResponse().getValues().get(0).getMatches());
    // check correctness of the first result
    if (rsp.getGroupResponse().getValues().get(0).getMatches() > 0) {
      final SolrDocument solrDocument = rsp.getGroupResponse().getValues().get(0).getValues().get(0).getResult().get(0);
      final Integer idAsInt = Integer.parseInt(solrDocument.getFieldValue(KEY_FIELD).toString());
      TestSegmentSorting.assertTrue
        (KEY_FIELD +"="+idAsInt+" of ("+solrDocument+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
         maxTimestampDocKeys.contains(idAsInt));
      TestSegmentSorting.assertEquals(ODD_FIELD, oddFieldValue, solrDocument.getFieldValue(ODD_FIELD));
    }
    // check segmentTerminatedEarly flag
    // at present segmentTerminateEarly cannot be used with grouped queries
    TestSegmentSorting.assertFalse("responseHeader.segmentTerminatedEarly present/true in "+rsp.getResponseHeader(),
        Boolean.TRUE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
  }

  void queryTimestampAscendingSegmentTerminateEarlyYes(CloudSolrClient cloudSolrClient) throws Exception {
    TestSegmentSorting.assertFalse(minTimestampDocKeys.isEmpty());
    TestSegmentSorting.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = (long) (minTimestampDocKeys.iterator().next().intValue() % 2);
    final SolrQuery query = new SolrQuery(ODD_FIELD +":"+oddFieldValue);
    query.setSort(TIMESTAMP_FIELD, SolrQuery.ORDER.asc); // a sort order that is _not_ compatible with the merge sort order
    query.setFields(KEY_FIELD, ODD_FIELD, TIMESTAMP_FIELD);
    query.setRows(1);
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, true);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestSegmentSorting.assertEquals("numFound", numDocs/2, rsp.getResults().getNumFound());
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      final Integer idAsInt = Integer.parseInt(solrDocument0.getFieldValue(KEY_FIELD).toString());
      TestSegmentSorting.assertTrue
        (KEY_FIELD +"="+idAsInt+" of ("+solrDocument0+") is not in minTimestampDocKeys("+minTimestampDocKeys+")",
         minTimestampDocKeys.contains(idAsInt));
      TestSegmentSorting.assertEquals(ODD_FIELD, oddFieldValue, solrDocument0.getFieldValue(ODD_FIELD));
    }
    // check segmentTerminatedEarly flag
    TestSegmentSorting.assertNotNull("responseHeader.segmentTerminatedEarly missing in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
    // segmentTerminateEarly cannot be used with incompatible sort orders
    TestSegmentSorting.assertTrue("responseHeader.segmentTerminatedEarly missing/true in "+rsp.getResponseHeader(),
        Boolean.FALSE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
  }
}
