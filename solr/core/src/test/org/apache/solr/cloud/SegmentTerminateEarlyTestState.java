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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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

  final String keyField = "id";
  final String timestampField = "timestamp";
  final String oddField = "odd_l1"; // <dynamicField name="*_l1"  type="long"   indexed="true"  stored="true" multiValued="false"/>
  final String quadField = "quad_l1"; // <dynamicField name="*_l1"  type="long"   indexed="true"  stored="true" multiValued="false"/>

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
        final Integer docKey = new Integer(numDocs);
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField(keyField, ""+docKey);
        final int MM = rand.nextInt(60); // minutes
        if (minTimestampMM == null || MM <= minTimestampMM.intValue()) {
          if (minTimestampMM != null && MM < minTimestampMM.intValue()) {
            minTimestampDocKeys.clear();
          }
          minTimestampMM = new Integer(MM);
          minTimestampDocKeys.add(docKey);
        }
        if (maxTimestampMM == null || maxTimestampMM.intValue() <= MM) {
          if (maxTimestampMM != null && maxTimestampMM.intValue() < MM) {
            maxTimestampDocKeys.clear();
          }
          maxTimestampMM = new Integer(MM);
          maxTimestampDocKeys.add(docKey);
        }
        doc.setField(timestampField, ZonedDateTime.of(2016, 1, 1, 0, MM, 0, 0, ZoneOffset.UTC).toInstant().toString());
        doc.setField(oddField, ""+(numDocs % 2));
        doc.setField(quadField, ""+(numDocs % 4)+1);
        cloudSolrClient.add(doc);
      }
      cloudSolrClient.commit();
    }
    if (optimize) {
      cloudSolrClient.optimize();
    }
  }

  void queryTimestampDescending(CloudSolrClient cloudSolrClient) throws Exception {
    TestMiniSolrCloudCluster.assertFalse(maxTimestampDocKeys.isEmpty());
    TestMiniSolrCloudCluster.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = new Long(maxTimestampDocKeys.iterator().next().intValue()%2);
    final SolrQuery query = new SolrQuery(oddField+":"+oddFieldValue);
    query.setSort(timestampField, SolrQuery.ORDER.desc);
    query.setFields(keyField, oddField, timestampField);
    query.setRows(1);
    // CommonParams.SEGMENT_TERMINATE_EARLY parameter intentionally absent
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestMiniSolrCloudCluster.assertEquals("numFound", numDocs/2, rsp.getResults().getNumFound());
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      TestMiniSolrCloudCluster.assertTrue(keyField+" of ("+solrDocument0+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
          maxTimestampDocKeys.contains(solrDocument0.getFieldValue(keyField)));
      TestMiniSolrCloudCluster.assertEquals(oddField, oddFieldValue, solrDocument0.getFieldValue(oddField));
    }
    // check segmentTerminatedEarly flag
    TestMiniSolrCloudCluster.assertNull("responseHeader.segmentTerminatedEarly present in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
  }

  void queryTimestampDescendingSegmentTerminateEarlyYes(CloudSolrClient cloudSolrClient) throws Exception {
    TestMiniSolrCloudCluster.assertFalse(maxTimestampDocKeys.isEmpty());
    TestMiniSolrCloudCluster.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = new Long(maxTimestampDocKeys.iterator().next().intValue()%2);
    final SolrQuery query = new SolrQuery(oddField+":"+oddFieldValue);
    query.setSort(timestampField, SolrQuery.ORDER.desc);
    query.setFields(keyField, oddField, timestampField);
    final int rowsWanted = 1;
    query.setRows(rowsWanted);
    final Boolean shardsInfoWanted = (rand.nextBoolean() ? null : new Boolean(rand.nextBoolean()));
    if (shardsInfoWanted != null) {
      query.set(ShardParams.SHARDS_INFO, shardsInfoWanted.booleanValue());
    }
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, true);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestMiniSolrCloudCluster.assertTrue("numFound", rowsWanted <= rsp.getResults().getNumFound());
    TestMiniSolrCloudCluster.assertTrue("numFound", rsp.getResults().getNumFound() <= numDocs/2);
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      TestMiniSolrCloudCluster.assertTrue(keyField+" of ("+solrDocument0+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
          maxTimestampDocKeys.contains(solrDocument0.getFieldValue(keyField)));
      TestMiniSolrCloudCluster.assertEquals(oddField, oddFieldValue, rsp.getResults().get(0).getFieldValue(oddField));
    }
    // check segmentTerminatedEarly flag
    TestMiniSolrCloudCluster.assertNotNull("responseHeader.segmentTerminatedEarly missing in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
    TestMiniSolrCloudCluster.assertTrue("responseHeader.segmentTerminatedEarly missing/false in "+rsp.getResponseHeader(),
        Boolean.TRUE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
    // check shards info
    final Object shardsInfo = rsp.getResponse().get(ShardParams.SHARDS_INFO);
    if (!Boolean.TRUE.equals(shardsInfoWanted)) {
      TestMiniSolrCloudCluster.assertNull(ShardParams.SHARDS_INFO, shardsInfo);
    } else {
      TestMiniSolrCloudCluster.assertNotNull(ShardParams.SHARDS_INFO, shardsInfo);
      int segmentTerminatedEarlyShardsCount = 0;
      for (Map.Entry<String, ?> si : (SimpleOrderedMap<?>)shardsInfo) {
        if (Boolean.TRUE.equals(((SimpleOrderedMap)si.getValue()).get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY))) {
          segmentTerminatedEarlyShardsCount += 1;
        }
      }
      // check segmentTerminatedEarly flag within shards info
      TestMiniSolrCloudCluster.assertTrue(segmentTerminatedEarlyShardsCount+" shards reported "+SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY,
          (0<segmentTerminatedEarlyShardsCount));
    }
  }

  void queryTimestampDescendingSegmentTerminateEarlyNo(CloudSolrClient cloudSolrClient) throws Exception {
    TestMiniSolrCloudCluster.assertFalse(maxTimestampDocKeys.isEmpty());
    TestMiniSolrCloudCluster.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = new Long(maxTimestampDocKeys.iterator().next().intValue()%2);
    final SolrQuery query = new SolrQuery(oddField+":"+oddFieldValue);
    query.setSort(timestampField, SolrQuery.ORDER.desc);
    query.setFields(keyField, oddField, timestampField);
    query.setRows(1);
    final Boolean shardsInfoWanted = (rand.nextBoolean() ? null : new Boolean(rand.nextBoolean()));
    if (shardsInfoWanted != null) {
      query.set(ShardParams.SHARDS_INFO, shardsInfoWanted.booleanValue());
    }
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, false);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestMiniSolrCloudCluster.assertEquals("numFound", numDocs/2, rsp.getResults().getNumFound());
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      TestMiniSolrCloudCluster.assertTrue(keyField+" of ("+solrDocument0+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
          maxTimestampDocKeys.contains(solrDocument0.getFieldValue(keyField)));
      TestMiniSolrCloudCluster.assertEquals(oddField, oddFieldValue, rsp.getResults().get(0).getFieldValue(oddField));
    }
    // check segmentTerminatedEarly flag
    TestMiniSolrCloudCluster.assertNull("responseHeader.segmentTerminatedEarly present in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
    TestMiniSolrCloudCluster.assertFalse("responseHeader.segmentTerminatedEarly present/true in "+rsp.getResponseHeader(),
        Boolean.TRUE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
    // check shards info
    final Object shardsInfo = rsp.getResponse().get(ShardParams.SHARDS_INFO);
    if (!Boolean.TRUE.equals(shardsInfoWanted)) {
      TestMiniSolrCloudCluster.assertNull(ShardParams.SHARDS_INFO, shardsInfo);
    } else {
      TestMiniSolrCloudCluster.assertNotNull(ShardParams.SHARDS_INFO, shardsInfo);
      int segmentTerminatedEarlyShardsCount = 0;
      for (Map.Entry<String, ?> si : (SimpleOrderedMap<?>)shardsInfo) {
        if (Boolean.TRUE.equals(((SimpleOrderedMap)si.getValue()).get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY))) {
          segmentTerminatedEarlyShardsCount += 1;
        }
      }
      TestMiniSolrCloudCluster.assertEquals("shards reporting "+SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY,
          0, segmentTerminatedEarlyShardsCount);
    }
  }

  void queryTimestampDescendingSegmentTerminateEarlyYesGrouped(CloudSolrClient cloudSolrClient) throws Exception {
    TestMiniSolrCloudCluster.assertFalse(maxTimestampDocKeys.isEmpty());
    TestMiniSolrCloudCluster.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = new Long(maxTimestampDocKeys.iterator().next().intValue()%2);
    final SolrQuery query = new SolrQuery(oddField+":"+oddFieldValue);
    query.setSort(timestampField, SolrQuery.ORDER.desc);
    query.setFields(keyField, oddField, timestampField);
    query.setRows(1);
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, true);
    TestMiniSolrCloudCluster.assertTrue("numDocs="+numDocs+" is not quad-able", (numDocs%4)==0);
    query.add("group.field", quadField);
    query.set("group", true);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestMiniSolrCloudCluster.assertEquals("matches", numDocs/2, rsp.getGroupResponse().getValues().get(0).getMatches());
    // check correctness of the first result
    if (rsp.getGroupResponse().getValues().get(0).getMatches() > 0) {
      final SolrDocument solrDocument = rsp.getGroupResponse().getValues().get(0).getValues().get(0).getResult().get(0);
      TestMiniSolrCloudCluster.assertTrue(keyField+" of ("+solrDocument+") is not in maxTimestampDocKeys("+maxTimestampDocKeys+")",
          maxTimestampDocKeys.contains(solrDocument.getFieldValue(keyField)));
      TestMiniSolrCloudCluster.assertEquals(oddField, oddFieldValue, solrDocument.getFieldValue(oddField));
    }
    // check segmentTerminatedEarly flag
    // at present segmentTerminateEarly cannot be used with grouped queries
    TestMiniSolrCloudCluster.assertFalse("responseHeader.segmentTerminatedEarly present/true in "+rsp.getResponseHeader(),
        Boolean.TRUE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
  }

  void queryTimestampAscendingSegmentTerminateEarlyYes(CloudSolrClient cloudSolrClient) throws Exception {
    TestMiniSolrCloudCluster.assertFalse(minTimestampDocKeys.isEmpty());
    TestMiniSolrCloudCluster.assertTrue("numDocs="+numDocs+" is not even", (numDocs%2)==0);
    final Long oddFieldValue = new Long(minTimestampDocKeys.iterator().next().intValue()%2);
    final SolrQuery query = new SolrQuery(oddField+":"+oddFieldValue);
    query.setSort(timestampField, SolrQuery.ORDER.asc); // a sort order that is _not_ compatible with the merge sort order
    query.setFields(keyField, oddField, timestampField);
    query.setRows(1);
    query.set(CommonParams.SEGMENT_TERMINATE_EARLY, true);
    final QueryResponse rsp = cloudSolrClient.query(query);
    // check correctness of the results count
    TestMiniSolrCloudCluster.assertEquals("numFound", numDocs/2, rsp.getResults().getNumFound());
    // check correctness of the first result
    if (rsp.getResults().getNumFound() > 0) {
      final SolrDocument solrDocument0 = rsp.getResults().get(0);
      TestMiniSolrCloudCluster.assertTrue(keyField+" of ("+solrDocument0+") is not in minTimestampDocKeys("+minTimestampDocKeys+")",
          minTimestampDocKeys.contains(solrDocument0.getFieldValue(keyField)));
      TestMiniSolrCloudCluster.assertEquals(oddField, oddFieldValue, solrDocument0.getFieldValue(oddField));
    }
    // check segmentTerminatedEarly flag
    TestMiniSolrCloudCluster.assertNotNull("responseHeader.segmentTerminatedEarly missing in "+rsp.getResponseHeader(),
        rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
    // segmentTerminateEarly cannot be used with incompatible sort orders
    TestMiniSolrCloudCluster.assertTrue("responseHeader.segmentTerminatedEarly missing/true in "+rsp.getResponseHeader(),
        Boolean.FALSE.equals(rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY)));
  }
}
