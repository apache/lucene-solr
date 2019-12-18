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

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import org.apache.solr.util.LogLevel;
import org.apache.solr.util.hll.HLL;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-9062")
@LogLevel("org.eclipse.jetty.client.HttpConnection=DEBUG")
public class TestDistributedStatsComponentCardinality extends BaseDistributedSearchTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  final static HashFunction HASHER = Hashing.murmur3_128();

  final static long BIG_PRIME = 982451653L;

  final static int MIN_NUM_DOCS = 10000;
  final static int MAX_NUM_DOCS = MIN_NUM_DOCS * 2;

  final static List<String> STAT_FIELDS = 
    Collections.unmodifiableList(Arrays.asList( "int_i", "long_l", "string_s" ));

  final int NUM_DOCS;
  final long MAX_LONG;
  final long MIN_LONG;

  public TestDistributedStatsComponentCardinality() {
    // we need DVs on point fields to compute stats
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    // we want some randomness in the shard number, but we don't want multiple iterations
    fixShardCount(TEST_NIGHTLY ? 7 : random().nextInt(3) + 1);

    handle.put("maxScore", SKIPVAL);
    NUM_DOCS = TestUtil.nextInt(random(), 10000, 15000);
    MAX_LONG = TestUtil.nextLong(random(), 0, NUM_DOCS * BIG_PRIME);
    MIN_LONG = MAX_LONG - (((long)NUM_DOCS-1) * BIG_PRIME);
  }

  /** CAUTION: this builds a very large index */
  public void buildIndex() throws Exception {
    log.info("Building an index of {} docs", NUM_DOCS);

    // we want a big spread in the long values we use, decrement by BIG_PRIME as we index
    long longValue = MAX_LONG;

    for (int i = 1; i <= NUM_DOCS; i++) {
      // with these values, we know that every doc indexed has a unique value in all of the
      // fields we will compute cardinality against.
      // which means the number of docs matching a query is the true cardinality for each field

      final String strValue = "s"+longValue;
      indexDoc(sdoc("id","" + i, 
                    "int_i", ""+i,
                    "int_i_prehashed_l", ""+HASHER.hashInt(i).asLong(),
                    "long_l", ""+longValue, 
                    "long_l_prehashed_l", ""+HASHER.hashLong(longValue).asLong(),
                    "string_s", strValue,
                    "string_s_prehashed_l", ""+HASHER.hashString(strValue, StandardCharsets.UTF_8).asLong()));

      longValue -= BIG_PRIME;
    }

    commit();
    
  }

  public void test() throws Exception {
    buildIndex();
    
    { // simple sanity checks - don't leak variables
      QueryResponse rsp = null;
      rsp = query(params("rows", "0", "q", "id:42")); 
      assertEquals(1, rsp.getResults().getNumFound());
      
      rsp = query(params("rows", "0", "q", "*:*", 
                         "stats","true", "stats.field", "{!min=true max=true}long_l"));
      assertEquals(NUM_DOCS, rsp.getResults().getNumFound());
      assertEquals(MIN_LONG, Math.round((double) rsp.getFieldStatsInfo().get("long_l").getMin()));
      assertEquals(MAX_LONG, Math.round((double) rsp.getFieldStatsInfo().get("long_l").getMax()));
    }

    final int NUM_QUERIES = atLeast(100);

    // Some Randomized queries with randomized log2m and max regwidth
    for (int i = 0; i < NUM_QUERIES; i++) {

      // testing shows that on random data, at the size we're dealing with, 
      // MINIMUM_LOG2M_PARAM is just too absurdly small to give anything remotely close the 
      // the theoretically expected relative error.
      //
      // So we have to use a slightly higher lower bound on what log2m values we randomly test
      final int log2m = TestUtil.nextInt(random(), 
                                         2 + HLL.MINIMUM_LOG2M_PARAM, 
                                         HLL.MAXIMUM_LOG2M_PARAM);

      // use max regwidth to try and prevent hash collisions from introducing problems
      final int regwidth = HLL.MAXIMUM_REGWIDTH_PARAM;

      final int lowId = TestUtil.nextInt(random(), 1, NUM_DOCS-2000);
      final int highId = TestUtil.nextInt(random(), lowId+1000, NUM_DOCS);
      final int numMatches = 1+highId-lowId;

      SolrParams p = buildCardinalityQ(lowId, highId, log2m, regwidth);
      QueryResponse rsp = query(p);
      assertEquals("sanity check num matches, p="+p, numMatches, rsp.getResults().getNumFound());

      Map<String,FieldStatsInfo> stats = rsp.getFieldStatsInfo();

      if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) {
        log.warn("SOLR-10918: can't relying on exact match with pre-hashed values when using points");
      } else {
        for (String f : STAT_FIELDS) {
          // regardless of log2m and regwidth, the estimated cardinality of the 
          // hashed vs prehashed values should be exactly the same for each field
          
          assertEquals(f + ": hashed vs prehashed, real="+ numMatches + ", p=" + p,
                       stats.get(f).getCardinality().longValue(),
                       stats.get(f+"_prehashed_l").getCardinality().longValue());
        }
      }

      for (String f : STAT_FIELDS) {
        // check the relative error of the estimate returned against the known truth

        final double relErr = expectedRelativeError(log2m);
        final long estimate = stats.get(f).getCardinality().longValue();
        assertTrue(f + ": relativeErr="+relErr+", estimate="+estimate+", real="+numMatches+", p=" + p,
                   (Math.abs(numMatches - estimate) / numMatches) < relErr);
        
      }
    }
    
    // Some Randomized queries with both low and high accuracy options
    for (int i = 0; i < NUM_QUERIES; i++) {

      final int lowId = TestUtil.nextInt(random(), 1, NUM_DOCS-2000);
      final int highId = TestUtil.nextInt(random(), lowId+1000, NUM_DOCS);
      final int numMatches = 1+highId-lowId;

      // WTF? - https://github.com/aggregateknowledge/java-hll/issues/15
      // 
      // aparently we can't rely on estimates always being more accurate with higher log2m values?
      // so for now, just try testing accuracy values that differ by at least 0.5
      //
      // (that should give us a significant enough log2m diff that the "highAccuracy" is always
      // more accurate -- if, not then the entire premise of the float value is fundementally bogus)
      // 
      final double lowAccuracy = random().nextDouble() / 2;
      // final double highAccuracy = Math.min(1.0D, lowAccuracy + (random().nextDouble() / 2));
      final double highAccuracy = Math.min(1.0D, lowAccuracy + 0.5D);

      SolrParams p = buildCardinalityQ(lowId, highId, lowAccuracy, highAccuracy);
      QueryResponse rsp = query(p);
      assertEquals("sanity check num matches, p="+p, numMatches, rsp.getResults().getNumFound());

      Map<String,FieldStatsInfo> stats = rsp.getFieldStatsInfo();

      // can't use STAT_FIELDS here ...
      //
      // hueristic differences for regwidth on 32 bit values mean we get differences 
      // between estimates for the normal field vs the prehashed (long) field
      //
      // so we settle for only testing things where the regwidth is consistent 
      // w/the prehashed long...
      for (String f : new String[] { "long_l", "string_s" }) {

        // regardless of accuracy, the estimated cardinality of the 
        // hashed vs prehashed values should be exactly the same for each field

        assertEquals(f + ": hashed vs prehashed (low), real="+ numMatches + ", p=" + p,
                     stats.get("low_"+f).getCardinality().longValue(),
                     stats.get("low_"+f+"_prehashed_l").getCardinality().longValue());
        assertEquals(f + ": hashed vs prehashed (high), real="+ numMatches + ", p=" + p,
                     stats.get("high_"+f).getCardinality().longValue(),
                     stats.get("high_"+f+"_prehashed_l").getCardinality().longValue());
      }
      
      for (String f : STAT_FIELDS) {
        for (String ff : new String[] { f, f+"_prehashed_l"}) {
          // for both the prehashed and regular fields, the high accuracy option 
          // should always produce an estimate at least as good as the low accuracy option
          
          long poorEst = stats.get("low_"+ff).getCardinality();
          long goodEst = stats.get("high_"+ff).getCardinality();
          assertTrue(ff + ": goodEst="+goodEst+", poorEst="+poorEst+", real="+numMatches+", p=" + p,
                     Math.abs(numMatches - goodEst) <= Math.abs(numMatches - poorEst));
        }
      }
    }
  }
    
  /**
   * Returns the (max) expected relative error according ot the HLL algorithm docs
   */
  private static double expectedRelativeError(final int log2m) {
    final long m = 1 << log2m;
    // theoretical error is 1.04D * sqrt(m)
    // fudge slightly to account for variance in random data
    return 1.1D / Math.sqrt(m);
  }

  /** 
   * Helper utility for building up a set of query params.  
   *
   * The main query is a simple range query against the id field (using lowId TO highId). 
   * 2 stats.field params are generated for every field in {@link #STAT_FIELDS} --
   * both with and w/o a prehashed_l suffix -- using the specified log2m and regwidth.
   * 
   * The response keys will be the full field names
   */
  private static SolrParams buildCardinalityQ(final int lowId, 
                                              final int highId, 
                                              final int log2m, 
                                              final int regwidth) {
    ModifiableSolrParams p = params("q", "id_i1:["+lowId+" TO "+highId+"]", 
                                    "rows", "0", "stats", "true");
    final String prefix = "{!cardinality=true hllLog2m="+log2m+" hllRegwidth="+regwidth;
    for (String f : STAT_FIELDS) {
      p.add("stats.field", prefix+"}"+f);
      p.add("stats.field", prefix+" hllPreHashed=true}"+f+"_prehashed_l");
    }
    return p;
  }

  /** 
   * Helper utility for building up a set of query params.  
   *
   * The main query is a simple range query against the id field (using lowId TO highId). 
   * 4 stats.field params are generated for every field in {@link #STAT_FIELDS} --
   * both with and w/o a prehashed_l suffix, and using both the low and high accuracy values
   *
   * The response keys will be the full field names with either a "low_" or "high_" prefix
   */
  private static SolrParams buildCardinalityQ(final int lowId, 
                                              final int highId, 
                                              final double lowAccuracy,
                                              final double highAccuracy) {
    ModifiableSolrParams p = params("q", "id_i1:["+lowId+" TO "+highId+"]", 
                                    "rows", "0", "stats", "true");
    final String[] prefixes = new String[] {
      "{!cardinality=" + lowAccuracy + " key=low_",
      "{!cardinality=" + highAccuracy + " key=high_"
    };

    for (String f : STAT_FIELDS) {
      for (String prefix : prefixes) {
        p.add("stats.field", prefix+f+"}"+f);
        p.add("stats.field", prefix+f+"_prehashed_l hllPreHashed=true}"+f+"_prehashed_l");
      }
    }
    return p;
  }
}
