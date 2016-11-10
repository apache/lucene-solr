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
package org.apache.solr.search.facet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.tdunning.math.stats.AVLTreeDigest;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.hll.HLL;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.macro.MacroExpander;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Lucene45","Appending"})
public class TestJsonFacets extends SolrTestCaseHS {

  private static SolrInstances servers;  // for distributed testing
  private static int origTableSize;
  private static FacetField.FacetMethod origDefaultFacetMethod;

  @BeforeClass
  public static void beforeTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = true;

    origTableSize = FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE;
    FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE=2; // stress test resizing

    origDefaultFacetMethod = FacetField.FacetMethod.DEFAULT_METHOD;
    // instead of the following, see the constructor
    //FacetField.FacetMethod.DEFAULT_METHOD = rand(FacetField.FacetMethod.values());

    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE=origTableSize;
    FacetField.FacetMethod.DEFAULT_METHOD = origDefaultFacetMethod;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
  }

  // tip: when debugging a test, comment out the @ParametersFactory and edit the constructor to be no-arg

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    // wrap each enum val in an Object[] and return as Iterable
    return () -> Arrays.stream(FacetField.FacetMethod.values()).map(it -> new Object[]{it}).iterator();
  }

  public TestJsonFacets(FacetField.FacetMethod defMethod) {
    FacetField.FacetMethod.DEFAULT_METHOD = defMethod; // note: the real default is restored in afterTests
  }

  // attempt to reproduce https://github.com/Heliosearch/heliosearch/issues/33
  @Test
  public void testComplex() throws Exception {
    Random r = random();

    Client client = Client.localClient;

    double price_low = 11000;
    double price_high = 100000;

    ModifiableSolrParams p = params("make_s","make_s", "model_s","model_s", "price_low",Double.toString(price_low), "price_high",Double.toString(price_high));


    MacroExpander m = new MacroExpander( p.getMap() );

    String make_s = m.expand("${make_s}");
    String model_s = m.expand("${model_s}");

    client.deleteByQuery("*:*", null);


    int nDocs = 99;
    String[] makes = {"honda", "toyota", "ford", null};
    Double[] prices = {10000.0, 30000.0, 50000.0, 0.0, null};
    String[] honda_models = {"accord", "civic", "fit", "pilot", null};  // make sure this is alphabetized to match tiebreaks in index
    String[] other_models = {"z1", "z2", "z3", "z4", "z5", "z6", null};

    int nHonda = 0;
    final int[] honda_model_counts = new int[honda_models.length];

    for (int i=0; i<nDocs; i++) {
      SolrInputDocument doc = sdoc("id", Integer.toString(i));

      Double price = rand(prices);
      if (price != null) {
        doc.addField("cost_f", price);
      }
      boolean matches_price = price!=null && price >= price_low && price <= price_high;

      String make = rand(makes);
      if (make != null) {
        doc.addField(make_s, make);
      }

      if ("honda".equals(make)) {
        int modelNum = r.nextInt(honda_models.length);
        String model = honda_models[modelNum];
        if (model != null) {
          doc.addField(model_s, model);
        }
        if (matches_price) {
          nHonda++;
          honda_model_counts[modelNum]++;
        }
      } else if (make == null) {
        doc.addField(model_s, rand(honda_models));  // add some docs w/ model but w/o make
      } else {
        // other makes
        doc.addField(model_s, rand(other_models));  // add some docs w/ model but w/o make
      }

      client.add(doc, null);
      if (r.nextInt(10) == 0) {
        client.add(doc, null);  // dup, causing a delete
      }
      if (r.nextInt(20) == 0) {
        client.commit();  // force new seg
      }
    }

    client.commit();

    // now figure out top counts
    List<Integer> idx = new ArrayList<>();
    for (int i=0; i<honda_model_counts.length-1; i++) {
      idx.add(i);
    }
    Collections.sort(idx, (o1, o2) -> {
      int cmp = honda_model_counts[o2] - honda_model_counts[o1];
      return cmp == 0 ? o1 - o2 : cmp;
    });



    // straight query facets
    client.testJQ(params(p, "q", "*:*", "rows","0", "fq","+${make_s}:honda +cost_f:[${price_low} TO ${price_high}]"
            , "json.facet", "{makes:{terms:{field:${make_s}, facet:{models:{terms:{field:${model_s}, limit:2, mincount:0}}}}}}}"
            , "facet","true", "facet.pivot","make_s,model_s", "facet.limit", "2"
        )
        , "facets=={count:" + nHonda + ", makes:{buckets:[{val:honda, count:" + nHonda + ", models:{buckets:["
           + "{val:" + honda_models[idx.get(0)] + ", count:" + honda_model_counts[idx.get(0)] + "},"
           + "{val:" + honda_models[idx.get(1)] + ", count:" + honda_model_counts[idx.get(1)] + "}]}"
           + "}]}}"
    );


  }


  public void indexSimple(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2", "val_b", "true", "sparse_s", "one"), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5", "val_b", "false"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ", "num_d", "2", "num_i", "3"), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7", "sparse_s", "two"),null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-5", "num_i", "-5"),null);
    client.commit();
  }

  @Test
  public void testMethodStream() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    // test multiple json.facet commands
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{x:'sum(num_d)'}"
            , "json.facet", "{y:'min(num_d)'}"
        )
        , "facets=={count:6 , x:3.0, y:-9.0 }"
    );


    // test streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream }}" + // won't stream; need sort:index asc
                              ", cat2:{terms:{field:'cat_s', method:stream, sort:'index asc' }}" +
                              ", cat3:{terms:{field:'cat_s', method:stream, sort:'index asc', mincount:3 }}" + // mincount
                              ", cat4:{terms:{field:'cat_s', method:stream, sort:'index asc', prefix:B }}" + // prefix
                              ", cat5:{terms:{field:'cat_s', method:stream, sort:'index asc', offset:1 }}" + // offset
                " }"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:B, count:3},{val:A, count:2}]}" +
            ", cat2:{buckets:[{val:A, count:2},{val:B, count:3}]}" +
            ", cat3:{buckets:[{val:B, count:3}]}" +
            ", cat4:{buckets:[{val:B, count:3}]}" +
            ", cat5:{buckets:[{val:B, count:3}]}" +
            " }"
    );


    // test nested streaming under non-streaming
    assertJQ(req("q", "*:*", "rows", "0"
        , "json.facet", "{   cat:{terms:{field:'cat_s', sort:'index asc', facet:{where:{terms:{field:where_s,method:stream,sort:'index asc'}}}   }}}"
        )
        , "facets=={count:6 " +
        ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1},{val:NY,count:1}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2},{val:NY,count:1}]}    }]}"
        + "}"
    );

    // test nested streaming under streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream,sort:'index asc', facet:{where:{terms:{field:where_s,method:stream,sort:'index asc'}}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1},{val:NY,count:1}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2},{val:NY,count:1}]}    }]}"
            + "}"
    );

    // test nested streaming with stats under streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream,sort:'index asc', facet:{  where:{terms:{field:where_s,method:stream,sort:'index asc',sort:'index asc', facet:{x:'max(num_d)'}     }}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1,x:2.0},{val:NY,count:1,x:4.0}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2,x:11.0},{val:NY,count:1,x:-5.0}]}    }]}"
            + "}"
    );

    // test nested streaming with stats under streaming with stats
    assertJQ(req("q", "*:*", "rows", "0",
            "facet","true"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream,sort:'index asc', facet:{ y:'min(num_d)',  where:{terms:{field:where_s,method:stream,sort:'index asc', facet:{x:'max(num_d)'}     }}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, y:2.0, where:{buckets:[{val:NJ,count:1,x:2.0},{val:NY,count:1,x:4.0}]}   },{val:B, count:3, y:-9.0, where:{buckets:[{val:NJ,count:2,x:11.0},{val:NY,count:1,x:-5.0}]}    }]}"
            + "}"
    );


    assertJQ(req("q", "*:*", "fq","cat_s:A")
        , "response/numFound==2"
    );
  }

  Map<String,String[]> suffixMap = new HashMap<>();
  {
    suffixMap.put("_s", new String[]{"_s","_ss","_sd","_sds"} );
    suffixMap.put("_ss", new String[]{"_ss","_sds"} );
    suffixMap.put("_l", new String[]{"_l","_ls","_ld","_lds"} );
    suffixMap.put("_ls", new String[]{"_ls","_lds"} );
    suffixMap.put("_i", new String[]{"_i","_is","_id","_ids", "_l","_ls","_ld","_lds"} );
    suffixMap.put("_is", new String[]{"_is","_ids", "_ls","_lds"} );
    suffixMap.put("_d", new String[]{"_d","_ds","_dd","_dds"} );
    suffixMap.put("_ds", new String[]{"_ds","_dds"} );
    suffixMap.put("_f", new String[]{"_f","_fs","_fd","_fds", "_d","_ds","_dd","_dds"} );
    suffixMap.put("_fs", new String[]{"_fs","_fds","_ds","_dds"} );
    suffixMap.put("_dt", new String[]{"_dt","_dts","_dtd","_dtds"} );
    suffixMap.put("_dts", new String[]{"_dts","_dtds"} );
    suffixMap.put("_b", new String[]{"_b"} );
  }

  List<String> getAlternatives(String field) {
    int idx = field.lastIndexOf("_");
    if (idx<=0 || idx>=field.length()) return Collections.singletonList(field);
    String suffix = field.substring(idx);
    String[] alternativeSuffixes = suffixMap.get(suffix);
    if (alternativeSuffixes == null) return Collections.singletonList(field);
    String base = field.substring(0, idx);
    List<String> out = new ArrayList<>(alternativeSuffixes.length);
    for (String altS : alternativeSuffixes) {
      out.add( base + altS );
    }
    Collections.shuffle(out, random());
    return out;
  }

  @Test
  public void testStats() throws Exception {
    doStats(Client.localClient, params("debugQuery", Boolean.toString(random().nextBoolean()) ));
  }

  @Test
  public void testStatsDistrib() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards(), "debugQuery", Boolean.toString(random().nextBoolean()) );
    doStats( client, params() );
  }

  public void doStats(Client client, ModifiableSolrParams p) throws Exception {

    Map<String, List<String>> fieldLists = new HashMap<>();
    fieldLists.put("noexist", getAlternatives("noexist_s"));
    fieldLists.put("cat_s", getAlternatives("cat_s"));
    fieldLists.put("where_s", getAlternatives("where_s"));
    fieldLists.put("num_d", getAlternatives("num_f")); // num_d name is historical, which is why we map it to num_f alternatives so we can include floats as well
    fieldLists.put("num_i", getAlternatives("num_i"));
    fieldLists.put("super_s", getAlternatives("super_s"));
    fieldLists.put("val_b", getAlternatives("val_b"));
    fieldLists.put("date", getAlternatives("date_dt"));
    fieldLists.put("sparse_s", getAlternatives("sparse_s"));
    fieldLists.put("multi_ss", getAlternatives("multi_ss"));

    // TODO: if a field will be used as a function source, we can't use multi-valued types for it (currently)

    int maxAlt = 0;
    for (List<String> fieldList : fieldLists.values()) {
      maxAlt = Math.max(fieldList.size(), maxAlt);
    }

    // take the field with the maximum number of alternative types and loop through our variants that many times
    for (int i=0; i<maxAlt; i++) {
      ModifiableSolrParams args = params(p);
      for (String field : fieldLists.keySet()) {
        List<String> alts = fieldLists.get(field);
        String alt = alts.get( i % alts.size() );
        args.add(field, alt);
      }

      args.set("rows","0");
      // doStatsTemplated(client, args);
    }


    // single valued strings
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_s",  "cat_s","cat_s", "where_s","where_s", "num_d","num_d", "num_i","num_i", "super_s","super_s", "val_b","val_b", "date","date_dt", "sparse_s","sparse_s"    ,"multi_ss","multi_ss") );

    // multi-valued strings, long/float substitute for int/double
    doStatsTemplated(client, params(p, "facet","true",       "rows","0", "noexist","noexist_ss", "cat_s","cat_ss", "where_s","where_ss", "num_d","num_f", "num_i","num_l", "num_is","num_ls", "num_fs", "num_ds", "super_s","super_ss", "val_b","val_b", "date","date_dt", "sparse_s","sparse_ss", "multi_ss","multi_ss") );

    // multi-valued strings, method=dv for terms facets
    doStatsTemplated(client, params(p, "terms", "method:dv,", "rows", "0", "noexist", "noexist_ss", "cat_s", "cat_ss", "where_s", "where_ss", "num_d", "num_f", "num_i", "num_l", "super_s", "super_ss", "val_b", "val_b", "date", "date_dt", "sparse_s", "sparse_ss", "multi_ss", "multi_ss"));

    // single valued docvalues for strings, and single valued numeric doc values for numeric fields
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_sd",  "cat_s","cat_sd", "where_s","where_sd", "num_d","num_dd", "num_i","num_id", "num_is","num_lds", "num_fs","num_dds", "super_s","super_sd", "val_b","val_b", "date","date_dtd", "sparse_s","sparse_sd"    ,"multi_ss","multi_sds") );

    // multi-valued docvalues
    FacetFieldProcessorByArrayDV.unwrap_singleValued_multiDv = false;  // better multi-valued coverage
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_sds",  "cat_s","cat_sds", "where_s","where_sds", "num_d","num_d", "num_i","num_i", "num_is","num_ids", "num_fs","num_fds",    "super_s","super_sds", "val_b","val_b", "date","date_dtds", "sparse_s","sparse_sds"    ,"multi_ss","multi_sds") );

    // multi-valued docvalues
    FacetFieldProcessorByArrayDV.unwrap_singleValued_multiDv = true;
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_sds",  "cat_s","cat_sds", "where_s","where_sds", "num_d","num_d", "num_i","num_i", "num_is","num_ids", "num_fs","num_fds",   "super_s","super_sds", "val_b","val_b", "date","date_dtds", "sparse_s","sparse_sds"    ,"multi_ss","multi_sds") );
  }

  public static void doStatsTemplated(Client client, ModifiableSolrParams p) throws Exception {
    p.set("Z_num_i", "Z_" + p.get("num_i") );
    if (p.get("num_is") == null) p.add("num_is","num_is");
    if (p.get("num_fs") == null) p.add("num_fs","num_fs");

    MacroExpander m = new MacroExpander( p.getMap() );

    String cat_s = m.expand("${cat_s}");
    String where_s = m.expand("${where_s}");
    String num_d = m.expand("${num_d}");
    String num_i = m.expand("${num_i}");
    String num_is = m.expand("${num_is}");
    String num_fs = m.expand("${num_fs}");
    String Z_num_i = m.expand("${Z_num_i}");
    String val_b = m.expand("${val_b}");
    String date = m.expand("${date}");
    String super_s = m.expand("${super_s}");
    String sparse_s = m.expand("${sparse_s}");
    String multi_ss = m.expand("${multi_ss}");

    client.deleteByQuery("*:*", null);

    SolrInputDocument doc =
               sdoc("id", "1", cat_s, "A", where_s, "NY", num_d, "4", num_i, "2",   num_is,"2",num_is,"-5", num_fs,"2",num_fs,"-5",   super_s, "zodiac", date, "2001-01-01T01:01:01Z", val_b, "true", sparse_s, "one");
    client.add(doc, null);
    client.add(doc, null);
    client.add(doc, null);  // a couple of deleted docs
    client.add(sdoc("id", "2", cat_s, "B", where_s, "NJ", num_d, "-9", num_i, "-5", num_is,"3",num_is,"-1", num_fs,"3",num_fs,"-1.5", super_s,"superman", date,"2002-02-02T02:02:02Z", val_b, "false"         , multi_ss,"a", multi_ss,"b" , Z_num_i, "0"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", cat_s, "A", where_s, "NJ", num_d, "2", num_i, "3",   num_is,"0",num_is,"3", num_fs,"0", num_fs,"3",   super_s,"spiderman", date,"2003-03-03T03:03:03Z"                         , multi_ss, "b", Z_num_i, ""+Integer.MIN_VALUE), null);
    client.add(sdoc("id", "5", cat_s, "B", where_s, "NJ", num_d, "11", num_i, "7",  num_is,"0",            num_fs,"0",               super_s,"batman"   , date,"2001-02-03T01:02:03Z"          ,sparse_s,"two", multi_ss, "a"), null);
    client.commit();
    client.add(sdoc("id", "6", cat_s, "B", where_s, "NY", num_d, "-5", num_i, "-5", num_is,"-1",           num_fs,"-1.5",            super_s,"hulk"     , date,"2002-03-01T03:02:01Z"                         , multi_ss, "b", multi_ss, "a", Z_num_i, ""+Integer.MAX_VALUE), null);
    client.commit();

    // test for presence of debugging info
    ModifiableSolrParams debugP = params(p);
    debugP.set("debugQuery","true");
    client.testJQ(params(debugP, "q", "*:*"
          , "json.facet", "{catA:{query:{q:'${cat_s}:A'}},  catA2:{query:{query:'${cat_s}:A'}},  catA3:{query:'${cat_s}:A'}    }"
      )
        , "facets=={ 'count':6, 'catA':{ 'count':2}, 'catA2':{ 'count':2}, 'catA3':{ 'count':2}}"
        , "debug/facet-trace=="  // just test for presence, not exact structure / values
    );


    // straight query facets
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{catA:{query:{q:'${cat_s}:A'}},  catA2:{query:{query:'${cat_s}:A'}},  catA3:{query:'${cat_s}:A'}    }"
        )
        , "facets=={ 'count':6, 'catA':{ 'count':2}, 'catA2':{ 'count':2}, 'catA3':{ 'count':2}}"
    );

    // nested query facets
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ catB:{type:query, q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} }}"
        )
        , "facets=={ 'count':6, 'catB':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}}"
    );

    // nested query facets on subset
    client.testJQ(params(p, "q", "id:(2 3)"
            , "json.facet", "{ catB:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} }}}"
        )
        , "facets=={ 'count':2, 'catB':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}}"
    );

    // nested query facets with stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ catB:{query:{q:'${cat_s}:B', facet:{nj:{query:{q:'${where_s}:NJ'}}, ny:{query:'${where_s}:NY'}} }}}"
        )
        , "facets=={ 'count':6, 'catB':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}}"
    );


    // field/terms facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{c1:{field:'${cat_s}'}, c2:{field:{field:'${cat_s}'}}, c3:{${terms} type:terms, field:'${cat_s}'}  }"
        )
        , "facets=={ 'count':6, " +
            "'c1':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c2':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c3':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}} "
    );

    // test mincount
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{  'buckets':[{ 'val':'B', 'count':3}]} } "
    );

    // test default mincount of 1
    client.testJQ(params(p, "q", "id:1"
            , "json.facet", "{f1:{terms:'${cat_s}'}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{  'buckets':[{ 'val':'A', 'count':1}]} } "
    );

    // test  mincount of 0 - need processEmpty for distrib to match up
    client.testJQ(params(p, "q", "id:1"
            , "json.facet", "{processEmpty:true, f1:{terms:{${terms} field:'${cat_s}', mincount:0}}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{  'buckets':[{ 'val':'A', 'count':1}, { 'val':'B', 'count':0}]} } "
    );

    // test  mincount of 0 with stats, need processEmpty for distrib to match up
    client.testJQ(params(p, "q", "id:1"
            , "json.facet", "{processEmpty:true, f1:{terms:{${terms} field:'${cat_s}', mincount:0, allBuckets:true, facet:{n1:'sum(${num_d})'}  }}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{ allBuckets:{ 'count':1, n1:4.0}, 'buckets':[{ 'val':'A', 'count':1, n1:4.0}, { 'val':'B', 'count':0 /*, n1:0.0 */ }]} } "
    );

    // test sorting by other stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'sum(${num_d})'}  }}" +
                " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'sum(${num_d})'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-3.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:6.0 }]} }"
    );

    // test sorting by other stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'min(${num_d})'}  }" +
                " , f2:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'max(${num_d})'}  } " +
                " , f3:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'unique(${where_s})'}  } " +
                " , f4:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'hll(${where_s})'}  } " +
                "}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, x:2.0 },  { val:'B', count:3, x:-9.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, x:11.0 }, { val:'A', count:2, x:4.0 }]} " +
            ", f3:{  'buckets':[{ val:'A', count:2, x:2 },    { val:'B', count:3, x:2 }]} " +
            ", f4:{  'buckets':[{ val:'A', count:2, x:2 },    { val:'B', count:3, x:2 }]} " +
            "}"
    );

    // test sorting by stat with function
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'avg(add(${num_d},${num_d}))'}  }}" +
                " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'avg(add(${num_d},${num_d}))'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-2.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-2.0}, { val:'A', count:2, n1:6.0 }]} }"
    );



    // percentiles 0,10,50,90,100
    // catA: 2.0 2.2 3.0 3.8 4.0
    // catB: -9.0 -8.2 -5.0 7.800000000000001 11.0
    // all: -9.0 -7.3999999999999995 2.0 8.200000000000001 11.0
    // test sorting by single percentile
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'percentile(${num_d},50)'}  }}" +
                " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'percentile(${num_d},50)'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:3.0 }, { val:'B', count:3, n1:-5.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-5.0}, { val:'A', count:2, n1:3.0 }]} }"
    );

    // test sorting by multiple percentiles (sort is by first)
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, sort:'n1 desc', facet:{n1:'percentile(${num_d},50,0,100)'}  }}" +
                " , f2:{terms:{${terms} field:${cat_s}, sort:'n1 asc', facet:{n1:'percentile(${num_d},50,0,100)'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:[3.0,2.0,4.0] }, { val:'B', count:3, n1:[-5.0,-9.0,11.0] }]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:[-5.0,-9.0,11.0]}, { val:'A', count:2, n1:[3.0,2.0,4.0] }]} }"
    );

    // test sorting by count/index order
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'count desc' }  }" +
                "           , f2:{terms:{${terms} field:'${cat_s}', sort:'count asc'  }  }" +
                "           , f3:{terms:{${terms} field:'${cat_s}', sort:'index asc'  }  }" +
                "           , f4:{terms:{${terms} field:'${cat_s}', sort:'index desc' }  }" +
                "}"
        )
        , "facets=={ count:6 " +
            " ,f1:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            " ,f2:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f3:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f4:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            "}"
    );

    // test sorting by default count/index order
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'count' }  }" +
                "           , f2:{terms:{${terms} field:'${cat_s}', sort:'count asc'  }  }" +
                "           , f3:{terms:{${terms} field:'${cat_s}', sort:'index'  }  }" +
                "           , f4:{terms:{${terms} field:'${cat_s}', sort:'index desc' }  }" +
                "}"
        )
        , "facets=={ count:6 " +
            " ,f1:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            " ,f2:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f3:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f4:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            "}"
    );


    // test tiebreaks when sorting by count
    client.testJQ(params(p, "q", "id:1 id:6"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'count desc' }  }" +
                "           , f2:{terms:{${terms} field:'${cat_s}', sort:'count asc'  }  }" +
                "}"
        )
        , "facets=={ count:2 " +
            " ,f1:{buckets:[ {val:A,count:1}, {val:B,count:1} ] }" +
            " ,f2:{buckets:[ {val:A,count:1}, {val:B,count:1} ] }" +
            "}"
    );

    // terms facet with nested query facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{${terms} field:'${cat_s}', facet:{nj:{query:'${where_s}:NJ'}}    }   }} }"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ 'buckets':[{ 'val':'B', 'count':3, 'nj':{ 'count':2}}, { 'val':'A', 'count':2, 'nj':{ 'count':1}}]} }"
    );

    // terms facet with nested query facet on subset
    client.testJQ(params(p, "q", "id:(2 5 4)"
            , "json.facet", "{cat:{terms:{${terms} field:'${cat_s}', facet:{nj:{query:'${where_s}:NJ'}}    }   }} }"
        )
        , "facets=={ 'count':3, " +
            "'cat':{ 'buckets':[{ 'val':'B', 'count':2, 'nj':{ 'count':2}}, { 'val':'A', 'count':1, 'nj':{ 'count':1}}]} }"
    );

    // test prefix
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:s, mincount:0 }}}"  // even with mincount=0, we should only see buckets with the prefix
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:spiderman, count:1}, {val:superman, count:1}]} } "
    );

    // test prefix that doesn't exist
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:ttt, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix that doesn't exist at start
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:aaaaaa, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix that doesn't exist at end
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:zzzzzz, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix on where field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f1:{${terms} type:terms, field:${where_s}, prefix:N  }" +
            ",f2:{${terms} type:terms, field:${where_s}, prefix:NY }" +
            ",f3:{${terms} type:terms, field:${where_s}, prefix:NJ }" +
            "}"
        )
        , "facets=={ 'count':6 " +
            ",f1:{ 'buckets':[ {val:NJ,count:3}, {val:NY,count:2} ]}" +
            ",f2:{ 'buckets':[ {val:NY,count:2} ]}" +
            ",f3:{ 'buckets':[ {val:NJ,count:3} ]}" +
            " } "
    );

    // test prefix on real multi-valued field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f1:{${terms} type:terms, field:${multi_ss}, prefix:A  }" +
            ",f2:{${terms} type:terms, field:${multi_ss}, prefix:z }" +
            ",f3:{${terms} type:terms, field:${multi_ss}, prefix:aa }" +
            ",f4:{${terms} type:terms, field:${multi_ss}, prefix:bb }" +
            ",f5:{${terms} type:terms, field:${multi_ss}, prefix:a }" +
            ",f6:{${terms} type:terms, field:${multi_ss}, prefix:b }" +
            "}"
        )
        , "facets=={ 'count':6 " +
            ",f1:{buckets:[]}" +
            ",f2:{buckets:[]}" +
            ",f3:{buckets:[]}" +
            ",f4:{buckets:[]}" +
            ",f5:{buckets:[ {val:a,count:3} ]}" +
            ",f6:{buckets:[ {val:b,count:3} ]}" +
            " } "
    );

    //
    // missing
    //

    // test missing w/ non-existent field
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${noexist}, missing:true}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[], missing:{count:6} } } "
    );

    // test missing
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1}, {val:two, count:1}], missing:{count:4} } } "
    );

    // test missing with stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true, facet:{x:'sum(${num_d})'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1, x:4.0}, {val:two, count:1, x:11.0}], missing:{count:4, x:-12.0}   } } "
    );

    // test that the missing bucket is not affected by any prefix
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true, prefix:on, facet:{x:'sum(${num_d})'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1, x:4.0}], missing:{count:4, x:-12.0}   } } "
    );

    // test missing with prefix that doesn't exist
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true, prefix:ppp, facet:{x:'sum(${num_d})'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[], missing:{count:4, x:-12.0}   } } "
    );

    // test numBuckets
    client.testJQ(params(p, "q", "*:*", "rows", "0", "facet", "true"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, numBuckets:true, limit:1}}}" // TODO: limit:0 produced an error
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:2, buckets:[{val:B, count:3}]} } "
    );

    // prefix should lower numBuckets
    client.testJQ(params(p, "q", "*:*", "rows", "0", "facet", "true"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, numBuckets:true, prefix:B}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:1, buckets:[{val:B, count:3}]} } "
    );

    // mincount should lower numBuckets
    client.testJQ(params(p, "q", "*:*", "rows", "0", "facet", "true"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, numBuckets:true, mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:1, buckets:[{val:B, count:3}]} } "
    );

    // basic range facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${num_d}, start:-5, end:10, gap:5}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );

    // basic range facet on dates
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${date}, start:'2001-01-01T00:00:00Z', end:'2003-01-01T00:00:00Z', gap:'+1YEAR'}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:'2001-01-01T00:00:00Z',count:2}, {val:'2002-01-01T00:00:00Z',count:2}] } }"
    );

    // range facet on dates w/ stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${date}, start:'2002-01-01T00:00:00Z', end:'2005-01-01T00:00:00Z', gap:'+1YEAR',   other:all, facet:{ x:'avg(${num_d})' } } }"
        )
        , "facets=={count:6, f:{buckets:[ {val:'2002-01-01T00:00:00Z',count:2,x:-7.0}, {val:'2003-01-01T00:00:00Z',count:1,x:2.0}, {val:'2004-01-01T00:00:00Z',count:0}], before:{count:2,x:7.5}, after:{count:0}, between:{count:3,x:-4.0}  } }"
    );

    // basic range facet with "include" params
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, include:upper}}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:0}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );

    // range facet with sub facets and stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */ } ] } }"
    );

    // range facet with sub facets and stats, with "other:all"
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */} ]" +
            ",before: {count:1,x:-5.0,ny:{count:0}}" +
            ",after:  {count:1,x:7.0, ny:{count:0}}" +
            ",between:{count:3,x:0.0, ny:{count:2}}" +
            " } }"
    );

    // range facet with mincount
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all, mincount:2,    facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}"
        )
        , "facets=={count:6, f:{buckets:[  {val:0.0,count:2,x:5.0,ny:{count:1}} ]" +
            ",before: {count:1,x:-5.0,ny:{count:0}}" +
            ",after:  {count:1,x:7.0, ny:{count:0}}" +
            ",between:{count:3,x:0.0, ny:{count:2}}" +
            " } }"
    );

    // range facet with sub facets and stats, with "other:all", on subset
    client.testJQ(params(p, "q", "id:(3 4 6)"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:3, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:1,x:3.0,ny:{count:0}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */} ]" +
            ",before: {count:0 /* ,x:0.0,ny:{count:0} */ }" +
            ",after:  {count:0 /* ,x:0.0,ny:{count:0} */}" +
            ",between:{count:2,x:-2.0, ny:{count:1}}" +
            " } }"
    );



    // stats at top level
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', avg2:'avg(def(${num_d},0))', min1:'min(${num_d})', max1:'max(${num_d})'" +
                ", numwhere:'unique(${where_s})', unique_num_i:'unique(${num_i})', unique_num_d:'unique(${num_d})', unique_date:'unique(${date})'" +
                ", where_hll:'hll(${where_s})', hll_num_i:'hll(${num_i})', hll_num_d:'hll(${num_d})', hll_date:'hll(${date})'" +
                ", med:'percentile(${num_d},50)', perc:'percentile(${num_d},0,50.0,100)' }"
        )
        , "facets=={ 'count':6, " +
            "sum1:3.0, sumsq1:247.0, avg1:0.6, avg2:0.5, min1:-9.0, max1:11.0" +
            ", numwhere:2, unique_num_i:4, unique_num_d:5, unique_date:5" +
            ", where_hll:2, hll_num_i:4, hll_num_d:5, hll_date:5" +
            ", med:2.0, perc:[-9.0,2.0,11.0]  }"
    );

    // stats at top level, no matches
    client.testJQ(params(p, "q", "id:DOESNOTEXIST"
            , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', min1:'min(${num_d})', max1:'max(${num_d})'" +
                ", numwhere:'unique(${where_s})', unique_num_i:'unique(${num_i})', unique_num_d:'unique(${num_d})', unique_date:'unique(${date})'" +
                ", where_hll:'hll(${where_s})', hll_num_i:'hll(${num_i})', hll_num_d:'hll(${num_d})', hll_date:'hll(${date})'" +
                ", med:'percentile(${num_d},50)', perc:'percentile(${num_d},0,50.0,100)' }"
        )
        , "facets=={count:0 " +
            "/* ,sum1:0.0, sumsq1:0.0, avg1:0.0, min1:'NaN', max1:'NaN', numwhere:0 */" +
            " }"
    );

    //
    // tests on a multi-valued field with actual multiple values, just to ensure that we are
    // using a multi-valued method for the rest of the tests when appropriate.
    //

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{${terms} field:'${multi_ss}', facet:{nj:{query:'${where_s}:NJ'}}    }   }} }"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ 'buckets':[{ 'val':'a', 'count':3, 'nj':{ 'count':2}}, { 'val':'b', 'count':3, 'nj':{ 'count':2}}]} }"
    );

    // test unique on multi-valued field
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                "x:'unique(${multi_ss})'" +
                ",y:{query:{q:'id:2', facet:{x:'unique(${multi_ss})'} }}  " +
                ",x2:'hll(${multi_ss})'" +
                ",y2:{query:{q:'id:2', facet:{x:'hll(${multi_ss})'} }}  " +

                " }"
        )
        , "facets=={count:6 " +
            ",x:2" +
            ",y:{count:1, x:2}" +  // single document should yield 2 unique values
            ",x2:2" +
            ",y2:{count:1, x:2}" +  // single document should yield 2 unique values
            " }"
    );

    // test allBucket multi-valued
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{x:{terms:{${terms} field:'${multi_ss}',allBuckets:true}}}"
        )
        , "facets=={ count:6, " +
            "x:{ buckets:[{val:a, count:3}, {val:b, count:3}] , allBuckets:{count:6} } }"
    );

    // allBuckets for multi-valued field with stats.  This can sometimes take a different path of adding complete DocSets to the Acc
    // also test limit:0
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f0:{${terms} type:terms, field:${multi_ss}, allBuckets:true, limit:0} " +
                ",f1:{${terms} type:terms, field:${multi_ss}, allBuckets:true, limit:0, offset:1} " +  // offset with 0 limit
                ",f2:{${terms} type:terms, field:${multi_ss}, allBuckets:true, limit:0, facet:{x:'sum(${num_d})'}, sort:'x desc' } " +
                ",f3:{${terms} type:terms, field:${multi_ss}, allBuckets:true, limit:0, missing:true, facet:{x:'sum(${num_d})', y:'avg(${num_d})'}, sort:'x desc' } " +
                "}"
        )
        , "facets=={ 'count':6, " +
            " f0:{allBuckets:{count:6}, buckets:[]}" +
            ",f1:{allBuckets:{count:6}, buckets:[]}" +
            ",f2:{allBuckets:{count:6, x:-15.0}, buckets:[]} " +
            ",f3:{allBuckets:{count:6, x:-15.0, y:-2.5}, buckets:[], missing:{count:2, x:4.0, y:4.0} }} " +
            "}"
    );

    // allBuckets with numeric field with stats.
    // also test limit:0
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f0:{${terms} type:terms, field:${num_i}, allBuckets:true, limit:0} " +
                ",f1:{${terms} type:terms, field:${num_i}, allBuckets:true, limit:0, offset:1} " +  // offset with 0 limit
                ",f2:{${terms} type:terms, field:${num_i}, allBuckets:true, limit:0, facet:{x:'sum(${num_d})'}, sort:'x desc' } " +
                "}"
        )
        , "facets=={ 'count':6, " +
            " f0:{allBuckets:{count:5}, buckets:[]}" +
            ",f1:{allBuckets:{count:5}, buckets:[]}" +
            ",f2:{allBuckets:{count:5, x:3.0}, buckets:[]} " +
            "}"
    );


    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // test converting legacy facets

    // test mincount
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:3}}}"
            , "facet","true", "facet.version", "2", "facet.field","{!key=f1}${cat_s}", "facet.mincount","3"
        )
        , "facets=={ 'count':6, " +
            "'f1':{  'buckets':[{ 'val':'B', 'count':3}]} } "
    );

    // test prefix
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f1:{terms:{field:${super_s}, prefix:s, mincount:0 }}}"  // even with mincount=0, we should only see buckets with the prefix
            , "facet","true", "facet.version", "2", "facet.field","{!key=f1}${super_s}", "facet.prefix","s", "facet.mincount","0"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:spiderman, count:1}, {val:superman, count:1}]} } "
    );

    // range facet with sub facets and stats
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
            , "facet","true", "facet.version", "2", "facet.range","{!key=f}${num_d}", "facet.range.start","-5", "facet.range.end","10", "facet.range.gap","5"
            , "f.f.facet.stat","x:sum(${num_i})", "subfacet.f.query","{!key=ny}${where_s}:NY"

        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */ } ] } }"
    );

    // test sorting by stat
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f1:{terms:{field:'${cat_s}', sort:'n1 desc', facet:{n1:'sum(${num_d})'}  }}" +
            //    " , f2:{terms:{field:'${cat_s}', sort:'n1 asc', facet:{n1:'sum(${num_d})'}  }} }"
            , "facet","true", "facet.version", "2", "facet.field","{!key=f1}${cat_s}", "f.f1.facet.sort","n1 desc", "facet.stat","n1:sum(${num_d})"
            , "facet.field","{!key=f2}${cat_s}", "f.f1.facet.sort","n1 asc"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-3.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:6.0 }]} }"
    );

    // range facet with sub facets and stats, with "other:all", on subset
    client.testJQ(params(p, "q", "id:(3 4 6)"
            //, "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
            , "facet","true", "facet.version", "2", "facet.range","{!key=f}${num_d}", "facet.range.start","-5", "facet.range.end","10", "facet.range.gap","5"
            , "f.f.facet.stat","x:sum(${num_i})", "subfacet.f.query","{!key=ny}${where_s}:NY", "facet.range.other","all"
        )
        , "facets=={count:3, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:1,x:3.0,ny:{count:0}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */} ]" +
            ",before: {count:0 /* ,x:0.0,ny:{count:0} */ }" +
            ",after:  {count:0 /* ,x:0.0,ny:{count:0} */}" +
            ",between:{count:2,x:-2.0, ny:{count:1}}" +
            " } }"
    );


    ////////////////////////////////////////////////////////////////////////////////////////////
    // multi-select / exclude tagged filters via excludeTags
    ////////////////////////////////////////////////////////////////////////////////////////////

    // test uncached multi-select (see SOLR-8496)
    client.testJQ(params(p, "q", "{!cache=false}*:*", "fq","{!tag=doc3,allfilt}-id:3"

            , "json.facet", "{" +
                "f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} }  " +
                "}"
        )
        , "facets=={ count:5, " +
            " f1:{ buckets:[ {val:B, count:3}, {val:A, count:2} ]  }" +
            "}"
    );

    // test sub-facets of  empty buckets with domain filter exclusions (canProduceFromEmpty) (see SOLR-9519)
    client.testJQ(params(p, "q", "*:*", "fq","{!tag=doc3}id:non-exist", "fq","{!tag=CATA}${cat_s}:A"

        , "json.facet", "{" +
            "f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} }  " +
            ",q1 :{type:query, q:'*:*', facet:{ f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} } }  }  " +  // nested under query
            ",q1a:{type:query, q:'id:4', facet:{ f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} } }  }  " +  // nested under query, make sure id:4 filter still applies
            ",r1 :{type:range, field:${num_d}, start:0, gap:3, end:5,  facet:{ f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} } }  }  " +  // nested under range, make sure range constraints still apply
            ",f2:{${terms} type:terms, field:${cat_s}, domain:{filter:'*:*'} }  " + // domain filter doesn't widen, so f2 should not appear.
            "}"
    )
    , "facets=={ count:0, " +
        " f1:{ buckets:[ {val:A, count:2} ]  }" +
        ",q1:{ count:0, f1:{buckets:[{val:A, count:2}]} }" +
        ",q1a:{ count:0, f1:{buckets:[{val:A, count:1}]} }" +
        ",r1:{ buckets:[ {val:0.0,count:0,f1:{buckets:[{val:A, count:1}]}}, {val:3.0,count:0,f1:{buckets:[{val:A, count:1}]}} ]  }" +
        "}"
    );

    // nested query facets on subset (with excludeTags)
    client.testJQ(params(p, "q", "*:*", "fq","{!tag=abc}id:(2 3)"
            , "json.facet", "{ processEmpty:true," +
                " f1:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:[xyz,qaz]}}" +
                ",f2:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:abc }}" +
                ",f3:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:'xyz,abc,qaz' }}" +
                ",f4:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:[xyz , abc , qaz] }}" +
                ",f5:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:[xyz,qaz]}}" +    // this is repeated, but it did fail when a single context was shared among sub-facets
                ",f6:{query:{q:'${cat_s}:B', facet:{processEmpty:true, nj:{query:'${where_s}:NJ'}, ny:{ type:query, q:'${where_s}:NY', excludeTags:abc}}  }}" +  // exclude in a sub-facet
                ",f7:{query:{q:'${cat_s}:B', facet:{processEmpty:true, nj:{query:'${where_s}:NJ'}, ny:{ type:query, q:'${where_s}:NY', excludeTags:xyz}}  }}" +  // exclude in a sub-facet that doesn't match
                "}"
        )
        , "facets=={ 'count':2, " +
            " 'f1':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}" +
            ",'f2':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}" +
            ",'f3':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}" +
            ",'f4':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}" +
            ",'f5':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}" +
            ",'f6':{'count':1, 'nj':{'count':1}, 'ny':{'count':1}}" +
            ",'f7':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}" +
            "}"
    );

    // terms facet with nested query facet (with excludeTags, using new format inside domain:{})
    client.testJQ(params(p, "q", "{!cache=false}*:*", "fq", "{!tag=doc6,allfilt}-id:6", "fq","{!tag=doc3,allfilt}-id:3"

            , "json.facet", "{processEmpty:true, " +
                " f0:{${terms} type:terms, field:${cat_s},                                    facet:{nj:{query:'${where_s}:NJ'}} }  " +
                ",f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3},   missing:true,  facet:{nj:{query:'${where_s}:NJ'}} }  " +
                ",f2:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:allfilt},missing:true,  facet:{nj:{query:'${where_s}:NJ'}} }  " +
                ",f3:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc6},   missing:true,  facet:{nj:{query:'${where_s}:NJ'}} }  " +
                "}"
        )
        , "facets=={ count:4, " +
            " f0:{ buckets:[ {val:A, count:2, nj:{ count:1}}, {val:B, count:2, nj:{count:2}} ] }" +
            ",f1:{ buckets:[ {val:A, count:2, nj:{ count:1}}, {val:B, count:2, nj:{count:2}} ] , missing:{count:1,nj:{count:0}} }" +
            ",f2:{ buckets:[ {val:B, count:3, nj:{ count:2}}, {val:A, count:2, nj:{count:1}} ] , missing:{count:1,nj:{count:0}} }" +
            ",f3:{ buckets:[ {val:B, count:3, nj:{ count:2}}, {val:A, count:2, nj:{count:1}} ] , missing:{count:0} }" +
            "}"
    );

    // range facet with sub facets and stats, with "other:all" (with excludeTags)
    client.testJQ(params(p, "q", "*:*", "fq", "{!tag=doc6,allfilt}-id:6", "fq","{!tag=doc3,allfilt}-id:3"
            , "json.facet", "{processEmpty:true " +
                ", f1:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}} , domain:{excludeTags:allfilt} }" +
                ", f2:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}  }" +
                "}"
        )
        , "facets=={count:4" +
            ",f1:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0} ]" +
              ",before: {count:1,x:-5.0,ny:{count:0}}" +
              ",after:  {count:1,x:7.0, ny:{count:0}}" +
              ",between:{count:3,x:0.0, ny:{count:2}} }" +
            ",f2:{buckets:[ {val:-5.0,count:0}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0} ]" +
              ",before: {count:1,x:-5.0,ny:{count:0}}" +
              ",after:  {count:1,x:7.0, ny:{count:0}}" +
              ",between:{count:2,x:5.0, ny:{count:1}} }" +
            "}"
    );


    //
    // facet on numbers
    //
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f1:{${terms}  type:field, field:${num_i} }" +
                ",f2:{${terms}  type:field, field:${num_i}, sort:'count asc' }" +
                ",f3:{${terms}  type:field, field:${num_i}, sort:'index asc' }" +
                ",f4:{${terms}  type:field, field:${num_i}, sort:'index desc' }" +
                ",f5:{${terms}  type:field, field:${num_i}, sort:'index desc', limit:1, missing:true, allBuckets:true, numBuckets:true }" +
                ",f6:{${terms}  type:field, field:${num_i}, sort:'index desc', mincount:2, numBuckets:true }" +   // mincount should lower numbuckets
                ",f7:{${terms}  type:field, field:${num_i}, sort:'index desc', offset:2, numBuckets:true }" +     // test offset
                ",f8:{${terms}  type:field, field:${num_i}, sort:'index desc', offset:100, numBuckets:true }" +   // test high offset
                ",f9:{${terms}  type:field, field:${num_i}, sort:'x desc', facet:{x:'avg(${num_d})'}, missing:true, allBuckets:true, numBuckets:true }" +            // test stats
                ",f10:{${terms}  type:field, field:${num_i}, facet:{a:{query:'${cat_s}:A'}}, missing:true, allBuckets:true, numBuckets:true }" +     // test subfacets
                ",f11:{${terms}  type:field, field:${num_i}, facet:{a:'unique(${num_d})'} ,missing:true, allBuckets:true, sort:'a desc' }" +     // test subfacet using unique on numeric field (this previously triggered a resizing bug)
                "}"
        )
        , "facets=={count:6 " +
            ",f1:{ buckets:[{val:-5,count:2},{val:2,count:1},{val:3,count:1},{val:7,count:1} ] } " +
            ",f2:{ buckets:[{val:2,count:1},{val:3,count:1},{val:7,count:1},{val:-5,count:2} ] } " +
            ",f3:{ buckets:[{val:-5,count:2},{val:2,count:1},{val:3,count:1},{val:7,count:1} ] } " +
            ",f4:{ buckets:[{val:7,count:1},{val:3,count:1},{val:2,count:1},{val:-5,count:2} ] } " +
            ",f5:{ buckets:[{val:7,count:1}]   , numBuckets:4, allBuckets:{count:5}, missing:{count:1}  } " +
            ",f6:{ buckets:[{val:-5,count:2}]  , numBuckets:1  } " +
            ",f7:{ buckets:[{val:2,count:1},{val:-5,count:2}] , numBuckets:4 } " +
            ",f8:{ buckets:[] , numBuckets:4 } " +
            ",f9:{ buckets:[{val:7,count:1,x:11.0},{val:2,count:1,x:4.0},{val:3,count:1,x:2.0},{val:-5,count:2,x:-7.0} ],  numBuckets:4, allBuckets:{count:5,x:0.6},missing:{count:1,x:0.0} } " +  // TODO: should missing exclude "x" because no values were collected?
            ",f10:{ buckets:[{val:-5,count:2,a:{count:0}},{val:2,count:1,a:{count:1}},{val:3,count:1,a:{count:1}},{val:7,count:1,a:{count:0}} ],  numBuckets:4, allBuckets:{count:5},missing:{count:1,a:{count:0}} } " +
            ",f11:{ buckets:[{val:-5,count:2,a:2},{val:2,count:1,a:1},{val:3,count:1,a:1},{val:7,count:1,a:1} ] , missing:{count:1,a:0} , allBuckets:{count:5,a:5}  } " +
            "}"
    );


    // facet on a float field - shares same code with integers/longs currently, so we only need to test labels/sorting
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f1:{${terms}  type:field, field:${num_d} }" +
                ",f2:{${terms}  type:field, field:${num_d}, sort:'index desc' }" +
                "}"
        )
        , "facets=={count:6 " +
            ",f1:{ buckets:[{val:-9.0,count:1},{val:-5.0,count:1},{val:2.0,count:1},{val:4.0,count:1},{val:11.0,count:1} ] } " +
            ",f2:{ buckets:[{val:11.0,count:1},{val:4.0,count:1},{val:2.0,count:1},{val:-5.0,count:1},{val:-9.0,count:1} ] } " +
            "}"
    );

    // test 0, min/max int
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
                " u : 'unique(${Z_num_i})'" +
                ", f1:{${terms}  type:field, field:${Z_num_i} }" +
        "}"
        )
        , "facets=={count:6 " +
            ",u:3" +
            ",f1:{ buckets:[{val:" + Integer.MIN_VALUE + ",count:1},{val:0,count:1},{val:" + Integer.MAX_VALUE+",count:1}]} " +
            "}"
    );



    // multi-valued integer
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            " c1:'unique(${num_is})', c2:'hll(${num_is})'" +
            ",f1:{${terms} type:terms, field:${num_is} }  " +
            "}"
        )
        , "facets=={ count:6 " +
            ", c1:5, c2:5" +
            ", f1:{ buckets:[ {val:-1,count:2},{val:0,count:2},{val:3,count:2},{val:-5,count:1},{val:2,count:1}  ] } " +
            "} "
    );

    // multi-valued float
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            " c1:'unique(${num_fs})', c2:'hll(${num_fs})'" +
            ",f1:{${terms} type:terms, field:${num_fs} }  " +
            "}"
        )
        , "facets=={ count:6 " +
            ", c1:5, c2:5" +
            ", f1:{ buckets:[ {val:-1.5,count:2},{val:0.0,count:2},{val:3.0,count:2},{val:-5.0,count:1},{val:2.0,count:1}  ] } " +
            "} "
    );

    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            // "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0}" +  // overrequest=0 test needs predictable layout
            "cat1:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:1}" +
            ",catDef:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:-1}" +  // -1 is default overrequest
            ",catBig:{type:terms, field:${cat_s}, sort:'count desc', offset:1, limit:2147483647, overrequest:2147483647}" +  // make sure overflows don't mess us up
            "}"
        )
        , "facets=={ count:6" +
            // ", cat0:{ buckets:[ {val:B,count:3} ] }"
            ", cat1:{ buckets:[ {val:B,count:3} ] }" +
            ", catDef:{ buckets:[ {val:B,count:3} ] }" +
            ", catBig:{ buckets:[ {val:A,count:2} ] }" +
            "}"
    );


    // test filter
    client.testJQ(params(p, "q", "*:*", "myfilt","${cat_s}:A", "ff","-id:1", "ff","-id:2"
        , "json.facet", "{" +
            "t:{${terms} type:terms, field:${cat_s}, domain:{filter:[]} }" + // empty filter list
            ",t_filt:{${terms} type:terms, field:${cat_s}, domain:{filter:'${cat_s}:B'} }" +
            ",t_filt2 :{${terms} type:terms, field:${cat_s}, domain:{filter:'{!query v=$myfilt}'} }" +  // test access to qparser and other query parameters
            ",t_filt2a:{${terms} type:terms, field:${cat_s}, domain:{filter:{param:myfilt} } }" +  // test filter via "param" type
            ",t_filt3: {${terms} type:terms, field:${cat_s}, domain:{filter:['-id:1','-id:2']} }" +
            ",t_filt3a:{${terms} type:terms, field:${cat_s}, domain:{filter:{param:ff}} }" +  // test multi-valued query parameter
            ",q:{type:query, q:'${cat_s}:B', domain:{filter:['-id:5']} }" + // also tests a top-level negative filter
            ",r:{type:range, field:${num_d}, start:-5, end:10, gap:5, domain:{filter:'-id:4'} }" +
            "}"
        )
        , "facets=={ count:6, " +
            "t        :{ buckets:[ {val:B, count:3}, {val:A, count:2} ] }" +
            ",t_filt  :{ buckets:[ {val:B, count:3}] } " +
            ",t_filt2 :{ buckets:[ {val:A, count:2}] } " +
            ",t_filt2a:{ buckets:[ {val:A, count:2}] } " +
            ",t_filt3 :{ buckets:[ {val:B, count:2}, {val:A, count:1}] } " +
            ",t_filt3a:{ buckets:[ {val:B, count:2}, {val:A, count:1}] } " +
            ",q:{count:2}" +
            ",r:{buckets:[ {val:-5.0,count:1}, {val:0.0,count:1}, {val:5.0,count:0} ] }" +
            "}"
    );

  }

  @Test
  public void testOverrequest() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards(), "debugQuery", Boolean.toString(random().nextBoolean()) );

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3);

    client.deleteByQuery("*:*", null);

    ModifiableSolrParams p = params("cat_s", "cat_s");
    String cat_s = p.get("cat_s");

    clients.get(0).add( sdoc("id", "1", cat_s, "A") ); // A will win tiebreak
    clients.get(0).add( sdoc("id", "2", cat_s, "B") );

    clients.get(1).add( sdoc("id", "3", cat_s, "B") );
    clients.get(1).add( sdoc("id", "4", cat_s, "A") ); // A will win tiebreak

    clients.get(2).add( sdoc("id", "5", cat_s, "B") );
    clients.get(2).add( sdoc("id", "6", cat_s, "B") );

    client.commit();

    // Shard responses should be A=1, A=1, B=2, merged should be "A=2, B=2" hence A wins tiebreak

    client.testJQ(params(p, "q", "*:*",
        "json.facet", "{" +
            "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0}" +
            ",cat1:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:1}" +
            ",catDef:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:-1}" +  // -1 is default overrequest
            ",catBig:{type:terms, field:${cat_s}, sort:'count desc', offset:1, limit:2147483647, overrequest:2147483647}" +  // make sure overflows don't mess us up
            "}"
        )
        , "facets=={ count:6" +
            ", cat0:{ buckets:[ {val:A,count:2} ] }" +  // with no overrequest, we incorrectly conclude that A is the top bucket
            ", cat1:{ buckets:[ {val:B,count:4} ] }" +
            ", catDef:{ buckets:[ {val:B,count:4} ] }" +
            ", catBig:{ buckets:[ {val:A,count:2} ] }" +
            "}"
    );
  }


  @Test
  public void testBigger() throws Exception {
    ModifiableSolrParams p = params("rows", "0", "cat_s", "cat_ss", "where_s", "where_ss");
    //    doBigger(Client.localClient, p);

    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards() );
    doBigger( client, p );
  }

  public void doBigger(Client client, ModifiableSolrParams p) throws Exception {
    MacroExpander m = new MacroExpander(p.getMap());

    String cat_s = m.expand("${cat_s}");
    String where_s = m.expand("${where_s}");

    client.deleteByQuery("*:*", null);

    Random r = new Random(0);  // make deterministic
    int numCat = 1;
    int numWhere = 2000000000;
    int commitPercent = 10;
    int ndocs=1000;

    Map<Integer, Map<Integer, List<Integer>>> model = new HashMap();  // cat->where->list<ids>
    for (int i=0; i<ndocs; i++) {
      Integer cat = r.nextInt(numCat);
      Integer where = r.nextInt(numWhere);
      client.add( sdoc("id", i, cat_s,cat, where_s, where) , null );
      Map<Integer,List<Integer>> sub = model.get(cat);
      if (sub == null) {
        sub = new HashMap<>();
        model.put(cat, sub);
      }
      List<Integer> ids = sub.get(where);
      if (ids == null) {
        ids = new ArrayList<>();
        sub.put(where, ids);
      }
      ids.add(i);

      if (r.nextInt(100) < commitPercent) {
        client.commit();
      }
    }

    client.commit();

    int sz = model.get(0).size();

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{type:terms, field:${cat_s}, limit:2, facet:{x:'unique($where_s)'}  }}"
        )
        , "facets=={ 'count':" + ndocs + "," +
            "'f1':{  'buckets':[{ 'val':'0', 'count':" + ndocs + ", x:" + sz + " }]} } "
    );

    if (client.local()) {
      // distrib estimation prob won't match
      client.testJQ(params(p, "q", "*:*"
              , "json.facet", "{f1:{type:terms, field:${cat_s}, limit:2, facet:{x:'hll($where_s)'}  }}"
          )
          , "facets=={ 'count':" + ndocs + "," +
              "'f1':{  'buckets':[{ 'val':'0', 'count':" + ndocs + ", x:" + sz + " }]} } "
      );
    }

  }

  public void testTolerant() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards() + ",[ff01::114]:33332:/ignore_exception");
    indexSimple(client);

    try {
      client.testJQ(params("ignore_exception", "true", "shards.tolerant", "false", "q", "*:*"
              , "json.facet", "{f:{type:terms, field:cat_s}}"
          )
          , "facets=={ count:6," +
              "f:{ buckets:[{val:B,count:3},{val:A,count:2}] }" +
              "}"
      );
      fail("we should have failed");
    } catch (Exception e) {
      // ok
    }

    client.testJQ(params("ignore_exception", "true", "shards.tolerant", "true", "q", "*:*"
            , "json.facet", "{f:{type:terms, field:cat_s}}"
        )
        , "facets=={ count:6," +
            "f:{ buckets:[{val:B,count:3},{val:A,count:2}] }" +
            "}"
    );
  }



  @Test
  public void testBlockJoin() throws Exception {
    doBlockJoin(Client.localClient());
  }

  public void doBlockJoin(Client client) throws Exception {
    ModifiableSolrParams p = params("rows","0");

    client.deleteByQuery("*:*", null);

    SolrInputDocument parent;
    parent = sdoc("id", "1", "type_s","book", "book_s","A", "v_t","q");
    client.add(parent, null);

    parent = sdoc("id", "2", "type_s","book", "book_s","B", "v_t","q w");
    parent.addChildDocument( sdoc("id","2.1", "type_s","page", "page_s","a", "v_t","x y z")  );
    parent.addChildDocument( sdoc("id","2.2", "type_s","page", "page_s","b", "v_t","x y  ") );
    parent.addChildDocument( sdoc("id","2.3", "type_s","page", "page_s","c", "v_t","  y z" )  );
    client.add(parent, null);

    parent = sdoc("id", "3", "type_s","book", "book_s","C", "v_t","q w e");
    parent.addChildDocument( sdoc("id","3.1", "type_s","page", "page_s","d", "v_t","x    ")  );
    parent.addChildDocument( sdoc("id","3.2", "type_s","page", "page_s","e", "v_t","  y  ")  );
    parent.addChildDocument( sdoc("id","3.3", "type_s","page", "page_s","f", "v_t","    z")  );
    client.add(parent, null);

    parent = sdoc("id", "4", "type_s","book", "book_s","D", "v_t","e");
    client.add(parent, null);

    client.commit();

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ " +
                "pages:{ type:query, domain:{blockChildren:'type_s:book'} , facet:{ x:{field:v_t} } }" +
                ",pages2:{type:terms, field:v_t, domain:{blockChildren:'type_s:book'} }" +
                ",books:{ type:query, domain:{blockParent:'type_s:book'}  , facet:{ x:{field:v_t} } }" +
                ",books2:{type:terms, field:v_t, domain:{blockParent:'type_s:book'} }" +
                ",pageof3:{ type:query, q:'id:3', facet : { x : { type:terms, field:page_s, domain:{blockChildren:'type_s:book'}}} }" +
                ",bookof22:{ type:query, q:'id:2.2', facet : { x : { type:terms, field:book_s, domain:{blockParent:'type_s:book'}}} }" +
                ",missing_blockParent:{ type:query, domain:{blockParent:'type_s:does_not_exist'} }" +
                ",missing_blockChildren:{ type:query, domain:{blockChildren:'type_s:does_not_exist'} }" +
                "}"
        )
        , "facets=={ count:10" +
            ", pages:{count:6 , x:{buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ]}  }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", books:{count:4 , x:{buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ]}  }" +
            ", books2:{ buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ] }" +
            ", pageof3:{count:1 , x:{buckets:[ {val:d,count:1},{val:e,count:1},{val:f,count:1} ]}  }" +
            ", bookof22:{count:1 , x:{buckets:[ {val:B,count:1} ]}  }" +
            ", missing_blockParent:{count:0}" +
            ", missing_blockChildren:{count:0}" +
            "}"
    );

    // no matches in base query
    client.testJQ(params("q", "no_match_s:NO_MATCHES"
            , "json.facet", "{ processEmpty:true," +
                "pages:{ type:query, domain:{blockChildren:'type_s:book'} }" +
                ",books:{ type:query, domain:{blockParent:'type_s:book'} }" +
                "}"
        )
        , "facets=={ count:0" +
            ", pages:{count:0}" +
            ", books:{count:0}" +
            "}"
    );


    // test facet on children nested under terms facet on parents
    client.testJQ(params("q", "*:*"
            , "json.facet", "{" +
                "books:{ type:terms, field:book_s, facet:{ pages:{type:terms, field:v_t, domain:{blockChildren:'type_s:book'}} } }" +
                "}"
        )
        , "facets=={ count:10" +
            ", books:{buckets:[{val:A,count:1,pages:{buckets:[]}}" +
            "                 ,{val:B,count:1,pages:{buckets:[{val:y,count:3},{val:x,count:2},{val:z,count:2}]}}" +
            "                 ,{val:C,count:1,pages:{buckets:[{val:x,count:1},{val:y,count:1},{val:z,count:1}]}}" +
            "                 ,{val:D,count:1,pages:{buckets:[]}}"+
            "] }" +
            "}"
    );

    // test filter after block join
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            "pages1:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:'*:*'} }" +
            ",pages2:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:'-id:3.1'} }" +
            ",books:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:'*:*'} }" +
            ",books2:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:'id:1'} }" +
            "}"
        )
        , "facets=={ count:10" +
            ", pages1:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:z,count:3},{val:x,count:2} ] }" +
            ", books:{ buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ] }" +
            ", books2:{ buckets:[ {val:q,count:1} ] }" +
            "}"
    );


    // test other various ways to get filters
    client.testJQ(params(p, "q", "*:*", "f1","-id:3.1", "f2","id:1"
        , "json.facet", "{ " +
            "pages1:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:[]} }" +
            ",pages2:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:{param:f1} } }" +
            ",books:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:[{param:q},{param:missing_param}]} }" +
            ",books2:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:[{param:f2}] } }" +
            "}"
        )
        , "facets=={ count:10" +
            ", pages1:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:z,count:3},{val:x,count:2} ] }" +
            ", books:{ buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ] }" +
            ", books2:{ buckets:[ {val:q,count:1} ] }" +
            "}"
    );

  }


  @Test
  public void testErrors() throws Exception {
    doTestErrors(Client.localClient());
  }

  public void doTestErrors(Client client) throws Exception {
    ModifiableSolrParams p = params("rows", "0");
    client.deleteByQuery("*:*", null);

    try {
      client.testJQ(params("ignore_exception", "true", "q", "*:*"
          , "json.facet", "{f:{type:ignore_exception_aaa, field:bbbbbb}}"
          )
      );
    } catch (SolrException e) {
      assertTrue( e.getMessage().contains("ignore_exception_aaa") );
    }

  }



  public void XtestPercentiles() {
    AVLTreeDigest catA = new AVLTreeDigest(100);
    catA.add(4);
    catA.add(2);

    AVLTreeDigest catB = new AVLTreeDigest(100);
    catB.add(-9);
    catB.add(11);
    catB.add(-5);

    AVLTreeDigest all = new AVLTreeDigest(100);
    all.add(catA);
    all.add(catB);

    System.out.println(str(catA));
    System.out.println(str(catB));
    System.out.println(str(all));

    // 2.0 2.2 3.0 3.8 4.0
    // -9.0 -8.2 -5.0 7.800000000000001 11.0
    // -9.0 -7.3999999999999995 2.0 8.200000000000001 11.0
  }

  private static String str(AVLTreeDigest digest) {
    StringBuilder sb = new StringBuilder();
    for (double d : new double[] {0,.1,.5,.9,1}) {
      sb.append(" ").append(digest.quantile(d));
    }
    return sb.toString();
  }

  /*** test code to ensure TDigest is working as we expect. */

  public void XtestTDigest() throws Exception {
    AVLTreeDigest t1 = new AVLTreeDigest(100);
    t1.add(10, 1);
    t1.add(90, 1);
    t1.add(50, 1);

    System.out.println(t1.quantile(0.1));
    System.out.println(t1.quantile(0.5));
    System.out.println(t1.quantile(0.9));

    assertEquals(t1.quantile(0.5), 50.0, 0.01);

    AVLTreeDigest t2 = new AVLTreeDigest(100);
    t2.add(130, 1);
    t2.add(170, 1);
    t2.add(90, 1);

    System.out.println(t2.quantile(0.1));
    System.out.println(t2.quantile(0.5));
    System.out.println(t2.quantile(0.9));

    AVLTreeDigest top = new AVLTreeDigest(100);

    t1.compress();
    ByteBuffer buf = ByteBuffer.allocate(t1.byteSize()); // upper bound
    t1.asSmallBytes(buf);
    byte[] arr1 = Arrays.copyOf(buf.array(), buf.position());

    ByteBuffer rbuf = ByteBuffer.wrap(arr1);
    top.add(AVLTreeDigest.fromBytes(rbuf));

    System.out.println(top.quantile(0.1));
    System.out.println(top.quantile(0.5));
    System.out.println(top.quantile(0.9));

    t2.compress();
    ByteBuffer buf2 = ByteBuffer.allocate(t2.byteSize()); // upper bound
    t2.asSmallBytes(buf2);
    byte[] arr2 = Arrays.copyOf(buf2.array(), buf2.position());

    ByteBuffer rbuf2 = ByteBuffer.wrap(arr2);
    top.add(AVLTreeDigest.fromBytes(rbuf2));

    System.out.println(top.quantile(0.1));
    System.out.println(top.quantile(0.5));
    System.out.println(top.quantile(0.9));
  }

  public void XtestHLL() {
    HLLAgg.HLLFactory fac = new HLLAgg.HLLFactory();
    HLL hll = fac.getHLL();
    hll.addRaw(123456789);
    hll.addRaw(987654321);
  }

}
