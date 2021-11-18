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
package org.apache.solr.search.join;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.JoinQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.toJSONString;

public class TestScoreJoinQPNoScore extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.filterCache.async", "true");
    initCore("solrconfig-basic.xml","schema-docValuesJoin.xml");
  }

  @Test
  public void testJoin() throws Exception {
    assertU(add(doc("id", "1","name_s", "john", "title_s", "Director", "dept_ss","Engineering")));
    assertU(add(doc("id", "2","name_s", "mark", "title_s", "VP", "dept_ss","Marketing")));
    assertU(add(doc("id", "3","name_s", "nancy", "title_s", "MTS", "dept_ss","Sales")));
    assertU(add(doc("id", "4","name_s", "dave", "title_s", "MTS", "dept_ss","Support", "dept_ss","Engineering")));
    assertU(add(doc("id", "5","name_s", "tina", "title_s", "VP", "dept_ss","Engineering")));

    assertU(add(doc("id","10", "dept_id_s", "Engineering", "text_t","These guys develop stuff")));
    assertU(add(doc("id","11", "dept_id_s", "Marketing", "text_t","These guys make you look good")));
    assertU(add(doc("id","12", "dept_id_s", "Sales", "text_t","These guys sell stuff")));
    assertU(add(doc("id","13", "dept_id_s", "Support", "text_t","These guys help customers")));

    assertU(commit());

    // test debugging TODO no debug in JoinUtil
  //  assertJQ(req("q","{!join from=dept_ss to=dept_id_s"+whateverScore()+"}title_s:MTS", "fl","id", "debugQuery","true")
  //      ,"/debug/join/{!join from=dept_ss to=dept_id_s"+whateverScore()+"}title_s:MTS=={'_MATCH_':'fromSetSize,toSetSize', 'fromSetSize':2, 'toSetSize':3}"
  //  );

    assertJQ(req("q","{!join from=dept_ss to=dept_id_s"+whateverScore()+"}title_s:MTS", "fl","id")
        ,"/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'10'},{'id':'12'},{'id':'13'}]}"
    );

    // empty from
    assertJQ(req("q","{!join from=noexist_s to=dept_id_s"+whateverScore()+"}*:*", "fl","id")
        ,"/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}"
    );

    // empty to
    assertJQ(req("q","{!join from=dept_ss to=noexist_s"+whateverScore()+"}*:*", "fl","id")
        ,"/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}"
    );

    // self join... return everyone with she same title as Dave
    assertJQ(req("q","{!join from=title_s to=title_s"+whateverScore()+"}name_s:dave", "fl","id")
        ,"/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'3'},{'id':'4'}]}"
    );

    // find people that develop stuff
    assertJQ(req("q","{!join from=dept_id_s to=dept_ss"+whateverScore()+"}text_t:develop", "fl","id")
        ,"/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
    );

    // self join on multivalued text_t field
    assertJQ(req("q","{!join from=title_s to=title_s"+whateverScore()+"}name_s:dave", "fl","id")
        ,"/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'3'},{'id':'4'}]}"
    );

    assertJQ(req("q","{!join from=dept_ss to=dept_id_s"+whateverScore()+"}title_s:MTS", "fl","id", "debugQuery","true")
        ,"/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'10'},{'id':'12'},{'id':'13'}]}"
    );
    
    // expected outcome for a sub query matching dave joined against departments
    final String davesDepartments = 
      "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'10'},{'id':'13'}]}";

    // straight forward query
    assertJQ(req("q","{!join from=dept_ss to=dept_id_s"+whateverScore()+"}name_s:dave", 
                 "fl","id"),
             davesDepartments);

    // variable deref for sub-query parsing
    assertJQ(req("q","{!join from=dept_ss to=dept_id_s v=$qq"+whateverScore()+"}", 
                 "qq","{!dismax}dave",
                 "qf","name_s",
                 "fl","id", 
                 "debugQuery","true"),
             davesDepartments);

    // variable deref for sub-query parsing w/localparams
    assertJQ(req("q","{!join from=dept_ss to=dept_id_s v=$qq"+whateverScore()+"}", 
                 "qq","{!dismax qf=name_s}dave",
                 "fl","id", 
                 "debugQuery","true"),
             davesDepartments);

    // defType local param to control sub-query parsing
    assertJQ(req("q","{!join from=dept_ss to=dept_id_s defType=dismax"+whateverScore()+"}dave", 
                 "qf","name_s",
                 "fl","id", 
                 "debugQuery","true"),
             davesDepartments);

    // find people that develop stuff - but limit via filter query to a name of "john"
    // this tests filters being pushed down to queries (SOLR-3062)
    assertJQ(req("q","{!join from=dept_id_s to=dept_ss"+whateverScore()+"}text_t:develop", "fl","id", "fq", "name_s:john")
             ,"/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'}]}"
            );
    

   assertJQ(req("q","{!join from=dept_ss to=dept_id_s"+whateverScore()+"}title_s:MTS", "fl","id"
          )
          ,"/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'10'},{'id':'12'},{'id':'13'}]}");

      // find people that develop stuff, even if it's requested as single value
    assertJQ(req("q","{!join from=dept_id_s to=dept_ss"+whateverScore()+"}text_t:develop", "fl","id")
        ,"/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
    );

  }

  public void testNotEquals() throws SyntaxError, IOException{
    try (SolrQueryRequest req = req("*:*")) {
      Query x = QParser.getParser("{!join from=dept_id_s to=dept_ss score=none}text_t:develop", req).getQuery();
      Query y = QParser.getParser("{!join from=dept_ss to=dept_ss score=none}text_t:develop", req).getQuery();
      assertFalse("diff from fields produce equal queries",
                  x.equals(y));
    }
  }
    
  public void testJoinQueryType() throws SyntaxError, IOException{
    SolrQueryRequest req = null;
    try{
      final String score = whateverScore();
      
      req = req("{!join from=dept_id_s to=dept_ss"+score+"}text_t:develop");
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      
      {
        final Query query = QParser.getParser(req.getParams().get("q"), req).getQuery();
        final Query rewrittenQuery = query.rewrite(req.getSearcher().getIndexReader());
        assertEquals(rewrittenQuery+" is expected to be from Solr",
            ScoreJoinQParserPlugin.class.getPackage().getName(), 
            rewrittenQuery.getClass().getPackage().getName());
      }
      {
        final Query query = QParser.getParser(
            "{!join from=dept_id_s to=dept_ss}text_t:develop"
            , req).getQuery();
        final Query rewrittenQuery = query.rewrite(req.getSearcher().getIndexReader());
        assertEquals(rewrittenQuery+" is expected to be from Solr",
              JoinQParserPlugin.class.getPackage().getName(), 
              rewrittenQuery.getClass().getPackage().getName());
      }
    }finally{
      if(req!=null){
        req.close();
      }
      SolrRequestInfo.clearRequestInfo();
    }
  }

  public static String whateverScore() {
      final ScoreMode[] vals = ScoreMode.values();
      return " score="+vals[random().nextInt(vals.length)]+" ";
  }

  @Test
  public void testRandomJoin() throws Exception {
    int indexIter=50 * RANDOM_MULTIPLIER;
    int queryIter=50 * RANDOM_MULTIPLIER;

    // groups of fields that have any chance of matching... used to
    // increase test effectiveness by avoiding 0 resultsets much of the time.
    String[][] compat = new String[][] {
        {"small_s_dv","small2_s_dv","small2_ss_dv","small3_ss_dv"},
        {"small_i_dv","small2_i_dv","small2_is_dv","small3_is_dv"}
    };


    while (--indexIter >= 0) {
      int indexSize = random().nextInt(20 * RANDOM_MULTIPLIER);

      List<FldType> types = new ArrayList<FldType>();
      types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
      /** no numeric fields so far LUCENE-5868
      types.add(new FldType("score_f_dv",ONE_ONE, new FVal(1,100)));  // field used to score
      **/
      types.add(new FldType("small_s_dv",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
      types.add(new FldType("small2_s_dv",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
      types.add(new FldType("small2_ss_dv",ZERO_TWO, new SVal('a',(char)('c'+indexSize/3),1,1)));
      types.add(new FldType("small3_ss_dv",new IRange(0,25), new SVal('A','z',1,1)));
      /** no numeric fields so far LUCENE-5868
      types.add(new FldType("small_i_dv",ZERO_ONE, new IRange(0,5+indexSize/3)));
      types.add(new FldType("small2_i_dv",ZERO_ONE, new IRange(0,5+indexSize/3)));
      types.add(new FldType("small2_is_dv",ZERO_TWO, new IRange(0,5+indexSize/3)));
      types.add(new FldType("small3_is_dv",new IRange(0,25), new IRange(0,100)));
      **/

      clearIndex();
      Map<Comparable, Doc> model = indexDocs(types, null, indexSize);
      Map<String, Map<Comparable, Set<Comparable>>> pivots = new HashMap<String, Map<Comparable, Set<Comparable>>>();

      for (int qiter=0; qiter<queryIter; qiter++) {
        String fromField;
        String toField;
        if (random().nextInt(100) < 5) {
          // pick random fields 5% of the time
          fromField = types.get(random().nextInt(types.size())).fname;
          // pick the same field 50% of the time we pick a random field (since other fields won't match anything)
          toField = (random().nextInt(100) < 50) ? fromField : types.get(random().nextInt(types.size())).fname;
        } else {
          // otherwise, pick compatible fields that have a chance of matching indexed tokens
          String[] group = compat[random().nextInt(compat.length)];
          fromField = group[random().nextInt(group.length)];
          toField = group[random().nextInt(group.length)];
        }

        Map<Comparable, Set<Comparable>> pivot = pivots.get(fromField+"/"+toField);
        if (pivot == null) {
          pivot = createJoinMap(model, fromField, toField);
          pivots.put(fromField+"/"+toField, pivot);
        }

        Collection<Doc> fromDocs = model.values();
        Set<Comparable> docs = join(fromDocs, pivot);
        List<Doc> docList = new ArrayList<Doc>(docs.size());
        for (Comparable id : docs) docList.add(model.get(id));
        Collections.sort(docList, createComparator("_docid_",true,false,false,false));
        List sortedDocs = new ArrayList();
        for (Doc doc : docList) {
          if (sortedDocs.size() >= 10) break;
          sortedDocs.add(doc.toObject(h.getCore().getLatestSchema()));
        }

        Map<String,Object> resultSet = new LinkedHashMap<String,Object>();
        resultSet.put("numFound", docList.size());
        resultSet.put("start", 0);
        resultSet.put("numFoundExact", true);
        resultSet.put("docs", sortedDocs);

        // todo: use different join queries for better coverage

        SolrQueryRequest req = req("wt","json","indent","true", "echoParams","all",
            "q","{!join from="+fromField+" to="+toField
                +" "+ (random().nextBoolean() ? "fromIndex=collection1" : "")
                +" "+ (random().nextBoolean() ? "TESTenforceSameCoreAsAnotherOne=true" : "")
                +" "+whateverScore()+"}*:*"
                , "sort", "_docid_ asc"
        );

        String strResponse = h.query(req);

        Object realResponse = Utils.fromJSONString(strResponse);
        String err = JSONTestUtil.matchObj("/response", realResponse, resultSet);
        if (err != null) {
          final String m = "JOIN MISMATCH: " + err
           + "\n\trequest="+req
           + "\n\tresult="+strResponse
           + "\n\texpected="+ toJSONString(resultSet)
          ;// + "\n\tmodel="+ JSONUtil.toJSON(model);
          log.error(m);
          {
            SolrQueryRequest f = req("wt","json","indent","true", "echoParams","all",
              "q","*:*", "facet","true",
              "facet.field", fromField 
                  , "sort", "_docid_ asc"
                  ,"rows","0"
                );
            log.error("faceting on from field: {}", h.query(f));
          }
          {
            final Map<String,String> ps = ((MapSolrParams)req.getParams()).getMap();
            final String q = ps.get("q");
            ps.put("q", q.replaceAll("join score=none", "join"));
            log.error("plain join: {}", h.query(req));
            ps.put("q", q);
            
          }
          {
          // re-execute the request... good for putting a breakpoint here for debugging
          final Map<String,String> ps = ((MapSolrParams)req.getParams()).getMap();
          final String q = ps.get("q");
          ps.put("q", q.replaceAll("\\}", " cache=false\\}"));
          String rsp = h.query(req);
          }
          fail(err);
        }

      }
    }
  }

  Map<Comparable, Set<Comparable>> createJoinMap(Map<Comparable, Doc> model, String fromField, String toField) {
    Map<Comparable, Set<Comparable>> id_to_id = new HashMap<Comparable, Set<Comparable>>();

    Map<Comparable, List<Comparable>> value_to_id = invertField(model, toField);

    for (Comparable fromId : model.keySet()) {
      Doc doc = model.get(fromId);
      List<Comparable> vals = doc.getValues(fromField);
      if (vals == null) continue;
      for (Comparable val : vals) {
        List<Comparable> toIds = value_to_id.get(val);
        if (toIds == null) continue;
        Set<Comparable> ids = id_to_id.get(fromId);
        if (ids == null) {
          ids = new HashSet<Comparable>();
          id_to_id.put(fromId, ids);
        }
        for (Comparable toId : toIds)
          ids.add(toId);
      }
    }

    return id_to_id;
  }


  Set<Comparable> join(Collection<Doc> input, Map<Comparable, Set<Comparable>> joinMap) {
    Set<Comparable> ids = new HashSet<Comparable>();
    for (Doc doc : input) {
      Collection<Comparable> output = joinMap.get(doc.id);
      if (output == null) continue;
      ids.addAll(output);
    }
    return ids;
  }

}
