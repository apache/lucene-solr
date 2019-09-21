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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.SolrPluginUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExtendedDismaxParser extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
    index();
  }
  
  public static void index() throws Exception {
    assertU(adoc("id", "42", "trait_ss", "Tool", "trait_ss", "Obnoxious",
            "name", "Zapp Brannigan"));
    assertU(adoc("id", "43" ,
            "title", "Democratic Order op Planets"));
    assertU(adoc("id", "44", "trait_ss", "Tool",
            "name", "The Zapper"));
    assertU(adoc("id", "45", "trait_ss", "Chauvinist",
            "title", "25 star General"));
    assertU(adoc("id", "46", 
                 "trait_ss", "Obnoxious",
                 "subject", "Defeated the pacifists op the Gandhi nebula",
                 "t_special", "literal:colon value",
                 "movies_t", "first is Mission: Impossible, second is Terminator 2: Judgement Day.  Terminator:3 ok...",
                 "foo_i", "8"
    ));
    assertU(adoc("id", "47", "trait_ss", "Pig",
            "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
    assertU(adoc("id", "48", "text_sw", "this has gigabyte potential", "foo_i","100"));
    assertU(adoc("id", "49", "text_sw", "start the big apple end", "foo_i","-100"));
    assertU(adoc("id", "50", "text_sw", "start new big city end"));
    assertU(adoc("id", "51", "store",   "12.34,-56.78"));
    assertU(adoc("id", "52", "text_sw", "tekna theou klethomen"));
    assertU(adoc("id", "53", "text_sw", "nun tekna theou esmen"));
    assertU(adoc("id", "54", "text_sw", "phanera estin ta tekna tou theou"));
    assertU(adoc("id", "55", "standardtok", "大"));
    assertU(adoc("id", "56", "standardtok", "大亚"));
    assertU(adoc("id", "57", "standardtok", "大亚湾"));
    assertU(adoc("id", "58", "HTMLstandardtok", "大"));
    assertU(adoc("id", "59", "HTMLstandardtok", "大亚"));
    assertU(adoc("id", "60", "HTMLstandardtok", "大亚湾"));
    assertU(adoc("id", "61", "text_sw", "bazaaa")); // synonyms in an expansion group
    assertU(adoc("id", "62", "text_sw", "oil stocks"));
    assertU(adoc("id", "63", "text_sw", "gold stocks"));
    assertU(adoc("id", "64", "text_sw", "stocks gold stockade"));
    assertU(adoc("id", "65", "text_sw", "snake oil"));
    // SOLR-8812 user query example
    assertU(adoc("id", "66", "text_sw", "hair ties barbie"));
    assertU(adoc("id", "67", "text_sw", "hair ties"));
    assertU(adoc("id", "68", "text_sw", "hair barbie"));
    assertU(adoc("id", "69", "text_sw", "ties barbie"));
    assertU(adoc("id", "70", "text_sw", "hair"));
    assertU(adoc("id", "71", "text_sw", "ties"));
    assertU(adoc("id", "72", "text_sw", "wifi ATM"));
    assertU(adoc("id", "73", "shingle23", "A B X D E"));
    assertU(adoc("id", "74", "isocharfilter", "niño"));
//    assertU(adoc("id", "74", "text_pick_best", "tabby"));
//    assertU(adoc("id", "74", "text_as_distinct", "persian"));

    assertU(commit());
  }

  @Test
  public void testSyntax() throws Exception {
    for (String sow : Arrays.asList("true", "false")) {
      // a bare * should be treated as *:*
      assertJQ(req("defType", "edismax", "q", "*", "df", "doesnotexist_s", "sow", sow)
          , "/response/docs/[0]=="   // make sure we get something...
      );
      assertJQ(req("defType", "edismax", "q", "doesnotexist_s:*", "sow", sow)
          , "/response/numFound==0"   // nothing should be found
      );
      assertJQ(req("defType", "edismax", "q", "doesnotexist_s:*", "sow", sow)
          , "/response/numFound==0"   // nothing should be found
      );
      assertJQ(req("defType", "edismax", "q", "doesnotexist_s:( * * * )", "sow", sow)
          , "/response/numFound==0"   // nothing should be found
      );
    }
  }


  public void testTrailingOperators() throws Exception {
    for (String sow : Arrays.asList("true", "false")) {
      // really just test that exceptions aren't thrown by
      // single + -

      assertJQ(req("defType", "edismax", "q", "-", "sow", sow)
          , "/response==");

      assertJQ(req("defType", "edismax", "q", "+", "sow", sow)
          , "/response==");

      assertJQ(req("defType", "edismax", "q", "+ - +", "sow", sow)
          , "/response==");

      assertJQ(req("defType", "edismax", "q", "- + -", "sow", sow)
          , "/response==");

      assertJQ(req("defType", "edismax", "q", "id:47 +", "sow", sow)
          , "/response/numFound==1");

      assertJQ(req("defType", "edismax", "q", "id:47 -", "sow", sow)
          , "/response/numFound==1");

      Random r = random();
      for (int i=0; i<100; i++) {
        StringBuilder sb = new StringBuilder();
        for (int j=0; j<r.nextInt(10); j++) {
          switch (r.nextInt(3)) {
            case 0: sb.append(' '); break;
            case 1: sb.append('+'); break;
            case 2: sb.append('-'); break;
            case 3: sb.append((char)r.nextInt(127)); break;
          }
        }

        String q = sb.toString();
        assertJQ(req("defType", "edismax", "q", q, "sow", sow)
            , "/response==");
      }
    }
  }


  public void testLowercaseOperators() {
    for (String sow : Arrays.asList("true", "false")) {
      assertQ("Upper case operator",
          req("q", "Zapp AND Brannigan",
              "qf", "name",
              "lowercaseOperators", "false",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");

      assertQ("Upper case operator, allow lowercase",
          req("q", "Zapp AND Brannigan",
              "qf", "name",
              "lowercaseOperators", "true",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");

      assertQ("Lower case operator, don't allow lowercase operators",
          req("q", "Zapp and Brannigan",
              "qf", "name",
              "q.op", "AND",
              "lowercaseOperators", "false",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=0]");

      assertQ("The default for lowercaseOperators should not allow lower case and",
          req("q", "Zapp and Brannigan",
              "qf", "name",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=0]");

      assertQ("Lower case operator, allow lower case operators",
          req("q", "Zapp and Brannigan",
              "qf", "name",
              "q.op", "AND",
              "lowercaseOperators", "true",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");
    }
  }

  public void testCharFilter() throws Exception {
    // test that charfilter was applied by the indexer
    assertQ(req("defType", "edismax",
        "stopwords","false",
        "qf", "isocharfilter",
        "q","nino"), "*[count(//doc)=1]"
    );

    // test that charfilter was applied to the query
    assertQ(req("defType", "edismax",
        "stopwords","false",
        "qf", "isocharfilter",
        "q","niño"), "*[count(//doc)=1]"
    );
  }

  // test the edismax query parser based on the dismax parser
  public void testFocusQueryParser() {
    String allq = "id:[42 TO 51]";
    String allr = "*[count(//doc)=10]";
    String oner = "*[count(//doc)=1]";
    String twor = "*[count(//doc)=2]";
    String nor = "*[count(//doc)=0]";
    
    assertQ("blank q",
        req("q"," ",
            "q.alt",allq,
            "defType","edismax")
        ,allr);
    
    assertQ("expected doc is missing (using un-escaped edismax w/qf)",
          req("q", "literal:colon", 
              "qf", "t_special",
              "defType", "edismax"),
          "//doc[1]/str[@name='id'][.='46']"); 

    assertQ("standard request handler returns all matches",
            req(allq),
            allr
    );

   assertQ("edismax query parser returns all matches",
            req("q", allq,
                "defType", "edismax"
            ),
            allr
    );

   assertQ(req("defType", "edismax", "qf", "trait_ss",
               "q","Tool"), twor
    );

   // test that field types that aren't applicable don't cause an exception to be thrown
   assertQ(req("defType", "edismax", "qf", "trait_ss foo_i foo_f foo_dt foo_l foo_d foo_b",
               "q","Tool"), twor
    );

   // test that numeric field types can be queried
   assertQ(req("defType", "edismax", "qf", "text_sw",
               "q","foo_i:100"), oner
    );

   // test that numeric field types can be queried
   assertQ(req("defType", "edismax", "qf", "text_sw",
               "q","foo_i:-100"), oner
    );

   // test that numeric field types can be queried  via qf
   assertQ(req("defType", "edismax", "qf", "text_sw foo_i",
               "q","100"), oner
    );

    assertQ("qf defaults to df",
        req("defType", "edismax", "df", "trait_ss",
        "q","Tool"), twor
    );

   assertQ("qf defaults to defaultSearchField"
           , req( "defType", "edismax"
                 ,"q","op")
           , twor
           );
   
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","op"), twor
    );
   assertQ(req("defType", "edismax", 
               "qf", "name title subject text_sw",
               "q.op", "AND",
               "q","Order op"), oner
    );
   assertQ(req("defType", "edismax", 
               "qf", "name title subject text_sw",
               "q.op", "OR",
               "q","Order op"), twor
    );
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order AND op"), oner
    );
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order and op"), twor
    );
    assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","+Order op"), oner
    );
    assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order OR op"), twor
    );
    assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order or op"), twor
    );
    assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","*:*"), allr
    );

    assertQ(req("defType", "edismax", "qf", "name title subject text",
           "q","star OR (-star)"), allr
    );
    assertQ(req("defType", "edismax", "qf", "name title subject text",
           "q","id:42 OR (-id:42)"), allr
    );

    // test that basic synonyms work
    assertQ(req("defType", "edismax", "qf", "text_sw",
           "q","GB"), oner
    );

    // test for stopword removal in main query part
    assertQ(req("defType", "edismax", "qf", "text_sw",
           "q","the big"), twor
    );

    // test for stopwords not removed   
    assertQ(req("defType", "edismax", 
                "qf", "text_sw", 
                "stopwords","false",
                "q.op","AND",
                "q","the big"), oner
    );

    // searching for a literal colon value when clearly not used for a field
    assertQ("expected doc is missing (using standard)",
            req("q", "t_special:literal\\:colon"),
            "//doc[1]/str[@name='id'][.='46']"); 
    assertQ("expected doc is missing (using escaped edismax w/field)",
            req("q", "t_special:literal\\:colon", 
                "defType", "edismax"),
            "//doc[1]/str[@name='id'][.='46']"); 
    assertQ("expected doc is missing (using un-escaped edismax w/field)",
            req("q", "t_special:literal:colon", 
                "defType", "edismax"),
            "//doc[1]/str[@name='id'][.='46']"); 
    assertQ("expected doc is missing (using escaped edismax w/qf)",
            req("q", "literal\\:colon", 
                "qf", "t_special",
                "defType", "edismax"),
            "//doc[1]/str[@name='id'][.='46']"); 
    assertQ("expected doc is missing (using un-escaped edismax w/qf)",
            req("q", "literal:colon", 
                "qf", "t_special",
                "defType", "edismax"),
            "//doc[1]/str[@name='id'][.='46']");

    assertQ(req("defType","edismax", "mm","100%", "q","terminator:3", "qf","movies_t"),
            oner);
    assertQ(req("defType","edismax", "mm","100%", "q","Mission:Impossible", "qf","movies_t"),
            oner);
    assertQ(req("defType","edismax", "mm","100%", "q","Mission : Impossible", "qf","movies_t"),
            oner);
    assertQ(req("defType","edismax", "mm","100%", "q","Mission: Impossible", "qf","movies_t"),
            oner);
    assertQ(req("defType","edismax", "mm","100%", "q","Terminator 2: Judgement Day", "qf","movies_t"),
            oner);

    // make sure the clause wasn't eliminated
    assertQ(req("defType","edismax", "mm","100%", "q","Terminator 10: Judgement Day", "qf","movies_t"),
            nor);

    // throw in a numeric field
    assertQ(req("defType","edismax", "mm","0", "q","Terminator: 100", "qf","movies_t foo_i"),
            twor);

    assertQ(req("defType","edismax", "mm","100%", "q","Terminator: 100", "qf","movies_t foo_i", "sow","true"),
            nor);
    // When sow=false, the per-field query structures differ (no "Terminator" query on integer field foo_i),
    // so a dismax-per-field is constructed.  As a result, mm=100% is applied per-field instead of per-term;
    // since there is only one term (100) required in the foo_i field's dismax, the query can match docs that
    // only have the 100 term in the foo_i field, and don't necessarily have "Terminator" in any field.
    assertQ(req("defType","edismax", "mm","100%", "q","Terminator: 100", "qf","movies_t foo_i", "sow","false"),
            oner);
    assertQ(req("defType","edismax", "mm","100%", "q","Terminator: 100", "qf","movies_t foo_i"), // default sow=false
        oner);

    assertQ(req("defType","edismax", "mm","100%", "q","Terminator: 8", "qf","movies_t foo_i"),
            oner);

    assertQ(req("defType","edismax", "mm","0", "q","movies_t:Terminator 100", "qf","movies_t foo_i"),
            twor);
    
    // special psuedo-fields like _query_ and _val_

    // _query_ should be excluded by default
    assertQ(req("defType", "edismax", 
                "mm", "100%",
                "fq", "id:51",
                "q", "_query_:\"{!geofilt d=20 sfield=store pt=12.34,-56.78}\"",
                "debugQuery", "true"),
            nor,
        "//str[@name='parsedquery_toString'][.='+(((text:queri) (text:\"geofilt d 20 sfield store pt 12 34 56 78\"))~2)']");
    // again; this time use embedded local-params style
    assertQ(req("defType", "edismax",
        "mm", "100%",
        "fq", "id:51",
        "q", " {!geofilt d=20 sfield=store pt=12.34,-56.78}"),//notice leading space
        nor);

    // should work when explicitly allowed
    assertQ(req("defType", "edismax", 
                "mm", "100%",
                "fq", "id:51",
                "uf", "id _query_",
                "q", "_query_:\"{!geofilt d=20 sfield=store pt=12.34,-56.78}\""),
            oner);
    assertQ(req("defType", "edismax", 
                "mm", "100%",
                "fq", "id:51",
                "uf", "id",
                "uf", "_query_",
                "q", "_query_:\"{!geofilt d=20 sfield=store pt=12.34,-56.78}\""),
            oner);
    // again; this time use embedded local-params style
    assertQ(req("defType", "edismax",
        "mm", "100%",
        "fq", "id:51",
        "uf", "id",
        "uf", "_query_",
        "q", " {!geofilt d=20 sfield=store pt=12.34,-56.78}"),//notice leading space
        oner);

    // should fail when prohibited
    assertQ(req("defType", "edismax", 
                "mm", "100%",
                "fq", "id:51",
                "uf", "* -_query_", // explicitly excluded
                "q", "_query_:\"{!geofilt d=20 sfield=store pt=12.34,-56.78}\""),
            nor);
    assertQ(req("defType", "edismax", 
                "mm", "100%",
                "fq", "id:51",
                "uf", "id", // excluded by omission
                "q", "_query_:\"{!geofilt d=20 sfield=store pt=12.34,-56.78}\""),
            nor);


    /** stopword removal in conjunction with multi-word synonyms at query time
     * break this test.
     // multi-word synonyms
     // remove id:50 which contans the false match      
    assertQ(req("defType", "edismax", "qf", "text_t", "indent","true", "debugQuery","true",
           "q","-id:50 nyc"), oner
    );
    **/

    /*** these fail because multi-word synonyms are being used at query time
    // this will incorrectly match "new big city"
    assertQ(req("defType", "edismax", "qf", "id title",
           "q","nyc"), oner
    );

    // this will incorrectly match "new big city"
    assertQ(req("defType", "edismax", "qf", "title",
           "q","the big apple"), nor
    );
    ***/

  }
  
  public void testBoostQuery() {
    assertQ(
        req("q", "tekna", "qf", "text_sw", "defType", "edismax", "bq", "id:54^100", "bq", "id:53^10", "fq", "id:[52 TO 54]", "fl", "id,score"), 
        "//doc[1]/str[@name='id'][.='54']",
        "//doc[2]/str[@name='id'][.='53']",
        "//doc[3]/str[@name='id'][.='52']"
     );
    
    // non-trivial bqs
    assertQ(req("q", "tekna", 
                "qf", "text_sw", 
                "defType", "edismax", 
                "bq", "(text_sw:blasdfadsf id:54)^100", 
                "bq", "id:[53 TO 53]^10", 
                "fq", "id:[52 TO 54]", 
                "fl", "id,score"), 
            "//doc[1]/str[@name='id'][.='54']",
            "//doc[2]/str[@name='id'][.='53']",
            "//doc[3]/str[@name='id'][.='52']"
            );

    // genuine negative boosts are not legal
    // see SOLR-3823, SOLR-3278, LUCENE-4378 and 
    // https://wiki.apache.org/solr/SolrRelevancyFAQ#How_do_I_give_a_negative_.28or_very_low.29_boost_to_documents_that_match_a_query.3F
    assertQ(
        req("q", "tekna", "qf", "text_sw", "defType", "edismax", "bq", "(*:* -id:54)^100", "bq", "id:53^10", "bq", "id:52", "fq", "id:[52 TO 54]", "fl", "id,score"), 
        "//doc[1]/str[@name='id'][.='53']",
        "//doc[2]/str[@name='id'][.='52']",
        "//doc[3]/str[@name='id'][.='54']"
     );
  }

  @Test
  public void testBf() {
    assertQ(
        req("q", "tekna", "qf", "text_sw", "defType", "edismax", "bf", "ord(id)", "fq", "id:[52 TO 54]", "fl", "id,score"),
        "//doc[1]/str[@name='id'][.='54']",
        "//doc[2]/str[@name='id'][.='53']",
        "//doc[3]/str[@name='id'][.='52']"
    );

    assertQ(req("q", "tekna", "qf", "text_sw", "defType", "edismax",
        "bf", "if(and(query({!v='id:53'})),120,if(query({!v='id:52'}),10,0))", "fq", "id:[52 TO 54]", "fl", "id,score"),
        "//doc[1]/str[@name='id'][.='53']",
        "//doc[2]/str[@name='id'][.='52']",
        "//doc[3]/str[@name='id'][.='54']");

    // adding value from a field
    // 0 would be returned for negative values or docs w/o a value
    assertQ(req("q", "*:*", "qf", "text_sw", "defType", "edismax",
                "bf", "foo_i", "fq", "id:[47 TO 49]", "fl", "id,score"),
            "//doc[1]/str[@name='id'][.='48']",
            // these should have identical score, in non-deterministic order
            "//doc[str[@name='id'][.='47'] and float[@name='score'][.='1.0']]",
            "//doc[str[@name='id'][.='49'] and float[@name='score'][.='1.0']]");
  }

  @Test
  public void testBoost() {
    assertQ(
        req("q", "*:*", "qf", "text_sw", "defType", "edismax", "boost", "exists(foo_i)", "fq", "id:[47 TO 49]",
            "fl", "id,score", "boost", "if(not(query({!v=id:49})),10,1)"),
        "//doc[1]/str[@name='id'][.='48']",
        "//doc[2]/str[@name='id'][.='49']",
        "//doc[3]/str[@name='id'][.='47']"
    );

    assertQ(req("q", "tekna", "qf", "text_sw", "defType", "edismax",
        "boost", "if(and(query({!v='id:53'})),120,if(query({!v='id:52'}),0.0002,1))", "fq", "id:[52 TO 54]", "fl", "id,score"),
        "//doc[1]/str[@name='id'][.='53']",
        "//doc[2]/str[@name='id'][.='54']",
        "//doc[3]/str[@name='id'][.='52']");

    // adding value from a field
    // using sum to verify the order
    // 0 would be returned for negative values or if the field value is not present
    assertQ(req("q", "*:*", "qf", "text_sw", "defType", "edismax",
        "boost", "sum(foo_i,1)", "fq", "id:[48 TO 50]", "fl", "id,score"),
        "//doc[1]/str[@name='id'][.='48']",
        "//doc[2]/str[@name='id'][.='50']",
        "//doc[3]/str[@name='id'][.='49']");
  }

  public void testUserFields() {
    String allr = "*[count(//doc)=10]";
    String oner = "*[count(//doc)=1]";
    String nor = "*[count(//doc)=0]";
    
    // User fields
    // Default is allow all "*"
    // If a list of fields are given, only those are allowed "foo bar"
    // Possible to invert with "-" syntax:
    //   Disallow all: "-*"
    //   Allow all but id: "* -id"
    // Also supports "dynamic" field name wildcarding
    assertQ(req("defType","edismax", "q","id:42"),
        oner);
    
    // SOLR-3377 - parens should be allowed immediately before field name
    assertQ(req("defType","edismax", "q","( id:42 )"),
        oner);
    assertQ(req("defType","edismax", "q","(id:42)"),
        oner);
    assertQ(req("defType","edismax", "q","(+id:42)"),
        oner);
    assertQ(req("defType","edismax", "q","+(+id:42)"),
        oner);
    assertQ(req("defType","edismax", "q","+(+((id:42)))"),
        oner);
    assertQ(req("defType","edismax", "q","+(+((+id:42)))"),
        oner);
    assertQ(req("defType","edismax", "q"," +( +( ( +id:42) ) ) "),
        oner);
    assertQ(req("defType","edismax", "q","(id:(*:*)^200)"),
        allr);

    assertQ(req("defType","edismax", "uf","id", "q","id:42"),
        oner);
    
    assertQ(req("defType","edismax", "uf","-*", "q","id:42"),
        nor);
    
    assertQ(req("defType","edismax", "uf","loremipsum", "q","id:42"),
        nor);
    
    assertQ(req("defType","edismax", "uf","* -id", "q","id:42"),
        nor);
    
    assertQ(req("defType","edismax", "uf","* -loremipsum", "q","id:42"),
        oner);
    
    assertQ(req("defType","edismax", "uf","id^5.0", "q","id:42"),
        oner);
    
    assertQ(req("defType","edismax", "uf","*^5.0", "q","id:42"),
        oner);
    
    assertQ(req("defType","edismax", "uf","id^5.0", "q","id:42^10.0"),
        oner);
    
    assertQ(req("defType","edismax", "uf","na*", "q","name:Zapp"),
        oner);
    
    assertQ(req("defType","edismax", "uf","*me", "q","name:Zapp"),
        oner);
    
    assertQ(req("defType","edismax", "uf","* -na*", "q","name:Zapp"),
        nor);
    
    assertQ(req("defType","edismax", "uf","*me -name", "q","name:Zapp"),
        nor);
    
    assertQ(req("defType","edismax", "uf","*ame -*e", "q","name:Zapp"),
        nor);
    
    // Boosts from user fields
    assertQ(req("defType","edismax", "debugQuery","true", "rows","0", "q","id:42"),
        "//str[@name='parsedquery_toString'][.='+id:42']");
    
    assertQ(req("defType","edismax", "debugQuery","true", "rows","0", "uf","*^5.0", "q","id:42"),
        "//str[@name='parsedquery_toString'][.='+(id:42)^5.0']");
    
    assertQ(req("defType","edismax", "debugQuery","true", "rows","0", "uf","*^2.0 id^5.0 -xyz", "q","name:foo"),
        "//str[@name='parsedquery_toString'][.='+(name:foo)^2.0']");
    
    assertQ(req("defType","edismax", "debugQuery","true", "rows","0", "uf","i*^5.0", "q","id:42"),
        "//str[@name='parsedquery_toString'][.='+(id:42)^5.0']");
    
    
    assertQ(req("defType","edismax", "uf","-*", "q","cannons", "qf","text"),
        oner);
    
    assertQ(req("defType","edismax", "uf","* -id", "q","42", "qf", "id"), oner);
    
  }
  
  public void testAliasing() throws Exception {
    String oner = "*[count(//doc)=1]";
    String twor = "*[count(//doc)=2]";
    String nor = "*[count(//doc)=0]";
    
 // Aliasing
    // Single field
    assertQ(req("defType","edismax", "q","myalias:Zapp"),
        nor);
    
    assertQ(req("defType","edismax", "q","myalias:Zapp", "f.myalias.qf","name"),
        oner);
    
    // Multi field
    assertQ(req("defType","edismax", "uf", "myalias", "q","myalias:(Zapp Obnoxious)", "f.myalias.qf","name^2.0 mytrait_ss^5.0", "mm", "50%"),
        oner);
    
    // Multi field
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "f.myalias.qf","name^2.0 mytrait_ss^5.0"),
        nor);
    
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "qf","myalias^10.0", "f.myalias.qf","name^2.0 mytrait_ss^5.0"), oner);
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "qf","myalias^10.0", "f.myalias.qf","name^2.0 trait_ss^5.0"), twor);
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "qf","myalias^10.0", "f.myalias.qf","name^2.0 trait_ss^5.0", "mm", "100%"), oner);
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "qf","who^10.0 where^3.0", "f.who.qf","name^2.0", "f.where.qf", "mytrait_ss^5.0"), oner);
    
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "qf","myalias", "f.myalias.qf","name mytrait_ss", "uf", "myalias"), oner);
    
    assertQ(req("defType","edismax", "uf","who", "q","who:(Zapp Obnoxious)", "f.who.qf", "name^2.0 trait_ss^5.0", "qf", "id"), twor);
    assertQ(req("defType","edismax", "uf","* -name", "q","who:(Zapp Obnoxious)", "f.who.qf", "name^2.0 trait_ss^5.0"), twor);
    
  }
  
  public void testAliasingBoost() throws Exception {
    assertQ(req("defType","edismax", "q","Zapp Pig", "qf","myalias", "f.myalias.qf","name trait_ss^0.1"), "//result/doc[1]/str[@name='id']=42", "//result/doc[2]/str[@name='id']=47");//doc 42 should score higher than 46
    assertQ(req("defType","edismax", "q","Zapp Pig", "qf","myalias^100 name", "f.myalias.qf","trait_ss^0.1"), "//result/doc[1]/str[@name='id']=47", "//result/doc[2]/str[@name='id']=42");//Now the order should be inverse
  }
  
  public void testCyclicAliasing() throws Exception {
    try {
      ignoreException(".*Field aliases lead to a cycle.*");

      SolrException e = expectThrows(SolrException.class, "Simple cyclic alising not detected",
          () -> h.query(req("defType","edismax", "q","blarg", "qf","who", "f.who.qf","name","f.name.qf","who")));
      assertCyclicDetectionErrorMessage(e);

      e = expectThrows(SolrException.class, "Cyclic alising not detected",
          () -> h.query(req("defType","edismax", "q","blarg", "qf","who", "f.who.qf","name","f.name.qf","myalias", "f.myalias.qf","who")));
      assertCyclicDetectionErrorMessage(e);

      e = expectThrows(SolrException.class, "Cyclic aliasing not detected", () -> h.query(req("defType","edismax", "q","blarg", "qf","field1", "f.field1.qf","field2 field3","f.field2.qf","field4 field5", "f.field4.qf","field5", "f.field5.qf","field6", "f.field3.qf","field6")));
      assertFalse("This is not cyclic aliasing", e.getCause().getMessage().contains("Field aliases lead to a cycle"));
      assertTrue("Should throw exception due to invalid field name", e.getCause().getMessage().contains("not a valid field name"));

      e = expectThrows(SolrException.class, "Cyclic alising not detected",
          () -> h.query(req("defType","edismax", "q","blarg", "qf","field1", "f.field1.qf","field2 field3", "f.field2.qf","field4 field5", "f.field4.qf","field5", "f.field5.qf","field4")));
      assertCyclicDetectionErrorMessage(e);

      e = expectThrows(SolrException.class, "Cyclic alising not detected",
          () -> h.query(req("defType","edismax", "q","who:(Zapp Pig)", "qf","text", "f.who.qf","name","f.name.qf","myalias", "f.myalias.qf","who")));
      assertCyclicDetectionErrorMessage(e);
    } finally {
      resetExceptionIgnores();
    }
  }

  private void assertCyclicDetectionErrorMessage(SolrException e) {
    assertTrue(e.getCause().getMessage().contains("Field aliases lead to a cycle"));
  }

  public void testOperatorsWithLiteralColons() {
    assertU(adoc("id", "142", "a_s", "bogus:xxx", "text_s", "yak"));
    assertU(adoc("id", "143", "a_s", "bogus:xxx"));
    assertU(adoc("id", "144", "text_s", "yak"));
    assertU(adoc("id", "145", "a_s", "a_s:xxx", "text_s", "yak"));
    assertU(adoc("id", "146", "a_s", "a_s:xxx"));
    assertU(adoc("id", "147", "a_s", "AND", "a_s", "NOT"));
    assertU(commit());

    assertQ(req("q", "bogus:xxx AND text_s:yak",
                "fl", "id",
                "qf", "a_s b_s",
                "defType", "edismax",
                "mm", "0"),
            "//*[@numFound='1']",
            "//str[@name='id'][.='142']");
    
    assertQ(req("q", "a_s:xxx AND text_s:yak",
                "fl", "id",
                "qf", "a_s b_s",
                "defType", "edismax",
                "mm", "0",
                "uf", "text_s"),
            "//*[@numFound='1']",
            "//str[@name='id'][.='145']");

    assertQ(req("q", "NOT bogus:xxx +text_s:yak",
                "fl", "id",
                "qf", "a_s b_s",
                "defType", "edismax",
                "mm", "0",
                "debugQuery", "true"),
            "//*[@numFound='2']",
            "//str[@name='id'][.='144']",
            "//str[@name='id'][.='145']");
    
    assertQ(req("q", "NOT a_s:xxx +text_s:yak",
                "fl", "id",
                "qf", "a_s b_s",
                "defType", "edismax",
                "mm", "0",
                "uf", "text_s"),
            "//*[@numFound='2']",
            "//str[@name='id'][.='142']",
            "//str[@name='id'][.='144']");
    
    assertQ(req("q", "+bogus:xxx yak",
                "fl", "id",
                "qf", "a_s b_s text_s",
                "defType", "edismax",
                "mm", "0"),
            "//*[@numFound='2']",
            "//str[@name='id'][.='142']",
            "//str[@name='id'][.='143']");

    assertQ(req("q", "+a_s:xxx yak",
                "fl", "id",
                "qf", "a_s b_s text_s",
                "defType", "edismax",
                "mm", "0",
                "uf", "b_s"),
            "//*[@numFound='2']",
            "//str[@name='id'][.='145']",
            "//str[@name='id'][.='146']");
  }
  
  // test phrase fields including pf2 pf3 and phrase slop
  public void testPfPs() {
    assertU(adoc("id", "s0", "phrase_sw", "foo bar a b c", "boost_d", "1.0"));    
    assertU(adoc("id", "s1", "phrase_sw", "foo a bar b c", "boost_d", "2.0"));    
    assertU(adoc("id", "s2", "phrase_sw", "foo a b bar c", "boost_d", "3.0"));    
    assertU(adoc("id", "s3", "phrase_sw", "foo a b c bar", "boost_d", "4.0"));    
    assertU(commit());

    assertQ("default order assumption wrong",
        req("q", "foo bar",
            "qf", "phrase_sw",
            "bf", "boost_d",
            "fl", "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s3']",
        "//doc[2]/str[@name='id'][.='s2']",
        "//doc[3]/str[@name='id'][.='s1']",
        "//doc[4]/str[@name='id'][.='s0']"); 

    assertQ("pf not working",
        req("q", "foo bar",
            "qf", "phrase_sw",
            "pf", "phrase_sw^10",
            "fl", "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s0']");
    
    assertQ("pf2 not working",
        req("q",   "foo bar", 
            "qf",  "phrase_sw",
            "pf2", "phrase_sw^10",
            "fl",  "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s0']"); 

    assertQ("pf3 not working",
        req("q",   "a b bar", 
            "qf",  "phrase_sw",
            "pf3", "phrase_sw^10",
            "fl",  "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s2']"); 

    assertQ("ps not working for pf2",
        req("q",   "bar foo", 
            "qf",  "phrase_sw",
            "pf2", "phrase_sw^10",
            "ps",  "2",
            "fl",  "score,*",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s0']"); 

    assertQ("ps not working for pf3",
        req("q",   "a bar foo", 
            "qf",  "phrase_sw",
            "pf3", "phrase_sw^10",
            "ps",  "3",
            "fl",  "score,*",
            "debugQuery",  "true",
            "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s1']"); 
    
    assertQ("ps/ps2/ps3 with default slop overrides not working",
        req("q", "zzzz xxxx cccc vvvv",
            "qf", "phrase_sw",
            "pf", "phrase_sw~1^10 phrase_sw~2^20 phrase_sw^30",
            "pf2", "phrase_sw~2^22 phrase_sw^33",
            "pf3", "phrase_sw~2^222 phrase_sw^333",
            "ps", "3",
            "defType", "edismax",
            "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx cccc vvvv\"~1)^10.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx cccc vvvv\"~2)^20.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx cccc vvvv\"~3)^30.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx\"~2)^22.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"xxxx cccc\"~2)^22.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"cccc vvvv\"~2)^22.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx\"~3)^33.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"xxxx cccc\"~3)^33.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"cccc vvvv\"~3)^33.0')]",        
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx cccc\"~2)^222.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"xxxx cccc vvvv\"~2)^222.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx cccc\"~3)^333.0')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"xxxx cccc vvvv\"~3)^333.0')]"
     );

    assertQ(
        "ps2 not working",
        req("q", "bar foo", "qf", "phrase_sw", "pf2", "phrase_sw^10", "ps2",
            "2", "fl", "score,*", "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s0']");
    
    assertQ(
        "Specifying slop in pf2 param not working",
        req("q", "bar foo", "qf", "phrase_sw", "pf2", "phrase_sw~2^10", 
            "fl", "score,*", "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s0']");
    
    assertQ(
        "Slop in ps2 parameter should override ps",
        req("q", "bar foo", "qf", "phrase_sw", "pf2", "phrase_sw^10", "ps",
            "0", "ps2", "2", "fl", "score,*", "defType",
            "edismax"), "//doc[1]/str[@name='id'][.='s0']");

    assertQ(
        "ps3 not working",
        req("q", "a bar foo", "qf", "phrase_sw", "pf3", "phrase_sw^10", "ps3",
            "3", "fl", "score,*", "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s1']");
    
    assertQ(
        "Specifying slop in pf3 param not working",
        req("q", "a bar foo", "qf", "phrase_sw", "pf3", "phrase_sw~3^10",
            "fl", "score,*", "defType", "edismax"),
        "//doc[1]/str[@name='id'][.='s1']");
   
    assertQ("ps2 should not override slop specified inline in pf2",
        req("q", "zzzz xxxx cccc vvvv",
            "qf", "phrase_sw",
            "pf2", "phrase_sw~2^22",
            "ps2", "4",
            "defType", "edismax",
            "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"zzzz xxxx\"~2)^22.0')]"
     );

    assertQ("phrase field queries spanning multiple fields should be within their own dismax queries",
        req("q", "aaaa bbbb cccc",
            "qf", "phrase_sw phrase1_sw",
            "pf2", "phrase_sw phrase1_sw",
            "pf3", "phrase_sw phrase1_sw",
            "defType", "edismax",
            "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"aaaa bbbb\" | phrase1_sw:\"aaaa bbbb\")')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"bbbb cccc\" | phrase1_sw:\"bbbb cccc\")')]",
        "//str[@name='parsedquery'][contains(.,'(phrase_sw:\"aaaa bbbb cccc\" | phrase1_sw:\"aaaa bbbb cccc\")')]"
    );
  }

  /**
   * verify that all reserved characters are properly escaped when being set in
   * {@link org.apache.solr.search.ExtendedDismaxQParser.Clause#val}.
   *
   * @see ExtendedDismaxQParser#splitIntoClauses(String, boolean)
   */
  @Test
  public void testEscapingOfReservedCharacters() throws Exception {
    // create a document that contains all reserved characters
    String allReservedCharacters = "!():^[]{}~*?\"+-\\|&/";

    assertU(adoc("id", "reservedChars",
                 "name", allReservedCharacters,
                 "cat_s", "foo/"));
    assertU(commit());

    // the backslash needs to be manually escaped (the query parser sees the raw backslash as an escape the subsequent
    // character)
    String query = allReservedCharacters.replace("\\", "\\\\");

    // query for all those reserved characters. This will fail to parse in the initial parse, meaning that the escaped
    // query will then be used
    assertQ("Escaping reserved characters",
        req("q", query,
            "qf", "name",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    
    // Query string field 'cat_s' for special char / - causes SyntaxError without patch SOLR-3467
    assertQ("Escaping string with reserved / character",
        req("q", "foo/",
            "qf", "cat_s",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    
    assertQ(
        "Might be double-escaping a client-escaped colon", 
        req("q", "text_sw:(theos OR thistokenhasa\\:preescapedcolon OR theou)", "defType", "edismax", "qf", "id"),
        "*[count(//doc)=3]");
    assertQ(
        "Might be double-escaping a client-escaped colon", 
        req("q", "text_sw:(theos OR thistokenhasa\\:preescapedcolon OR theou)", "defType", "edismax", "qf", "text"),
        "*[count(//doc)=3]");    
    
  }
  
  /**
   * SOLR-3589: Edismax parser does not honor mm parameter if analyzer splits a token
   */
  public void testCJK() throws Exception {
    assertQ("test cjk (disjunction)",
        req("q", "大亚湾",
            "qf", "standardtok",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=3]");
    assertQ("test cjk (minShouldMatch)",
        req("q", "大亚湾",
            "qf", "standardtok",
            "mm", "67%",
            "defType", "edismax")
        , "*[count(//doc)=2]");
    assertQ("test cjk (conjunction)",
        req("q", "大亚湾",
            "qf", "standardtok",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
  }
  
  /** 
   * test that minShouldMatch works with aliasing
   * for implicit boolean queries
   */
  public void testCJKAliasing() throws Exception {
    // single field
    assertQ("test cjk (aliasing+disjunction)",
        req("q", "myalias:大亚湾",
            "f.myalias.qf", "standardtok",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=3]");
    assertQ("test cjk (aliasing+minShouldMatch)",
        req("q", "myalias:大亚湾",
            "f.myalias.qf", "standardtok",
            "mm", "67%",
            "defType", "edismax")
        , "*[count(//doc)=2]");
    assertQ("test cjk (aliasing+conjunction)",
        req("q", "myalias:大亚湾",
            "f.myalias.qf", "standardtok",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    // multifield
    assertQ("test cjk (aliasing+disjunction)",
        req("q", "myalias:大亚湾",
            "f.myalias.qf", "standardtok HTMLstandardtok",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=6]");
    assertQ("test cjk (aliasing+minShouldMatch)",
        req("q", "myalias:大亚湾",
            "f.myalias.qf", "standardtok HTMLstandardtok",
            "mm", "67%",
            "defType", "edismax")
        , "*[count(//doc)=4]");
    assertQ("test cjk (aliasing+conjunction)",
        req("q", "myalias:大亚湾",
            "f.myalias.qf", "standardtok HTMLstandardtok",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=2]");
  }
  
  /** Test that we apply boosts correctly */
  public void testCJKBoosts() throws Exception {
    assertQ("test cjk (disjunction)",
        req("q", "大亚湾",
            "qf", "standardtok^2 HTMLstandardtok",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=6]", "//result/doc[1]/str[@name='id'][.='57']");
    assertQ("test cjk (minShouldMatch)",
        req("q", "大亚湾",
            "qf", "standardtok^2 HTMLstandardtok",
            "mm", "67%",
            "defType", "edismax")
        , "*[count(//doc)=4]", "//result/doc[1]/str[@name='id'][.='57']");
    assertQ("test cjk (conjunction)",
        req("q", "大亚湾",
            "qf", "standardtok^2 HTMLstandardtok",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='57']");
    
    // now boost the other field
    assertQ("test cjk (disjunction)",
        req("q", "大亚湾",
            "qf", "standardtok HTMLstandardtok^2",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=6]", "//result/doc[1]/str[@name='id'][.='60']");
    assertQ("test cjk (minShouldMatch)",
        req("q", "大亚湾",
            "qf", "standardtok HTMLstandardtok^2",
            "mm", "67%",
            "defType", "edismax")
        , "*[count(//doc)=4]", "//result/doc[1]/str[@name='id'][.='60']");
    assertQ("test cjk (conjunction)",
        req("q", "大亚湾",
            "qf", "standardtok HTMLstandardtok^2",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=2]", "//result/doc[1]/str[@name='id'][.='60']");
  }
  
  /** always apply minShouldMatch to the inner booleanqueries
   *  created from whitespace, as these are never structured lucene queries
   *  but only come from unstructured text */
  public void testCJKStructured() throws Exception {
    assertQ("test cjk (disjunction)",
        req("q", "大亚湾 OR bogus",
            "qf", "standardtok",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=3]");
    assertQ("test cjk (minShouldMatch)",
        req("q", "大亚湾 OR bogus",
            "qf", "standardtok",
            "mm", "67%",
            "defType", "edismax")
        , "*[count(//doc)=2]");
    assertQ("test cjk (conjunction)",
        req("q", "大亚湾 OR bogus", // +(((((standardtok:大 standardtok:亚 standardtok:湾)~3)) (standardtok:bogus))~2)
            "qf", "standardtok",
            "mm", "100%",
            "defType", "edismax")
        , "//*[@numFound='0']");
  }
  
  /**
   * Test that we don't apply minShouldMatch to the inner boolean queries
   * when there are synonyms (these are indicated by coordination factor)
   */
  public void testSynonyms() throws Exception {
    // document only contains baraaa, but should still match.
    assertQ("test synonyms",
        req("q", "fooaaa",
            "qf", "text_sw",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
  }

  /**
   * Test that the default operator and MM are interacting appropriately when both provided
   */
  public void testDefaultOperatorWithMm() throws Exception {
    // Text we are searching
    // "line up and fly directly at the enemy death cannons, clogging them with wreckage!"
    assertQ("test default operator with mm (AND + 0% => 0 hits)",
        req("q", "(line notfound) OR notfound",
            "qf", "text",
            "q.op", "AND",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=0]");
    assertQ("test default operator with mm (OR + 0% => 1 hit)",
        req("q", "line notfound OR notfound",
            "qf", "text",
            "q.op", "OR",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    assertQ("test default operator with mm (OR + 100% => 0 hits)",
        req("q", "line notfound OR notfound",
            "qf", "text",
            "q.op", "OR",
            "mm", "100%",
            "defType", "edismax")
        , "*[count(//doc)=0]");
    assertQ("test default operator with mm (OR + 35% => 1 hit)",
        req("q", "line notfound notfound2 OR notfound",
            "qf", "text",
            "q.op", "OR",
            "mm", "35%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    assertQ("test default operator with mm (OR + 75% => 0 hits)",
        req("q", "line notfound notfound2 OR notfound3",
            "qf", "text",
            "q.op", "OR",
            "mm", "75%",
            "defType", "edismax")
        , "*[count(//doc)=0]");
    assertQ("test default operator with mm (AND + 0% => 1 hit)",
        req("q", "(line enemy) OR notfound",
            "qf", "text",
            "q.op", "AND",
            "mm", "0%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    assertQ("test default operator with mm (AND + 50% => 1 hit)",
        req("q", "(line enemy) OR (line notfound) OR (death cannons) OR (death notfound)",
            "qf", "text",
            "q.op", "AND",
            "mm", "50%",
            "defType", "edismax")
        , "*[count(//doc)=1]");
    assertQ("test default operator with mm (AND + 75% => 0 hits)",
        req("q", "(line enemy) OR (line notfound) OR (death cannons) OR (death notfound)",
            "qf", "text",
            "q.op", "AND",
            "mm", "75%",
            "defType", "edismax")
        , "*[count(//doc)=0]");
  }
  
  /**
   * Test that minShouldMatch applies to Optional terms only
   */
  public void testMinShouldMatchOptional() throws Exception {
    for (String sow : Arrays.asList("true", "false")) {
      assertQ("test minShouldMatch (top level optional terms only)",
          req("q", "stocks oil gold", // +(((text_sw:stock) (text_sw:oil) (text_sw:gold))~1)
              "qf", "text_sw",
              "mm", "50%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");

      assertQ("test minShouldMatch (top level optional terms only and sow=false)",
          req("q", "stocks oil gold", // +(((text_sw:stock) (text_sw:oil) (text_sw:gold))~1)
              "qf", "text_sw",
              "mm", "50%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");

      assertQ("test minShouldMatch (top level optional and negative terms mm=50%)",
          req("q", "stocks oil gold -stockade", // +(((text_sw:stock) (text_sw:oil) (text_sw:gold) -(text_sw:stockad))~1)
              "qf", "text_sw",
              "mm", "50%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");

      assertQ("test minShouldMatch (top level optional and negative terms mm=100%)",
          req("q", "stocks gold -stockade", // +(((text_sw:stock) (text_sw:oil) (text_sw:gold) -(text_sw:stockad))~2)
              "qf", "text_sw",
              "mm", "100%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");

      assertQ("test minShouldMatch (top level required terms only)",
          req("q", "stocks AND oil", // +(+(text_sw:stock) +(text_sw:oil))
              "qf", "text_sw",
              "mm", "50%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");

      assertQ("test minShouldMatch (top level optional and required terms)",
          req("q", "oil gold +stocks", // +(((text_sw:oil) (text_sw:gold) +(text_sw:stock))~1)
              "qf", "text_sw",
              "mm", "50%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");

      assertQ("test minShouldMatch (top level optional with explicit OR and parens)",
          req("q", "(snake OR stocks) oil",
              "qf", "text_sw",
              "mm", "100%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=2]");

      // The results for these two appear odd, but are correct as per BooleanQuery processing.
      // See: http://searchhub.org/2011/12/28/why-not-and-or-and-not/
      // Non-parenthesis OR/AND precedence is not true to abstract boolean logic in solr when q.op = AND
      //   and when q.op = OR all three clauses are top-level and optional so mm takes over
      assertQ("test minShouldMatch (top level optional with explicit OR without parens)",
          req("q", "snake OR stocks oil",
              "qf", "text_sw",
              "q.op", "OR",
              "mm", "100%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=0]");
      assertQ("test minShouldMatch (top level optional with explicit OR without parens)",
          req("q", "snake OR stocks oil",
              "qf", "text_sw",
              "q.op", "AND",
              "mm", "100%",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=0]");

      // SOLR-9174
      assertQ("test minShouldMatch=1<-1 with explicit OR, one impossible clause, and no explicit q.op",
          req("q", "barbie OR (hair AND nonexistentword)",
              "qf", "text_sw",
              "mm", "1<-1",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");
    }
  }

  /* SOLR-8812 */
  @Test
  public void testDefaultMM() throws Exception {
    // Ensure MM is off when explicit operators (+/-/OR/NOT) are used and no explicit mm spec is specified.
    for (String sow : Arrays.asList("true", "false")) {
      assertQ("Explicit OR in query with no explicit mm and q.op=AND => mm = 0%",
          req("q", "oil OR stocks",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");
      assertQ("Explicit 'or' in query with lowercaseOperators=true, no explicit mm and q.op=AND => mm = 0%",
          req("q", "oil or stocks",
              "qf", "text_sw",
              "q.op", "AND",
              "lowercaseOperators", "true",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");
      assertQ("Explicit OR in query with no explicit mm and no explicit q.op => mm = 0%",
          req("q", "oil OR stocks",
              "qf", "text_sw",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");
      assertQ("No operator in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "oil stocks",
              "qf", "text_sw",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");
      assertQ("No operator in query with no explicit mm and q.op=AND => mm = 100%",
          req("q", "oil stocks",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");
      assertQ("No operator in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "oil stocks",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=4]");

      assertQ("Explicit '-' operator in query with no explicit mm and no explicit q.op => mm = 0%",
          req("q", "hair ties -barbie",
              "qf", "text_sw",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");
      assertQ("Explicit NOT in query with no explicit mm and no explicit q.op => mm = 0%",
          req("q", "hair ties NOT barbie",
              "qf", "text_sw",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");

      assertQ("Explicit '-' operator in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "hair ties -barbie",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");
      assertQ("Explicit NOT in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "hair ties NOT barbie",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");

      assertQ("Explicit '-' operator in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "hair AND ties -barbie",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");
      assertQ("Explicit NOT in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "hair AND ties -barbie",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");

      assertQ("No explicit non-AND operator in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "hair AND ties barbie",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=2]");
      assertQ("No explicit non-AND operator in query with no explicit mm and q.op=AND => mm = 100%",
          req("q", "hair AND ties barbie",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");
      assertQ("No explicit non-AND operator in query with no explicit mm and no explicit q.op => mm = 0%",
          req("q", "hair AND ties barbie",
              "qf", "text_sw",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=2]");
      assertQ("No explicit non-AND operator in query with no explicit mm and no explicit q.op => mm = 0%",
          req("q", "hair and ties barbie",
              "qf", "text_sw",
              "lowercaseOperators", "true",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=2]");

      assertQ("Explicit '-' operator in query with no explicit mm and q.op=AND => mm = 100%",
          req("q", "hair ties -barbie",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");
      assertQ("Explicit NOT in query with no explicit mm and q.op=AND => mm = 100%",
          req("q", "hair ties NOT barbie",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");

      assertQ("Explicit OR in query with no explicit mm and q.op=AND => mm = 0%",
          req("q", "hair OR ties barbie",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=3]");
      assertQ("Explicit OR in query with no explicit mm and q.op=OR => mm = 0%",
          req("q", "hair OR ties barbie",
              "qf", "text_sw",
              "q.op", "OR",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=6]");
      assertQ("Explicit OR in query with no explicit mm and no explicit q.op => mm = 0%",
          req("q", "hair OR ties barbie",
              "qf", "text_sw",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=6]");

      assertQ("Explicit '+' operator in query with no explicit mm and q.op=AND => mm = 0%",
          req("q", "hair ties +barbie",
              "qf", "text_sw",
              "q.op", "AND",
              "sow", sow,
              "defType", "edismax")
          , "*[count(//doc)=1]");
    }
  }

  public void testEdismaxSimpleExtension() throws SyntaxError {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "foo bar");
    params.set("qf", "subject title^5");
    params.set("qf_fr", "subject_fr title_fr^5");
    params.set("qf_en", "subject_en title_en^5");
    params.set("qf_es", "subject_es title_es^5");
    
    MultilanguageQueryParser parser = new MultilanguageQueryParser("foo bar", new ModifiableSolrParams(), params, req(params));
    Query query = parser.parse();
    assertNotNull(query);
    assertTrue(containsClause(query, "title", "foo", 5, false));
    assertTrue(containsClause(query, "title", "bar", 5, false));
    assertTrue(containsClause(query, "subject", "foo", 1, false));
    assertTrue(containsClause(query, "subject", "bar", 1, false));
    
    params.set("language", "es");
    parser = new MultilanguageQueryParser("foo bar", new ModifiableSolrParams(), params, req(params));
    query = parser.parse();
    assertNotNull(query);
    assertTrue(containsClause(query, "title_es", "foo", 5, false));
    assertTrue(containsClause(query, "title_es", "bar", 5, false));
    assertTrue(containsClause(query, "subject_es", "foo", 1, false));
    assertTrue(containsClause(query, "subject_es", "bar", 1, false));
    
    FuzzyDismaxQParser parser2 = new FuzzyDismaxQParser("foo bar absence", new ModifiableSolrParams(), params, req(params));
    query = parser2.parse();
    assertNotNull(query);
    assertTrue(containsClause(query, "title", "foo", 5, false));
    assertTrue(containsClause(query, "title", "bar", 5, false));
    assertTrue(containsClause(query, "title", "absence", 5, true));
    
  }

  @Test
  public void testSplitOnWhitespace_Basic() throws Exception {
    // The "text_sw" field has synonyms loaded from synonyms.txt

    // retrieve the single document containing literal "wifi"
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wifi", "sow","true")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );

    // trigger the "wi fi => wifi" synonym
    assertJQ(req("qf", "text_sw title", "defType","edismax", "q","wi fi", "sow","false")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );
    assertJQ(req("qf", "text_sw title", "defType","edismax", "q","wi fi", "sow","true")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );

    assertJQ(req("qf","text_sw title", "q","{!edismax sow=false}wi fi")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );
    assertJQ(req("qf", "text_sw title", "q","{!edismax sow=true}wi fi")
        , "/response/numFound==0"
    );
    assertJQ(req("qf", "text_sw title", "q", "{!edismax}wi fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );

    assertQ(req("qf", "name title", 
                "q", "barking curds of stigma",
                "defType", "edismax",
                "sow", "false",
                "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:barking | title:barking))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:curds | title:curds))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:of | title:of))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:stigma | title:stigma))')]"
    );
    assertQ(req("qf", "name title",
        "q", "barking curds of stigma",
        "defType", "edismax",
        "sow", "true",
        "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:barking | title:barking))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:curds | title:curds))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:of | title:of))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:stigma | title:stigma))')]"
    );
    assertQ(req("qf", "name title",
        "q", "barking curds of stigma",
        "defType", "edismax",
        "debugQuery", "true"), // Default sow=false
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:barking | title:barking))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:curds | title:curds))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:of | title:of))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((name:stigma | title:stigma))')]"
    );
  }
  
  public void testSplitOnWhitespace_Different_Field_Analysis() throws Exception {
    // When the *structure* of produced queries is different in each field, 
    // sow=true produces boolean-of-dismax query structure,
    // and sow=false produces dismax-of-boolean query structure.
    assertQ(req("qf", "text_sw title",
        "q", "olive the other",
        "defType", "edismax",
        "sow", "true",
        "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((text_sw:oliv | title:olive))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((title:the))')]",
        "//str[@name='parsedquery'][contains(.,'DisjunctionMaxQuery((text_sw:other | title:other))')]"
    );
    assertQ(req("qf", "text_sw title",
        "q", "olive the other",
        "defType", "edismax",
        "sow", "false",
        "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'+DisjunctionMaxQuery(((text_sw:oliv text_sw:other) | (title:olive title:the title:other)))')]"
    );

    // When field's analysis produce different query structures, mm processing is always done on the boolean query.
    // sow=true produces (boolean-of-dismax)~<mm> query structure,
    // and sow=false produces dismax-of-(boolean)~<mm> query structure.
    assertQ(req("qf", "text_sw title",
        "q", "olive the other",
        "defType", "edismax",
        "sow", "true",
        "mm", "100%",
        "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'+(DisjunctionMaxQuery((text_sw:oliv | title:olive)) DisjunctionMaxQuery((title:the)) DisjunctionMaxQuery((text_sw:other | title:other)))~3')]"
    );
    assertQ(req("qf", "text_sw title",
        "q", "olive the other",
        "defType", "edismax",
        "sow", "false",
        "mm", "100%",
        "debugQuery", "true"),
        "//str[@name='parsedquery'][contains(.,'+DisjunctionMaxQuery((((text_sw:oliv text_sw:other)~2) | ((title:olive title:the title:other)~3)))')]"
    );


    // When the *structure* of produced queries is the same in each field, 
    // sow=false/true produce the same boolean-of-dismax query structure 
    for (String sow : Arrays.asList("true", "false")) {
      assertQ(req("qf", "text_sw title",
          "q", "olive blah other",
          "defType", "edismax",
          "sow", sow,
          "debugQuery", "true"),
          "//str[@name='parsedquery'][contains(.,'"
              + "+(DisjunctionMaxQuery((text_sw:oliv | title:olive))"
              + " DisjunctionMaxQuery((text_sw:blah | title:blah))"
              + " DisjunctionMaxQuery((text_sw:other | title:other)))')]"
      );
    }
  }

  public void testOperatorsAndMultiWordSynonyms() throws Exception {
    // The "text_sw" field has synonyms loaded from synonyms.txt

    // retrieve the single document containing literal "wifi"
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wifi", "sow","true")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );
    // trigger the "wi fi => wifi" synonym
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi", "sow","false")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='72'"
    );

    assertJQ(req("qf","text_sw title", "defType","edismax", "q","+wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","-wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","!wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi* fi", "sow","false")
        , "/response/numFound==1"    // matches because wi* matches "wifi"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","w? fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi~1 fi", "sow","false")
        , "/response/numFound==4"   // matches because wi~1 matches ti (stemmed "ties")
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi^2 fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi^=2 fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi +fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi -fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi !fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi*", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi?", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi~1", "sow","false")
        , "/response/numFound==4"   // matches because fi~1 matches ti (stemmed "ties")
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi^2", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi^=2", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","text_sw:wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi text_sw:fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NOT wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi NOT fi", "sow","false")
        , "/response/numFound==0"
    );

    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi AND ATM", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","ATM AND wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi && ATM", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","ATM && wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi) AND ATM", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","ATM AND (wi fi)", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi) && ATM", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","ATM && (wi fi)", "sow","false")
        , "/response/numFound==1"
    );

    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi OR NotThereAtAll", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NotThereAtAll OR wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi || NotThereAtAll", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NotThereAtAll || wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi) OR NotThereAtAll", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NotThereAtAll OR (wi fi)", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi) || NotThereAtAll", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NotThereAtAll || (wi fi)", "sow","false")
        , "/response/numFound==1"
    );

    assertJQ(req("qf","text_sw title", "defType","edismax", "q","\"wi\" fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi \"fi\"", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi) fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi (fi)", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","/wi/ fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi /fi/", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi)", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","+(wi fi)", "sow","false")
        , "/response/numFound==1"
    );

    Map all = (Map) Utils.fromJSONString(h.query(req("q", "*:*", "rows", "0", "wt", "json")));
    int totalDocs = Integer.parseInt(((Map)all.get("response")).get("numFound").toString());
    int allDocsExceptOne = totalDocs - 1;

    assertJQ(req("qf","text_sw title", "defType","edismax", "q","-(wi fi)", "sow","false")
        , "/response/numFound==" + allDocsExceptOne  // one doc contains "wifi" in the text_sw field
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","!(wi fi)", "sow","false")
        , "/response/numFound==" + allDocsExceptOne  // one doc contains "wifi" in the text_sw field
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NOT (wi fi)", "sow","false")
        , "/response/numFound==" + allDocsExceptOne  // one doc contains "wifi" in the text_sw field
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi)^2", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","(wi fi)^=2", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","text_sw:(wi fi)", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","+ATM wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","-ATM wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","-NotThereAtAll wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","!ATM wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","!NotThereAtAll wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NOT ATM wi fi", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","NOT NotThereAtAll wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","AT* wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","AT? wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","\"ATM\" wi fi", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi +ATM", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi -ATM", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi -NotThereAtAll", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi !ATM", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi !NotThereAtAll", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi NOT ATM", "sow","false")
        , "/response/numFound==0"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi NOT NotThereAtAll", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi AT*", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi AT?", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","wi fi \"ATM\"", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","\"wi fi\"~2", "sow","false")
        , "/response/numFound==1"
    );
    assertJQ(req("qf","text_sw title", "defType","edismax", "q","text_sw:\"wi fi\"", "sow","false")
        , "/response/numFound==1"
    );
  }

  public void testAutoGeneratePhraseQueries() throws Exception {
    ModifiableSolrParams noSowParams = new ModifiableSolrParams();
    noSowParams.add("df", "text");
    ModifiableSolrParams sowFalseParams = new ModifiableSolrParams();
    sowFalseParams.add("sow", "false");
    sowFalseParams.add("df", "text");
    ModifiableSolrParams sowTrueParams = new ModifiableSolrParams();
    sowTrueParams.add("sow", "true");
    sowTrueParams.add("df", "text");

    // From synonyms.txt:
    //
    //     crow blackbird, grackle

    for (SolrParams params : Arrays.asList(noSowParams, sowFalseParams)) {
      try (SolrQueryRequest req = req(params)) {
        QParser qParser = QParser.getParser("text:grackle", "edismax", req); // "text" has autoGeneratePhraseQueries="true"
        Query q = qParser.getQuery();
        assertEquals("+((text:\"crow blackbird\" text:grackl))", q.toString());
      }
    }
    try (SolrQueryRequest req = req(sowTrueParams)) {
      QParser qParser = QParser.getParser("text:grackle", "edismax", req);
      Query q = qParser.getQuery();
      assertEquals("+spanOr([spanNear([text:crow, text:blackbird], 0, true), text:grackl])", q.toString());
    }
    for (SolrParams params : Arrays.asList(noSowParams, sowTrueParams, sowFalseParams)) {
      try (SolrQueryRequest req = req(params)) {
        QParser qParser = QParser.getParser("text_sw:grackle", "edismax", req); // "text_sw" doesn't specify autoGeneratePhraseQueries => default false
        Query q = qParser.getQuery();
        assertEquals("+(((+text_sw:crow +text_sw:blackbird) text_sw:grackl))", q.toString());
      }
    }

    Stream.of(noSowParams, sowTrueParams, sowFalseParams).forEach(p->p.add("qf", "text text_sw"));

    for (SolrParams params : Arrays.asList(noSowParams, sowFalseParams)) {
      try (SolrQueryRequest req = req(params)) {
        QParser qParser = QParser.getParser("grackle", "edismax", req);
        Query q = qParser.getQuery();
        assertEquals("+(((text:\"crow blackbird\" text:grackl))"
                + " | (((+text_sw:crow +text_sw:blackbird) text_sw:grackl)))",
            q.toString());

        qParser = QParser.getParser("grackle wi fi", "edismax", req);
        q = qParser.getQuery();
        assertEquals("+(((text:\"crow blackbird\" text:grackl) text:wifi)"
                + " | (((+text_sw:crow +text_sw:blackbird) text_sw:grackl) text_sw:wifi))",
            q.toString());
      }
    }
    
    try (SolrQueryRequest req = req(sowTrueParams)) {
      QParser qParser = QParser.getParser("grackle", "edismax", req);
      Query q = qParser.getQuery();
      assertEquals("+(spanOr([spanNear([text:crow, text:blackbird], 0, true), text:grackl])"
              + " | (((+text_sw:crow +text_sw:blackbird) text_sw:grackl)))",
          q.toString());

      qParser = QParser.getParser("grackle wi fi", "edismax", req);
      q = qParser.getQuery();
      assertEquals("+((spanOr([spanNear([text:crow, text:blackbird], 0, true), text:grackl])"
              + " | (((+text_sw:crow +text_sw:blackbird) text_sw:grackl))) (text:wi | text_sw:wi) (text:fi | text_sw:fi))",
          q.toString());
    }
  }
  
  public void testSowFalseWithBoost() throws Exception {
    try (SolrQueryRequest req = req("sow", "false", "qf", "subject title")) {
      QParser qParser = QParser.getParser("one two", "edismax", req);
      Query q = qParser.getQuery();
      assertEquals("+((title:one | subject:on) (title:two | subject:two))", q.toString());
    }
    try (SolrQueryRequest req = req("sow", "false", "qf", "subject title^5")) {
      QParser qParser = QParser.getParser("one two", "edismax", req);
      Query q = qParser.getQuery();
      assertEquals("+(((title:one)^5.0 | subject:on) ((title:two)^5.0 | subject:two))", q.toString());
    }
    try (SolrQueryRequest req = req("sow", "false", "qf", "subject^3 title")) {
      QParser qParser = QParser.getParser("one two", "edismax", req);
      Query q = qParser.getQuery();
      assertEquals("+((title:one | (subject:on)^3.0) (title:two | (subject:two)^3.0))", q.toString());
    }
    try (SolrQueryRequest req = req("sow", "false", "qf", "subject^10 title^20")) {
      QParser qParser = QParser.getParser("one two", "edismax", req);
      Query q = qParser.getQuery();
      assertEquals("+(((title:one)^20.0 | (subject:on)^10.0) ((title:two)^20.0 | (subject:two)^10.0))", q.toString());
    }
  }


  private boolean containsClause(Query query, String field, String value,
                                 int boost, boolean fuzzy) {

    float queryBoost = 1f;
    if (query instanceof BoostQuery) {
      BoostQuery bq = (BoostQuery) query;
      query = bq.getQuery();
      queryBoost = bq.getBoost();
    }

    if(query instanceof BooleanQuery) {
      return containsClause((BooleanQuery)query, field, value, boost, fuzzy);
    }
    if(query instanceof DisjunctionMaxQuery) {
      return containsClause((DisjunctionMaxQuery)query, field, value, boost, fuzzy);
    }
    if (boost != queryBoost) {
      return false;
    }
    if(query instanceof TermQuery && !fuzzy) {
      return containsClause((TermQuery)query, field, value);
    }
    if(query instanceof FuzzyQuery && fuzzy) {
      return containsClause((FuzzyQuery)query, field, value);
    }
    return false;
  }

  private boolean containsClause(FuzzyQuery query, String field, String value) {
    if(query.getTerm().field().equals(field) && 
       query.getTerm().bytes().utf8ToString().equals(value)) {
      return true;
    }
    return false;
  }

  private boolean containsClause(BooleanQuery query, String field, String value, int boost, boolean fuzzy) {
    for(BooleanClause clause:query) {
      if(containsClause(clause.getQuery(), field, value, boost, fuzzy)) {
        return true;
      }
    }
    return false;
  }
  
  private boolean containsClause(TermQuery query, String field, String value) {
    if(query.getTerm().field().equals(field) && 
       query.getTerm().bytes().utf8ToString().equals(value)) {
      return true;
    }
    return false;
  }
  
  private boolean containsClause(DisjunctionMaxQuery query, String field, String value, int boost, boolean fuzzy) {
    for(Query disjunct:query.getDisjuncts()) {
      if(containsClause(disjunct, field, value, boost, fuzzy)) {
        return true;
      }
    }
    return false;
  }

  static class MultilanguageQueryParser extends ExtendedDismaxQParser {

    public MultilanguageQueryParser(String qstr, SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }
    
    @Override
    protected ExtendedDismaxConfiguration createConfiguration(String qstr,
        SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      return new MultilanguageDismaxConfiguration(localParams, params, req);
    }
    
    class MultilanguageDismaxConfiguration extends ExtendedDismaxConfiguration {

      public MultilanguageDismaxConfiguration(SolrParams localParams,
          SolrParams params, SolrQueryRequest req) {
        super(localParams, params, req);
        String language = params.get("language");
        if(language != null) {
          super.queryFields = SolrPluginUtils.parseFieldBoosts(solrParams.getParams("qf_" + language)); 
        }
      }
      
    }
    
  }



  static class FuzzyDismaxQParser extends ExtendedDismaxQParser {
    
    private static final float MIN_SIMILARITY = 0.75F;

    public FuzzyDismaxQParser(String qstr, SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }
    
    @Override
    protected ExtendedSolrQueryParser createEdismaxQueryParser(QParser qParser,
        String field) {
      return new FuzzyQueryParser(qParser, field);
    }
    
    class FuzzyQueryParser extends ExtendedSolrQueryParser{
      
      private Set<String> frequentlyMisspelledWords;

      public FuzzyQueryParser(QParser parser, String defaultField) {
        super(parser, defaultField);
        frequentlyMisspelledWords = new HashSet<>();
        frequentlyMisspelledWords.add("absence");
        frequentlyMisspelledWords.add("absenc");
      }
      
      @Override
      protected Query getFieldQuery(String field,
          String val, boolean quoted, boolean raw) throws SyntaxError {
        if(frequentlyMisspelledWords.contains(val)) {
          return getFuzzyQuery(field, val, MIN_SIMILARITY);
        }
        return super.getFieldQuery(field, val, quoted, raw);
      }
      
      /** 
       * Handle multi-term queries by repacking boolean queries with frequently misspelled term
       * queries rewritten as fuzzy queries.
       **/
      @Override
      protected Query newFieldQuery(Analyzer analyzer, String field, String queryText,
                                    boolean quoted, boolean fieldAutoGenPhraseQueries,
                                    boolean fieldEnableGraphQueries, SynonymQueryStyle synonymQueryStyle)
          throws SyntaxError {
        Query q = super.newFieldQuery
            (analyzer, field, queryText, quoted, fieldAutoGenPhraseQueries, fieldEnableGraphQueries, synonymQueryStyle);
        if (q instanceof BooleanQuery) {
          boolean rewrittenSubQ = false; // dirty flag: rebuild the repacked query?
          BooleanQuery.Builder builder = newBooleanQuery();
          for (BooleanClause clause : ((BooleanQuery)q).clauses()) {
            Query subQ = clause.getQuery();
            if (subQ instanceof TermQuery) {
              Term subTerm = ((TermQuery)subQ).getTerm(); 
              if (frequentlyMisspelledWords.contains(subTerm.text())) {
                rewrittenSubQ = true;
                Query fuzzySubQ = newFuzzyQuery(subTerm, MIN_SIMILARITY, getFuzzyPrefixLength());
                clause = newBooleanClause(fuzzySubQ, clause.getOccur());
              } 
            } 
            builder.add(clause);
          }
          if (rewrittenSubQ) {
            builder.setMinimumNumberShouldMatch(((BooleanQuery)q).getMinimumNumberShouldMatch());
            q = builder.build();
          }
        }
        return q;
      }
    }
  }

  @Test
  public void testShingleQueries() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("sow", "false");
    params.add("defType", "edismax");

    try (SolrQueryRequest req = req(params)) {
      QParser qParser = QParser.getParser("shingle23:(A B C)", req);
      Query q = qParser.getQuery();
      assertEquals("Synonym(shingle23:A_B shingle23:A_B_C) shingle23:B_C", q.toString());
    }

    assertJQ(req("df", "shingle23", "q", "A B C", "sow", "false")
        , "/response/numFound==1"
    );
  }

  /** SOLR-11512 */
  @Test
  public void killInfiniteRecursionParse() throws Exception {
    SolrException exception = expectThrows(SolrException.class, () -> {
      h.query(req("defType", "edismax", "q", "*", "qq", "{!edismax v=something}", "bq", "{!edismax v=$qq}"));
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertTrue(exception.getMessage().contains("Infinite Recursion detected parsing query"));
  }

  /** SOLR-5163 */
  @Test
  public void testValidateQueryFields() throws Exception {
    // field aliasing covered by test - testAliasing
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("defType", "edismax");
    params.add("df", "text");
    params.add("q", "olive AND other");
    params.add("qf", "subject^3 title");
    params.add("debugQuery", "true");

    // test valid field names
    String response = h.query(req(params));
    assertTrue(response.contains("+DisjunctionMaxQuery((title:olive | " +
        "(subject:oliv)^3.0)) +DisjunctionMaxQuery((title:other | (subject:other)^3.0))"));

    // test invalid field name
    params.set("qf", "subject^3 nosuchfield");
    SolrException exception = expectThrows(SolrException.class, () -> h.query(req(params)));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("org.apache.solr.search.SyntaxError: Query Field 'nosuchfield' is not a valid field name",
        exception.getMessage());
  }

}
