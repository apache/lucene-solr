/**
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

import java.io.IOException;

import org.apache.solr.common.SolrException;
import org.apache.solr.util.AbstractSolrTestCase;

public class TestExtendedDismaxParser extends AbstractSolrTestCase {
  @Override
  public String getSchemaFile() { return "schema12.xml"; }
  @Override
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  // public String getCoreName() { return "collection1"; }

  @Override
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
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
    assertU(commit());
  }
  @Override
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
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

   assertQ("qf defaults to defaultSearchField"
           , req( "defType", "edismax"
                 ,"q","op")
           , twor
           );
   
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","op"), twor
    );
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order op"), oner
    );
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order AND op"), oner
    );
   assertQ(req("defType", "edismax", "qf", "name title subject text",
               "q","Order and op"), oner
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
    assertQ(req("defType", "edismax", "qf", "text_sw", "stopwords","false",
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

    assertQ(req("defType","edismax", "mm","100%", "q","Terminator: 100", "qf","movies_t foo_i"),
            nor);

    assertQ(req("defType","edismax", "mm","100%", "q","Terminator: 8", "qf","movies_t foo_i"),
            oner);

    assertQ(req("defType","edismax", "mm","0", "q","movies_t:Terminator 100", "qf","movies_t foo_i"),
            twor);
    
    // special psuedo-fields like _query_ and _val_

    // special fields (and real field id) should be included by default
    assertQ(req("defType", "edismax", 
                "mm", "100%",
                "fq", "id:51",
                "q", "_query_:\"{!geofilt d=20 sfield=store pt=12.34,-56.78}\""),
            oner);
    // should also work when explicitly allowed
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
                "uf", "id", // excluded by ommision
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

  public void testUserFields() {
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
    
    assertQ(req("defType","edismax", "uf","*", "q","id:42"),
        oner);
    
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
        "//str[@name='parsedquery_toString'][.='+id:42^5.0']");
    
    assertQ(req("defType","edismax", "debugQuery","true", "rows","0", "uf","*^2.0 id^5.0 -xyz", "q","name:foo"),
        "//str[@name='parsedquery_toString'][.='+name:foo^2.0']");
    
    assertQ(req("defType","edismax", "debugQuery","true", "rows","0", "uf","i*^5.0", "q","id:42"),
        "//str[@name='parsedquery_toString'][.='+id:42^5.0']");
    
    
    assertQ(req("defType","edismax", "uf","-*", "q","cannons"),
        oner);
    
    assertQ(req("defType","edismax", "uf","* -id", "q","42", "qf", "id"), oner);
    
  }
  
  public void testAliasing() throws IOException, Exception {
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
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Obnoxious", "f.myalias.qf","name^2.0 mytrait_ss^5.0"),
        nor);
    
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Obnoxious", "qf","myalias^10.0", "f.myalias.qf","name^2.0 mytrait_ss^5.0"), oner);
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Obnoxious", "qf","myalias^10.0", "f.myalias.qf","name^2.0 trait_ss^5.0"), twor);
    assertQ(req("defType","edismax", "q","Zapp Obnoxious", "qf","myalias^10.0", "f.myalias.qf","name^2.0 trait_ss^5.0", "mm", "100%"), oner);
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Obnoxious", "qf","who^10.0 where^3.0", "f.who.qf","name^2.0", "f.where.qf", "mytrait_ss^5.0"), oner);
    
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Obnoxious", "qf","myalias", "f.myalias.qf","name mytrait_ss", "uf", "myalias"), oner);
    
    assertQ(req("defType","edismax", "mm", "0", "uf","who", "q","who:(Zapp Obnoxious)", "f.who.qf", "name^2.0 trait_ss^5.0", "qf", "id"), twor);
    assertQ(req("defType","edismax", "mm", "0", "uf","* -name", "q","who:(Zapp Obnoxious)", "f.who.qf", "name^2.0 trait_ss^5.0"), twor);
    
  }
  
  public void testAliasingBoost() throws IOException, Exception {
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Pig", "qf","myalias", "f.myalias.qf","name trait_ss^0.5"), "//result/doc[1]/str[@name='id']=42", "//result/doc[2]/str[@name='id']=47");//doc 42 should score higher than 46
    assertQ(req("defType","edismax", "mm", "0", "q","Zapp Pig", "qf","myalias^100 name", "f.myalias.qf","trait_ss^0.5"), "//result/doc[1]/str[@name='id']=47", "//result/doc[2]/str[@name='id']=42");//Now the order should be inverse
  }
  
  public void testCyclicAliasing() throws IOException, Exception {
    try {
      h.query(req("defType","edismax", "mm", "0", "q","Zapp Pig", "qf","who", "f.who.qf","name","f.name.qf","who"));
      fail("Simple cyclic alising");
    } catch (SolrException e) {
      assertTrue(e.getCause().getMessage().contains("Field aliases lead to a cycle"));
    }
    
    try {
      h.query(req("defType","edismax", "mm", "0", "q","Zapp Pig", "qf","who", "f.who.qf","name","f.name.qf","myalias", "f.myalias.qf","who"));
      fail();
    } catch (SolrException e) {
      assertTrue(e.getCause().getMessage().contains("Field aliases lead to a cycle"));
    }
    
    try {
      h.query(req("defType","edismax", "mm", "0", "q","Zapp Pig", "qf","field1", "f.field1.qf","field2 field3","f.field2.qf","field4 field5", "f.field4.qf","field5", "f.field5.qf","field6", "f.field3.qf","field6"));
    } catch (SolrException e) {
      fail("This is not cyclic alising");
    }
    
    try {
      h.query(req("defType","edismax", "mm", "0", "q","Zapp Pig", "qf","field1", "f.field1.qf","field2 field3", "f.field2.qf","field4 field5", "f.field4.qf","field5", "f.field5.qf","field4"));
      fail();
    } catch (SolrException e) {
      assertTrue(e.getCause().getMessage().contains("Field aliases lead to a cycle"));
    }
    
    try {
      h.query(req("defType","edismax", "mm", "0", "q","who:(Zapp Pig)", "qf","field1", "f.who.qf","name","f.name.qf","myalias", "f.myalias.qf","who"));
      fail();
    } catch (SolrException e) {
      assertTrue(e.getCause().getMessage().contains("Field aliases lead to a cycle"));
    }
  }
  
}
