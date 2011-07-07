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

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.AbstractSolrTestCase;

public class TestQueryTypes extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() { return "schema11.xml"; }
  @Override
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  public String getCoreName() { return "basic"; }


  @Override
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  @Override
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
  }


  public void testQueryTypes() {
    assertU(adoc("id","0"));
    assertU(adoc("id","1", "v_t","Hello Dude"));
    assertU(adoc("id","2", "v_t","Hello Yonik"));
    assertU(adoc("id","3", "v_s","{!literal}"));
    assertU(adoc("id","4", "v_s","other stuff"));
    assertU(adoc("id","5", "v_f","3.14159"));
    assertU(adoc("id","6", "v_f","8983"));
    assertU(adoc("id","7", "v_f","1.5"));
    assertU(adoc("id","8", "v_ti","5"));
    assertU(adoc("id","9", "v_s","internal\"quote"));

    Object[] arr = new Object[] {
    "id",999.0
    ,"v_s","wow dude"
    ,"v_t","wow"
    ,"v_ti",-1
    ,"v_tis",-1
    ,"v_tl",-1234567891234567890L
    ,"v_tls",-1234567891234567890L
    ,"v_tf",-2.0f
    ,"v_tfs",-2.0f
    ,"v_td",-2.0
    ,"v_tds",-2.0
    ,"v_tdt","2000-05-10T01:01:01Z"
    ,"v_tdts","2002-08-26T01:01:01Z"
    };
    String[] sarr = new String[arr.length];
    for (int i=0; i<arr.length; i++) {
      sarr[i] = arr[i].toString();
    }

    assertU(adoc(sarr));
    assertU(optimize());

    // test field queries
    for (int i=0; i<arr.length; i+=2) {
      String f = arr[i].toString();
      String v = arr[i+1].toString();

      // normal lucene fielded query
      assertQ(req( "q",f+":\""+v+'"')
              ,"//result[@numFound='1']"
              ,"//*[@name='id'][.='999.0']"
              ,"//*[@name='" + f + "'][.='" + v + "']"
              );

      // field qparser
      assertQ(req( "q", "{!field f="+f+"}"+v)
              ,"//result[@numFound='1']"
              );

      // lucene range
      assertQ(req( "q", f + ":[\"" + v + "\" TO \"" + v + "\"]" )
              ,"//result[@numFound='1']"
              );
    }

    // frange and function query only work on single valued field types
    Object[] fc_vals = new Object[] {
      "id",999.0
      ,"v_s","wow dude"
      ,"v_ti",-1
      ,"v_tl",-1234567891234567890L
      ,"v_tf",-2.0f
      ,"v_td",-2.0
      ,"v_tdt","2000-05-10T01:01:01Z"
    };
    
    for (int i=0; i<fc_vals.length; i+=2) {
      String f = fc_vals[i].toString();
      String v = fc_vals[i+1].toString();
      
      // frange qparser
      assertQ(req( "q", "{!frange v="+f+" l='"+v+"' u='"+v+"'}" )
              ,"//result[@numFound='1']"
              );

       // frange as filter not cached
      assertQ(req( "q","*:*", "fq", "{!frange cache=false v="+f+" l='"+v+"' u='"+v+"'}" )
              ,"//result[@numFound='1']"
              );

       // frange as filter run after the main query
      assertQ(req( "q","*:*", "fq", "{!frange cache=false cost=100 v="+f+" l='"+v+"' u='"+v+"'}" )
              ,"//result[@numFound='1']"
              );

      // exists()
      assertQ(req( "fq","id:999", "q", "{!frange l=1 u=1}if(exists("+f+"),1,0)" )
              ,"//result[@numFound='1']"
              );

      // boolean value of non-zero values (just leave off the exists from the prev test)
      assertQ(req( "fq","id:999", "q", "{!frange l=1 u=1}if("+f+",1,0)" )
              ,"//result[@numFound='1']"
              );

      if (!"id".equals(f)) {
        assertQ(req( "fq","id:1", "q", "{!frange l=1 u=1}if(exists("+f+"),1,0)" )
            ,"//result[@numFound='0']"
        );

       // boolean value of zero/missing values (just leave off the exists from the prev test)
       assertQ(req( "fq","id:1", "q", "{!frange l=1 u=1}if("+f+",1,0)" )
            ,"//result[@numFound='0']"
        );

      }

      // function query... just make sure it doesn't throw an exception
      if ("v_s".equals(f)) continue;  // in this context, functions must be able to be interpreted as a float
      assertQ(req( "q", "+id:999 _val_:\"" + f + "\"")
              ,"//result[@numFound='1']"
              );
    }

    // Some basic tests to ensure that parsing local params is working
    assertQ("test prefix query",
            req("q","{!prefix f=v_t}hel")
            ,"//result[@numFound='2']"
            );

    assertQ("test raw query",
            req("q","{!raw f=v_t}hello")
            ,"//result[@numFound='2']"
            );

    // no analysis is done, so these should match nothing
    assertQ("test raw query",
            req("q","{!raw f=v_t}Hello")
            ,"//result[@numFound='0']"
            );
    assertQ("test raw query",
            req("q","{!raw f=v_f}1.5")
            ,"//result[@numFound='0']"
            );

    // test "term" qparser, which should only do readableToIndexed
    assertQ(
            req("q","{!term f=v_f}1.5")
            ,"//result[@numFound='1']"
            );
    
    // text fields are *not* analyzed since they may not be idempotent
    assertQ(
           req("q","{!term f=v_t}Hello")
           ,"//result[@numFound='0']"
           );
     assertQ(
           req("q","{!term f=v_t}hello")
           ,"//result[@numFound='2']"
           );


    //
    // test escapes in quoted strings
    //

    // the control... unescaped queries looking for internal"quote
    assertQ(req("q","{!raw f=v_s}internal\"quote")
            ,"//result[@numFound='1']"
            );

    // test that single quoted string needs no escape
    assertQ(req("q","{!raw f=v_s v='internal\"quote'}")
            ,"//result[@numFound='1']"
            );

    // but it's OK if the escape is done
    assertQ(req("q","{!raw f=v_s v='internal\\\"quote'}")
            ,"//result[@numFound='1']"
            );

    // test unicode escape
    assertQ(req("q","{!raw f=v_s v=\"internal\\u0022quote\"}")
            ,"//result[@numFound='1']"
            );

    // inside a quoted string, internal"quote needs to be escaped
    assertQ(req("q","{!raw f=v_s v=\"internal\\\"quote\"}")
            ,"//result[@numFound='1']"
            );

    assertQ("test custom plugin query",
            req("q","{!foo f=v_t}hello")
            ,"//result[@numFound='2']"
            );


    assertQ("test single term field query on text type",
            req("q","{!field f=v_t}HELLO")
            ,"//result[@numFound='2']"
            );

    assertQ("test single term field query on type with diff internal rep",
            req("q","{!field f=v_f}1.5")
            ,"//result[@numFound='1']"
            );    

    assertQ(
            req("q","{!field f=v_ti}5")
            ,"//result[@numFound='1']"
            );

     assertQ("test multi term field query on text type",
            req("q","{!field f=v_t}Hello  DUDE")
            ,"//result[@numFound='1']"
            );


    assertQ("test prefix query with value in local params",
            req("q","{!prefix f=v_t v=hel}")
            ,"//result[@numFound='2']"
    );

    assertQ("test optional quotes",
            req("q","{!prefix f='v_t' v=\"hel\"}")
            ,"//result[@numFound='2']"
    );

    assertQ("test extra whitespace",
            req("q","{!prefix   f=v_t   v=hel   }")
            ,"//result[@numFound='2']"
    );

    assertQ("test literal with {! in it",
            req("q","{!prefix f=v_s}{!lit")
            ,"//result[@numFound='1']"
    );

    assertQ("test param subst",
            req("q","{!prefix f=$myf v=$my.v}"
                ,"myf","v_t", "my.v", "hel"
            )
            ,"//result[@numFound='2']"
    );

    // test wacky param names
    assertQ(
            req("q","{!prefix f=$a/b/c v=$'a b/c'}"
                ,"a/b/c","v_t", "a b/c", "hel"
            )
            ,"//result[@numFound='2']"
    );

    assertQ("test param subst with literal",
            req("q","{!prefix f=$myf v=$my.v}"
                ,"myf","v_s", "my.v", "{!lit"
            )
            ,"//result[@numFound='1']"
    );

   // lucene queries
   assertQ("test lucene query",
            req("q","{!lucene}v_t:hel*")
            ,"//result[@numFound='2']"
            );

   // lucene queries
   assertQ("test lucene default field",
            req("q","{!df=v_t}hel*")
            ,"//result[@numFound='2']"
            );

   // lucene operator
   assertQ("test lucene operator",
            req("q","{!q.op=OR df=v_t}Hello Yonik")
            ,"//result[@numFound='2']"
            );
   assertQ("test lucene operator",
            req("q","{!q.op=AND df=v_t}Hello Yonik")
            ,"//result[@numFound='1']"
            );

    // test boost queries
    assertQ("test boost",
            req("q","{!boost b=sum(v_f,1)}id:[5 TO 6]"
                ,"fl","*,score"
            )
            ,"//result[@numFound='2']"
            ,"//doc[./float[@name='v_f']='3.14159' and ./float[@name='score']='4.14159']"
    );

    assertQ("test boost and default type of func",
            req("q","{!boost v=$q1 b=$q2}"
                ,"q1", "{!func}v_f", "q2","v_f"
                ,"fl","*,score"
            )
            ,"//doc[./float[@name='v_f']='1.5' and ./float[@name='score']='2.25']"
    );


    // dismax query from std request handler
    assertQ("test dismax query",
             req("q","{!dismax}hello"
                ,"qf","v_t"
                ,"bf","sqrt(v_f)^100 log(sum(v_f,1))^50"
                ,"bq","{!prefix f=v_t}he"
                , CommonParams.DEBUG_QUERY,"on"
             )
             ,"//result[@numFound='2']"
             );

    // dismax query from std request handler, using local params
    assertQ("test dismax query w/ local params",
             req("q","{!dismax qf=v_t}hello"
                ,"qf","v_f"
             )
             ,"//result[@numFound='2']"
             );

    assertQ("test nested query",
            req("q","_query_:\"{!query v=$q1}\"", "q1","{!prefix f=v_t}hel")
            ,"//result[@numFound='2']"
            );

    assertQ("test nested nested query",
            req("q","_query_:\"{!query defType=query v=$q1}\"", "q1","{!v=$q2}","q2","{!prefix f=v_t v=$qqq}","qqq","hel")
            ,"//result[@numFound='2']"
            );

  }
}
