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

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.ErrorLogMuter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

    
/**
 * This tests was converted from a legacy testing system.
 *
 * it does not represent the best practices that should be used when
 * writing Solr JUnit tests
 */
public class ConvertedLegacyTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  @Test
  public void testABunchOfConvertedStuff() {
    // these may be reused by things that need a special query
    SolrQueryRequest req = null;
    Map<String,String> args = new HashMap<>();
    lrf.args.put(CommonParams.VERSION,"2.2");

    // compact the index, keep things from getting out of hand

    assertU("<optimize/>");

    // test query

    assertQ(req("qlkciyopsbgzyvkylsjhchghjrdf")
            ,"//result[@numFound='0']"
            );

    // test escaping of ";"

    assertU("<delete><id>42</id></delete>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"val_s\">aa;bb</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND val_s:aa\\;bb")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:42 AND val_s:\"aa;bb\"")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:42 AND val_s:\"aa\"")
            ,"//*[@numFound='0']"
            );



    // test allowDups default of false

    assertU("<delete><id>42</id></delete>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"val_s\">AAA</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"val_s\">BBB</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42")
            ,"//*[@numFound='1'] "
            ,"//str[.='BBB']"
            );
    assertU("<add><doc><field name=\"id\">42</field><field name=\"val_s\">CCC</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"val_s\">DDD</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42")
            ,"//*[@numFound='1'] "
            ,"//str[.='DDD']"
            );
    assertU("<delete><id>42</id></delete>");

    // test deletes

    assertU("<delete><query>id:[100 TO 110]</query></delete>");
    assertU("<add overwrite=\"true\"><doc><field name=\"id\">101</field></doc></add>");
    assertU("<add overwrite=\"true\"><doc><field name=\"id\">101</field></doc></add>");
    assertU("<add  overwrite=\"false\"><doc><field name=\"id\">105</field></doc></add>");
    assertU("<add overwrite=\"true\"><doc><field name=\"id\">102</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">103</field></doc></add>");
    assertU("<add overwrite=\"true\"><doc><field name=\"id\">101</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='4']"
            );
    assertU("<delete><id>102</id></delete>");
    assertU("<commit/>");
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='3']"
            );
    assertU("<delete><query>id:105</query></delete>");
    assertU("<commit/>");
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='2']"
            );
    assertU("<delete><query>id:[100 TO 110]</query></delete>");
    assertU("<commit/>");
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='0']"
            );

    // test range

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"val_s\">apple</field><field name=\"val_s1\">apple</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"val_s\">banana</field><field name=\"val_s1\">banana</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"val_s\">pear</field><field name=\"val_s1\">pear</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("val_s:[a TO z]")
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=3] "
            ,"//*[@start='0']"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 2, 5 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=1] "
            ,"*//doc[1]/str[.='pear'] "
            ,"//*[@start='2']"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 3, 5 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 4, 5 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 25, 5 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 0, 1 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=1] "
            ,"*//doc[1]/str[.='apple']"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 0, 2 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=2] "
            ,"*//doc[2]/str[.='banana']"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 1, 1 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=1] "
            ,"*//doc[1]/str[.='banana']"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 3, 1 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 4, 1 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 1, 0 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 0, 0 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    args.put("sort","val_s1 asc");
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 0, 0 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    args = new HashMap<>();
    args.put("sort","val_s1 desc");
    req = new LocalSolrQueryRequest(h.getCore(), "val_s:[a TO z]",
                                    "/select", 0, 0 , args);
    assertQ(req
            ,"//*[@numFound='3'] "
            ,"*[count(//doc)=0]"
            );
    assertQ(req("val_s:[a TO b]")
            ,"//*[@numFound='1']"
            );
    assertQ(req("val_s:[a TO cat]")
            ,"//*[@numFound='2']"
            );
    assertQ(req("val_s:[a TO *]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:[* TO z]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:[* TO *]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:[apple TO pear]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:[bear TO boar]")
            ,"//*[@numFound='0']"
            );
    assertQ(req("val_s:[a TO a]")
            ,"//*[@numFound='0']"
            );
    assertQ(req("val_s:[apple TO apple]")
            ,"//*[@numFound='1']"
            );
    assertQ(req("val_s:{apple TO pear}")
            ,"//*[@numFound='1']"
            );
    assertQ(req("val_s:{a TO z}")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:{* TO *}")
            ,"//*[@numFound='3']"
            );
    // test rangequery within a boolean query

    assertQ(req("id:44 AND val_s:[a TO z]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("id:44 OR val_s:[a TO z]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:[a TO b] OR val_s:[b TO z]")
            ,"//*[@numFound='3']"
            );
    assertQ(req("+val_s:[a TO b] -val_s:[b TO z]")
            ,"//*[@numFound='1']"
            );
    assertQ(req("-val_s:[a TO b] +val_s:[b TO z]")
            ,"//*[@numFound='2']"
            );
    assertQ(req("val_s:[a TO c] AND val_s:[apple TO z]")
            ,"//*[@numFound='2']"
            );
    assertQ(req("val_s:[a TO c] AND val_s:[a TO apple]")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:44 AND (val_s:[a TO c] AND val_s:[a TO apple])")
            ,"//*[@numFound='1']"
            );
    assertQ(req("(val_s:[apple TO apple] OR val_s:[a TO c]) AND (val_s:[b TO c] OR val_s:[b TO b])")
            ,"//*[@numFound='1'] "
            ,"//str[.='banana']"
            );
    assertQ(req("(val_s:[apple TO apple] AND val_s:[a TO c]) OR (val_s:[p TO z] AND val_s:[a TO z])")
            ,"//*[@numFound='2'] "
            ,"//str[.='apple'] "
            ,"//str[.='pear']"
            );

    // check for docs that appear more than once in a range

    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"val_s\">apple</field><field name=\"val_s\">banana</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("val_s:[* TO *] OR  val_s:[* TO *]")
            ,"//*[@numFound='4']"
            );
    assertQ(req("val_s:[* TO *] AND  val_s:[* TO *]")
            ,"//*[@numFound='4']"
            );
    assertQ(req("val_s:[* TO *]")
            ,"//*[@numFound='4']"
            );


    // <delete><id>44</id></delete>

    assertU("<add><doc><field name=\"id\">44</field><field name=\"text\">red riding hood</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44 AND red")
            ,"//@numFound[.='1'] "
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:44 AND ride")
            ,"//@numFound[.='1']"
            );
    assertQ(req("id:44 AND blue")
            ,"//@numFound[.='0']"
            );

    // allow duplicates

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"text\">red riding hood</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"text\">big bad wolf</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//@numFound[.='2']"
            );
    assertQ(req("id:44 AND red")
            ,"//@numFound[.='1'] "
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:44 AND wolf")
            ,"//@numFound[.='1'] "
            ,"*[count(//doc)=1]"
            );
    assertQ(req("+id:44 red wolf")
            ,"//@numFound[.='2']"
            );

    // test removal of multiples w/o adding anything else

    assertU("<delete><id>44</id></delete>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//@numFound[.='0']"
            );

    // untokenized string type

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"ssto\">and a 10.4 ?</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//str[.='and a 10.4 ?']"
            );
    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"sind\">abc123</field></doc></add>");
    assertU("<commit/>");
    // TODO: how to search for something with spaces....

    assertQ(req("sind:abc123")
            ,"//@numFound[.='1'] "
            ,"*[count(//@name[.='sind'])=0] "
            ,"*[count(//@name[.='id'])=1]"
            );

    assertU("<delete><id>44</id></delete>");
    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"sindsto\">abc123</field></doc></add>");
    assertU("<commit/>");
    // TODO: how to search for something with spaces....

    assertQ(req("sindsto:abc123")
            ,"//str[.='abc123']"
            );

    // test output of multivalued fields

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"title\">yonik3</field><field name=\"title\" boost=\"2\">yonik4</field></doc></add>");
    assertU("<commit></commit>");
    assertQ(req("id:44")
            ,"//arr[@name='title'][./str='yonik3' and ./str='yonik4'] "
            ,"*[count(//@name[.='title'])=1]"
            );
    assertQ(req("title:yonik3")
            ,"//@numFound[.>'0']"
            );
    assertQ(req("title:yonik4")
            ,"//@numFound[.>'0']"
            );
    assertQ(req("title:yonik5")
            ,"//@numFound[.='0']"
            );
    assertU("<delete><query>title:yonik4</query></delete>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//@numFound[.='0']"
            );


    // not visible until commit

    assertU("<delete><id>44</id></delete>");
    assertU("<commit/>");
    assertU("<add><doc><field name=\"id\">44</field></doc></add>");
    assertQ(req("id:44")
            ,"//@numFound[.='0']"
            );
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//@numFound[.='1']"
            );

    // test configurable stop words

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"teststop\">world stopworda view</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("+id:44 +teststop:world")
            ,"//@numFound[.='1']"
            );
    assertQ(req("teststop:stopworda")
            ,"//@numFound[.='0']"
            );

    // test ignoreCase stop words

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"stopfilt\">world AnD view</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("+id:44 +stopfilt:world")
            ,"//@numFound[.='1']"
            );
    assertQ(req("stopfilt:\"and\"")
            ,"//@numFound[.='0']"
            );
    assertQ(req("stopfilt:\"AND\"")
            ,"//@numFound[.='0']"
            );
    assertQ(req("stopfilt:\"AnD\"")
            ,"//@numFound[.='0']"
            );

    // test dynamic field types

    assertU("<delete fromPending=\"true\" fromCommitted=\"true\"><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"gack_i\">51778</field><field name=\"t_name\">cats</field></doc></add>");
    assertU("<commit/>");
    // test if the dyn fields got added

    assertQ(req("id:44")
            ,"*[count(//doc/*)>=3]  "
            ,"//arr[@name='gack_i']/int[.='51778']  "
            ,"//arr[@name='t_name']/str[.='cats']"
            );
    // now test if we can query by a dynamic field (requires analyzer support)

    assertQ(req("t_name:cat")
            ,"//arr[@name='t_name' and .='cats']/str"
            );
    // check that deleteByQuery works for dynamic fields

    assertU("<delete><query>t_name:cat</query></delete>");
    assertU("<commit/>");
    assertQ(req("t_name:cat")
            ,"//@numFound[.='0']"
            );

    // test that longest dynamic field match happens first

    assertU("<add><doc><field name=\"id\">44</field><field name=\"xaa\">mystr</field><field name=\"xaaa\">12321</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//arr[@name='xaa'][.='mystr']/str  "
            ,"//arr[@name='xaaa'][.='12321']/int"
            );


    // test integer ranges and sorting

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">1234567890</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">10</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">2</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">15</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">-1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">-987654321</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">2147483647</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">-2147483648</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_i1\">0</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"*[count(//doc)=10]"
            );
    assertQ(req("num_i1:2147483647")
            ,"//@numFound[.='1']  "
            ,"//int[.='2147483647']"
            );
    assertQ(req("num_i1:\"-2147483648\"")
            ,"//@numFound[.='1'] "
            ,"//int[.='-2147483648']"
            );
    assertQ(req("q", "id:44", "sort","num_i1 asc")
            ,"//doc[1]/int[.='-2147483648'] "
            ,"//doc[last()]/int[.='2147483647']"
            );
    assertQ(req("q","id:44","sort","num_i1 desc")
            ,"//doc[1]/int[.='2147483647'] "
            ,"//doc[last()]/int[.='-2147483648']"
            );
    assertQ(req("num_i1:[0 TO 9]")
            ,"*[count(//doc)=3]"
            );
    assertQ(req("num_i1:[-2147483648 TO 2147483647]")
            ,"*[count(//doc)=10]"
            );
    assertQ(req("num_i1:[-10 TO -1]")
            ,"*[count(//doc)=1]"
            );

    // test long ranges and sorting

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">1234567890</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">10</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">2</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">15</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">-1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">-987654321</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">9223372036854775807</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">-9223372036854775808</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_l1\">0</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"*[count(//doc)=10]"
            );
    assertQ(req("num_l1:9223372036854775807")
            ,"//@numFound[.='1'] "
            ,"//long[.='9223372036854775807']"
            );
    assertQ(req("num_l1:\"-9223372036854775808\"")
            ,"//@numFound[.='1'] "
            ,"//long[.='-9223372036854775808']"
            );
    assertQ(req("q","id:44","sort","num_l1 asc")
            ,"//doc[1]/long[.='-9223372036854775808'] "
            ,"//doc[last()]/long[.='9223372036854775807']"
            );
    assertQ(req("q","id:44", "sort", "num_l1 desc")
            ,"//doc[1]/long[.='9223372036854775807'] "
            ,"//doc[last()]/long[.='-9223372036854775808']"
            );
    assertQ(req("num_l1:[-1 TO 9]")
            ,"*[count(//doc)=4]"
            );
    assertQ(req("num_l1:[-9223372036854775808 TO 9223372036854775807]")
            ,"*[count(//doc)=10]"
            );
    assertQ(req("num_l1:[-10 TO -1]")
            ,"*[count(//doc)=1]"
            );

    // test binary float ranges and sorting

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">1.4142135</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">Infinity</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">-Infinity</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">NaN</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">2</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">-1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">-987654321</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">-999999.99</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">-1e20</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_f1\">0</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"*[count(//doc)=10]"
            );
    assertQ(req("num_f1:Infinity")
            ,"//@numFound[.='1']  "
            ,"//float[.='Infinity']"
            );
    assertQ(req("num_f1:\"-Infinity\"")
            ,"//@numFound[.='1']  "
            ,"//float[.='-Infinity']"
            );
    assertQ(req("num_f1:\"NaN\"")
            ,"//@numFound[.='1']  "
            ,"//float[.='NaN']"
            );
    assertQ(req("num_f1:\"-1e20\"")
            ,"//@numFound[.='1']"
            );
    assertQ(req("q", "id:44", "sort", "num_f1 asc")
            ,"//doc[1]/float[.='-Infinity'] "
            ,"//doc[last()]/float[.='NaN']"
            );
    assertQ(req("q", "id:44", "sort","num_f1 desc")
            ,"//doc[1]/float[.='NaN'] "
            ,"//doc[last()]/float[.='-Infinity']"
            );
    assertQ(req("num_f1:[-1 TO 2]")
            ,"*[count(//doc)=4]"
            );
    assertQ(req("num_f1:[-Infinity TO Infinity]")
            ,"*[count(//doc)=9]"
            );



    // test binary double ranges and sorting

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">1.4142135</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">Infinity</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">-Infinity</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">NaN</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">2</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">-1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">1e-100</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">-999999.99</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">-1e100</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"num_d1\">0</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"*[count(//doc)=10]"
            );
    assertQ(req("num_d1:Infinity")
            ,"//@numFound[.='1']  "
            ,"//double[.='Infinity']"
            );
    assertQ(req("num_d1:\"-Infinity\"")
            ,"//@numFound[.='1']  "
            ,"//double[.='-Infinity']"
            );
    assertQ(req("num_d1:\"NaN\"")
            ,"//@numFound[.='1']  "
            ,"//double[.='NaN']"
            );
    assertQ(req("num_d1:\"-1e100\"")
            ,"//@numFound[.='1']"
            );
    assertQ(req("num_d1:\"1e-100\"")
            ,"//@numFound[.='1']"
            );
    assertQ(req("q", "id:44", "sort", "num_d1 asc")
            ,"//doc[1]/double[.='-Infinity'] "
            ,"//doc[last()]/double[.='NaN']"
            );
    assertQ(req("q","id:44","sort","num_d1 desc")
            ,"//doc[1]/double[.='NaN'] "
            ,"//doc[last()]/double[.='-Infinity']"
            );
    assertQ(req("num_d1:[-1 TO 2]")
            ,"*[count(//doc)=5]"
            );
    assertQ(req("num_d1:[-Infinity TO Infinity]")
            ,"*[count(//doc)=9]"
            );


    // test sorting on multiple fields

    assertU("<delete><id>44</id></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"a_i1\">10</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"a_i1\">1</field><field name=\"b_i1\">100</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"a_i1\">-1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"a_i1\">15</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"a_i1\">1</field><field name=\"b_i1\">50</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id\">44</field><field name=\"a_i1\">0</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"*[count(//doc)=6]"
            );

    assertQ(req("q","id:44", "sort", "a_i1 asc,b_i1 desc")
            ,"*[count(//doc)=6] "
            ,"//doc[3]/int[.='100'] "
            ,"//doc[4]/int[.='50']"
            );
    assertQ(req("q","id:44", "sort", "a_i1 asc  , b_i1 asc")
            ,"*[count(//doc)=6] "
            ,"//doc[3]/int[.='50'] "
            ,"//doc[4]/int[.='100']"
            );
    assertQ(req("q", "id:44", "sort", "a_i1 asc")
            ,"*[count(//doc)=6] "
            ,"//doc[1]/int[.='-1'] "
            ,"//doc[last()]/int[.='15']"
            );
    assertQ(req("q","id:44","sort","a_i1 asc , score top")
            ,"*[count(//doc)=6] "
            ,"//doc[1]/int[.='-1'] "
            ,"//doc[last()]/int[.='15']"
            );
    assertQ(req("q","id:44","sort","score top , a_i1 top, b_i1 bottom ")
            ,"*[count(//doc)=6] "
            ,"//doc[last()]/int[.='-1'] "
            ,"//doc[1]/int[.='15'] "
            ,"//doc[3]/int[.='50'] "
            ,"//doc[4]/int[.='100']"
            );


    // test sorting  with some docs missing the sort field

    assertU("<delete><query>id_i:[1000 TO 1010]</query></delete>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1000</field><field name=\"a_i1\">1</field><field name=\"nullfirst\">Z</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1001</field><field name=\"a_i1\">10</field><field name=\"nullfirst\">A</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1002</field><field name=\"a_i1\">1</field><field name=\"b_i1\">100</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1003</field><field name=\"a_i1\">-1</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1004</field><field name=\"a_i1\">15</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1005</field><field name=\"a_i1\">1</field><field name=\"b_i1\">50</field></doc></add>");
    assertU("<add overwrite=\"false\"><doc><field name=\"id_i\">1006</field><field name=\"a_i1\">0</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id_i:[1000 TO 1010]")
            ,"*[count(//doc)=7]"
            );
    assertQ(req("q","id_i:[1000 TO 1010]","sort","b_i1 asc")
            ,"*[count(//doc)=7] "
            ,"//doc[1]/int[.='50'] "
            ,"//doc[2]/int[.='100']"
            );
    assertQ(req("q","id_i:[1000 TO 1010]","sort"," b_i1 desc")
            ,"*[count(//doc)=7] "
            ,"//doc[1]/int[.='100'] "
            ,"//doc[2]/int[.='50']"
            );
    assertQ(req("q","id_i:[1000 TO 1010]","sort"," a_i1 asc,b_i1 desc")
            ,"*[count(//doc)=7] "
            ,"//doc[3]/int[@name='b_i1' and .='100'] "
            ,"//doc[4]/int[@name='b_i1' and .='50']  "
            ,"//doc[5]/arr[@name='id_i' and .='1000']"
            );
    assertQ(req("q","id_i:[1000 TO 1010]","sort"," a_i1 asc,b_i1 asc")
            ,"*[count(//doc)=7] "
            ,"//doc[3]/int[@name='b_i1' and .='50'] "
            ,"//doc[4]/int[@name='b_i1' and .='100']  "
            ,"//doc[5]/arr[@name='id_i' and .='1000']"
            );
    // nullfirst tests
    assertQ(req("q","id_i:[1000 TO 1002]","sort"," nullfirst asc")
            ,"*[count(//doc)=3] "
            ,"//doc[1]/arr[@name='id_i' and .='1002']"
            ,"//doc[2]/arr[@name='id_i' and .='1001']  "
            ,"//doc[3]/arr[@name='id_i' and .='1000']"
            );
    assertQ(req("q","id_i:[1000 TO 1002]","sort"," nullfirst desc")
            ,"*[count(//doc)=3] "
            ,"//doc[1]/arr[@name='id_i' and .='1002']"
            ,"//doc[2]/arr[@name='id_i' and .='1000']  "
            ,"//doc[3]/arr[@name='id_i' and .='1001']"
            );

    // Sort parsing exception tests.  (SOLR-6, SOLR-99)
    try (ErrorLogMuter errors = ErrorLogMuter.substring("shouldbeunindexed")) {
      assertQEx( "can not sort unindexed fields",
                 req( "q","id_i:1000", "sort", "shouldbeunindexed asc" ), 400 );
      assertEquals(1, errors.getCount());
    }
    
    try (ErrorLogMuter errors = ErrorLogMuter.substring("nullfirst")) {
      assertQEx( "invalid query format",
                 req( "q","id_i:1000", "sort", "nullfirst" ), 400 );
      assertEquals(1, errors.getCount());
    }

    try (ErrorLogMuter abc = ErrorLogMuter.substring("abcde12345");
         ErrorLogMuter aaa = ErrorLogMuter.substring("aaa")) {
      assertQEx( "unknown sort field",
                 req( "q","id_i:1000", "sort", "abcde12345 asc" ), 400 ); 

      assertQEx( "unknown sort order",
                 req( "q","id_i:1000", "sort", "nullfirst aaa" ), 400 );
      
      assertEquals(1, abc.getCount());
      assertEquals(1, aaa.getCount());
    }
        
    // test prefix query

    assertU("<delete><query>val_s:[* TO *]</query></delete>");
    assertU("<add><doc><field name=\"id\">100</field><field name=\"val_s\">apple</field></doc></add>");
    assertU("<add><doc><field name=\"id\">101</field><field name=\"val_s\">banana</field></doc></add>");
    assertU("<add><doc><field name=\"id\">102</field><field name=\"val_s\">apple</field></doc></add>");
    assertU("<add><doc><field name=\"id\">103</field><field name=\"val_s\">pearing</field></doc></add>");
    assertU("<add><doc><field name=\"id\">104</field><field name=\"val_s\">pear</field></doc></add>");
    assertU("<add><doc><field name=\"id\">105</field><field name=\"val_s\">appalling</field></doc></add>");
    assertU("<add><doc><field name=\"id\">106</field><field name=\"val_s\">pearson</field></doc></add>");
    assertU("<add><doc><field name=\"id\">107</field><field name=\"val_s\">port</field></doc></add>");
    assertU("<commit/>");

    assertQ(req("val_s:a*")
            ,"//*[@numFound='3']"
            );
    assertQ(req("val_s:p*")
            ,"//*[@numFound='4']"
            );
    // val_s:* %//*[@numFound="8"]

    // test wildcard query
    assertQ(req("val_s:a*p*") ,"//*[@numFound='3']");
    assertQ(req("val_s:p?a*") ,"//*[@numFound='3']");

    assertU("<delete><query>id:[100 TO 110]</query></delete>");

    // test copyField functionality

    assertU("<add><doc><field name=\"id\">42</field><field name=\"title\">How Now4 brown Cows</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND title:Now")
            ,"*[count(//doc)=0]"
            );
    assertQ(req("id:42 AND title_lettertok:Now")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND title:cow")
            ,"*[count(//doc)=0]"
            );
    assertQ(req("id:42 AND title_stemmed:cow")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND text:cow")
            ,"*[count(//doc)=1]"
            );

    // test copyField functionality with a pattern.

    assertU("<add><doc><field name=\"id\">42</field><field name=\"copy_t\">Copy me to the text field pretty please.</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND text:pretty")
        ,"*[count(//doc)=1]"
        );
    assertQ(req("id:42 AND copy_t:pretty")
        ,"*[count(//doc)=1]"
        );
    
    // test slop

    assertU("<add><doc><field name=\"id\">42</field><field name=\"text\">foo bar</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND text:\"foo bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND text:\"foo\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND text:\"bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND text:\"bar foo\"")
            ,"*[count(//doc)=0]"
            );
    assertQ(req("id:42 AND text:\"bar foo\"~2")
            ,"*[count(//doc)=1]"
            );


    // intra-word delimiter testing (WordDelimiterGraphFilter)

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">foo bar</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:\"foo bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"foo\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"bar foo\"")
            ,"*[count(//doc)=0]"
            );
    assertQ(req("id:42 AND subword:\"bar foo\"~2")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"foo/bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:foobar")
            ,"*[count(//doc)=0]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">foo-bar</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:\"foo bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"foo\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"bar foo\"")
            ,"*[count(//doc)=0]"
            );
    assertQ(req("id:42 AND subword:\"bar foo\"~2")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"foo/bar\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:foobar")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">Canon PowerShot SD500 7MP</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:\"power-shot\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"power shot sd 500\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"powershot\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"SD-500\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"SD500\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"SD500-7MP\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"PowerShotSD500-7MP\"")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">Wi-Fi</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:wifi")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:wi+=fi")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:wi+=fi")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:WiFi")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"wi fi\"")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">'I.B.M' A's,B's,C's</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:\"'I.B.M.'\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:I.B.M")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:IBM")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:I--B--M")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"I B M\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:IBM's")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"IBM'sx\"")
            ,"*[count(//doc)=0]"
            );

    // this one fails since IBM and ABC are separated by two tokens

    // id:42 AND subword:IBM's-ABC's  %*[count(//doc)=1]

    assertQ(req("id:42 AND subword:\"IBM's-ABC's\"~2")
            ,"*[count(//doc)=1]"
            );

    assertQ(req("id:42 AND subword:\"A's B's-C's\"")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">Sony KDF-E50A10</field></doc></add>");
    assertU("<commit/>");

    // check for exact match:

    //  Sony KDF E/KDFE 50 A 10  (this is how it's indexed)

    //  Sony KDF E      50 A 10  (and how it's queried)

    assertQ(req("id:42 AND subword:\"Sony KDF-E50A10\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:10")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:Sony")
            ,"*[count(//doc)=1]"
            );

    // this one fails without slop since Sony and KDFE have a token inbetween

    // id:42 AND subword:SonyKDFE50A10  %*[count(//doc)=1]

    assertQ(req("id:42 AND subword:\"SonyKDFE50A10\"~10")
            ,"*[count(//doc)=1]"
            );

    assertQ(req("id:42 AND subword:\"Sony KDF E-50-A-10\"")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">http://www.yahoo.com</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:yahoo")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:www.yahoo.com")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:http\\://www.yahoo.com")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">--Q 1-- W2 E-3 Ok xY 4R 5-T *6-Y- 7-8-- 10A-B</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:42 AND subword:Q")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:1")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"w 2\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"e 3\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"o k\"")
            ,"*[count(//doc)=0]"
            );
    assertQ(req("id:42 AND subword:\"ok\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"x y\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"xy\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"4 r\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"5 t\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"5 t\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"6 y\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"7 8\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"78\"")
            ,"*[count(//doc)=1]"
            );
    assertQ(req("id:42 AND subword:\"10 A+B\"")
            ,"*[count(//doc)=1]"
            );

    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">FooBarBaz</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">FooBar10</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">10FooBar</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">BAZ</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">10</field></doc></add>");
    assertU("<add><doc><field name=\"id\">42</field><field name=\"subword\">Mark, I found what's the problem! It turns to be from the latest schema. I found tons of exceptions in the resin.stdout that prevented the builder from performing. It's all coming from the WordDelimiterFilter which was just added to the latest schema: [2005-08-29 15:11:38.375] java.lang.IndexOutOfBoundsException: Index: 3, Size: 3 673804 [2005-08-29 15:11:38.375]  at java.util.ArrayList.RangeCheck(ArrayList.java:547) 673805 [2005-08-29 15:11:38.375]  at java.util.ArrayList.get(ArrayList.java:322) 673806 [2005-08-29 15:11:38.375]  at solr.analysis.WordDelimiterFilter.addCombos(WordDelimiterFilter.java:349) 673807 [2005-08-29 15:11:38.375]  at solr.analysis.WordDelimiterFilter.next(WordDelimiterFilter.java:325) 673808 [2005-08-29 15:11:38.375]  at org.apache.lucene.analysis.LowerCaseFilter.next(LowerCaseFilter.java:32) 673809 [2005-08-29 15:11:38.375]  at org.apache.lucene.analysis.StopFilter.next(StopFilter.java:98) 673810 [2005-08-29 15:11:38.375]  at solr.EnglishPorterFilter.next(TokenizerFactory.java:163) 673811 [2005-08-29 15:11:38.375]  at org.apache.lucene.index.DocumentWriter.invertDocument(DocumentWriter.java:143) 673812 [2005-08-29 15:11:38.375]  at org.apache.lucene.index.DocumentWriter.addDocument(DocumentWriter.java:81) 673813 [2005-08-29 15:11:38.375]  at org.apache.lucene.index.IndexWriter.addDocument(IndexWriter.java:307) 673814 [2005-08-29 15:11:38.375]  at org.apache.lucene.index.IndexWriter.addDocument(IndexWriter.java:294) 673815 [2005-08-29 15:11:38.375]  at solr.DirectUpdateHandler2.doAdd(DirectUpdateHandler2.java:170) 673816 [2005-08-29 15:11:38.375]  at solr.DirectUpdateHandler2.overwriteBoth(DirectUpdateHandler2.java:317) 673817 [2005-08-29 15:11:38.375]  at solr.DirectUpdateHandler2.addDoc(DirectUpdateHandler2.java:191) 673818 [2005-08-29 15:11:38.375]  at solr.SolrCore.update(SolrCore.java:795) 673819 [2005-08-29 15:11:38.375]  at solrserver.SolrServlet.doPost(SolrServlet.java:71) 673820 [2005-08-29 15:11:38.375]  at javax.servlet.http.HttpServlet.service(HttpServlet.java:154) 673821 [2005-08-29 15:11:38.375]  at javax.servlet.http.HttpServlet.service(HttpServlet.java:92) 673822 [2005-08-29 15:11:38.375]  at com.caucho.server.dispatch.ServletFilterChain.doFilter(ServletFilterChain.java:99) 673823 [2005-08-29 15:11:38.375]  at com.caucho.server.cache.CacheFilterChain.doFilter(CacheFilterChain.java:188) 673824 [2005-08-29 15:11:38.375]  at com.caucho.server.webapp.WebAppFilterChain.doFilter(WebAppFilterChain.java:163) 673825 [2005-08-29 15:11:38.375]  at com.caucho.server.dispatch.ServletInvocation.service(ServletInvocation.java:208) 673826 [2005-08-29 15:11:38.375]  at com.caucho.server.http.HttpRequest.handleRequest(HttpRequest.java:259) 673827 [2005-08-29 15:11:38.375]  at com.caucho.server.port.TcpConnection.run(TcpConnection.java:363) 673828 [2005-08-29 15:11:38.375]  at com.caucho.util.ThreadPool.runTasks(ThreadPool.java:490) 673829 [2005-08-29 15:11:38.375]  at com.caucho.util.ThreadPool.run(ThreadPool.java:423) 673830 [2005-08-29 15:11:38.375]  at java.lang.Thread.run(Thread.java:595) With the previous schema I'm able to perform a successful full build: http://c12-ssa-dev40-so-mas1.cnet.com:5078/select/?stylesheet=q=docTypeversion=2.0start=0rows=10indent=on Do you want to rollback to the previous schema version</field></doc></add>");


    // 

    assertU("<delete fromPending=\"true\" fromCommitted=\"true\"><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"fname_s\">Yonik</field><field name=\"here_b\">true</field><field name=\"iq_l\">10000000000</field><field name=\"description_t\">software engineer</field><field name=\"ego_d\">1e100</field><field name=\"pi_f\">3.1415962</field><field name=\"when_dt\">2005-03-18T01:14:34Z</field><field name=\"arr_f\">1.414213562</field><field name=\"arr_f\">.999</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            );
    args = new HashMap<>();
    args.put("fl","fname_s,arr_f  ");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//str[.='Yonik']  "
            ,"//float[.='1.4142135']"
            );
    args = new HashMap<>();
    args.put("fl","fname_s,score");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//str[.='Yonik']"
            ,"//float[@name='score' and . > 0]"
            );

    // test addition of score field

    args = new HashMap<>();
    args.put("fl","score,* ");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//str[.='Yonik']  "
            ,"//float[.='1.4142135'] "
            ,"//float[@name='score'] "
            ,"*[count(//doc/*)>=13]"
            );
    args = new HashMap<>();
    args.put("fl","*,score ");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//str[.='Yonik']  "
            ,"//float[.='1.4142135'] "
            ,"//float[@name='score'] "
            ,"*[count(//doc/*)>=13]"
            );
    args = new HashMap<>();
    args.put("fl","* ");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//str[.='Yonik']  "
            ,"//float[.='1.4142135'] "
            ,"*[count(//doc/*)>=12]"
            );

    // test maxScore

    args = new HashMap<>();
    args.put("fl","score ");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//result[@maxScore>0]"
            );
    args = new HashMap<>();
    args.put("fl","score ");
    args.put("sort","id desc");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//result[@maxScore>0]"
            );
    args = new HashMap<>();
    args.put("fl","score ");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//@maxScore = //doc/float[@name='score']"
            );
    args = new HashMap<>();
    args.put("fl","score ");
    args.put("sort","id desc");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 10, args);
    assertQ(req
            ,"//@maxScore = //doc/float[@name='score']"
            );
    args = new HashMap<>();
    args.put("fl","*,score");
    args.put("sort","id desc");
    req = new LocalSolrQueryRequest(h.getCore(), "id:44",
                                    "/select", 0, 0 , args);
    assertQ(req
            ,"//result[@maxScore>0]"
            );


    //  test schema field attribute inheritance and overriding

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"shouldbestored\">hi</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//*[@name='shouldbestored']"
            );
    assertQ(req("+id:44 +shouldbestored:hi")
            ,"//*[@numFound='1']"
            );

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"shouldbeunstored\">hi</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"not(//*[@name='shouldbeunstored'])"
            );
    assertQ(req("+id:44 +shouldbeunstored:hi")
            ,"//*[@numFound='1']"
            );

    assertU("<delete><id>44</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"shouldbeunindexed\">hi</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:44")
            ,"//*[@name='shouldbeunindexed']"
            );
    //  this should result in an error... how to check for that?

    // +id:44 +shouldbeunindexed:hi %//*[@numFound="0"]


    // test spaces between XML elements because that can introduce extra XML events that

    // can mess up parsing (and it has in the past)

    assertU("<delete>  <id>44</id>  </delete>");
    assertU("<add>  <doc>  <field name=\"id\">44</field>  <field name=\"shouldbestored\">hi</field>  </doc>  </add>");
    assertU("<commit />");

    // test adding multiple docs per add command

    // assertU("<delete><query>id:[0 TO 99]</query></delete>");
    // assertU("<add><doc><field name=\"id\">1</field></doc><doc><field name=\"id\">2</field></doc></add>");
    // assertU("<commit/>");
    // assertQ(req("id:[0 TO 99]")
    // ,"//*[@numFound='2']"
    // );

    // test synonym filter

    assertU("<delete><query>id:[10 TO 100]</query></delete>");
    assertU("<add><doc><field name=\"id\">10</field><field name=\"syn\">a</field></doc></add>");
    assertU("<add><doc><field name=\"id\">11</field><field name=\"syn\">b</field></doc></add>");
    assertU("<add><doc><field name=\"id\">12</field><field name=\"syn\">c</field></doc></add>");
    assertU("<add><doc><field name=\"id\">13</field><field name=\"syn\">foo</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("id:10 AND syn:a")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:10 AND syn:aa")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:11 AND syn:b")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:11 AND syn:b1")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:11 AND syn:b2")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:12 AND syn:c")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:12 AND syn:c1")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:12 AND syn:c2")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:13 AND syn:foo")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:13 AND syn:bar")
            ,"//*[@numFound='1']"
            );
    assertQ(req("id:13 AND syn:baz")
            ,"//*[@numFound='1']"
            );


    // test position increment gaps between field values

    assertU("<delete><id>44</id></delete>");
    assertU("<delete><id>45</id></delete>");
    assertU("<add><doc><field name=\"id\">44</field><field name=\"textgap\">aa bb cc</field><field name=\"textgap\">dd ee ff</field></doc></add>");
    assertU("<add><doc><field name=\"id\">45</field><field name=\"text\">aa bb cc</field><field name=\"text\">dd ee ff</field></doc></add>");
    assertU("<commit/>");
    assertQ(req("+id:44 +textgap:\"aa bb cc\"")
            ,"//*[@numFound='1']"
            );
    assertQ(req("+id:44 +textgap:\"dd ee ff\"")
            ,"//*[@numFound='1']"
            );
    assertQ(req("+id:44 +textgap:\"cc dd\"")
            ,"//*[@numFound='0']"
            );
    assertQ(req("+id:44 +textgap:\"cc dd\"~100")
            ,"//*[@numFound='1']"
            );
    assertQ(req("+id:44 +textgap:\"bb cc dd ee\"~90")
            ,"//*[@numFound='0']"
            );
    assertQ(req("+id:44 +textgap:\"bb cc dd ee\"~100")
            ,"//*[@numFound='1']"
            );
    assertQ(req("+id:45 +text:\"cc dd\"")
            ,"//*[@numFound='1']"
            );
  }
}
