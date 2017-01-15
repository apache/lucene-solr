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
package org.apache.solr.search.mlt;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO: Assert against expected parsed query for different min/maxidf values.
public class SimpleMLTQParserTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void moreLikeThisBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void doTest() throws Exception {
    String id = "id";
    String FIELD1 = "lowerfilt" ;
    String FIELD2 = "lowerfilt1" ;
    delQ("*:*");
    assertU(adoc(id, "1", FIELD1, "toyota"));
    assertU(adoc(id, "2", FIELD1, "chevrolet"));
    assertU(adoc(id, "3", FIELD1, "suzuki"));
    assertU(adoc(id, "4", FIELD1, "ford"));
    assertU(adoc(id, "5", FIELD1, "ferrari"));
    assertU(adoc(id, "6", FIELD1, "jaguar"));
    assertU(adoc(id, "7", FIELD1, "mclaren moon or the moon and moon moon shine " +
        "and the moon but moon was good foxes too"));
    assertU(adoc(id, "8", FIELD1, "sonata"));
    assertU(adoc(id, "9", FIELD1, "The quick red fox jumped over the lazy big " +
        "and large brown dogs."));
    assertU(adoc(id, "10", FIELD1, "blue"));
    assertU(adoc(id, "12", FIELD1, "glue"));
    assertU(adoc(id, "13", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "14", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "15", FIELD1, "The fat red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "16", FIELD1, "The slim red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "17", FIELD1, "The quote red fox jumped moon over the lazy " +
        "brown dogs moon. Of course moon. Foxes and moon come back to the foxes and moon"));
    assertU(adoc(id, "18", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "19", FIELD1, "The hose red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "20", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "21", FIELD1, "The court red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "22", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "23", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "24", FIELD1, "The file red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "25", FIELD1, "rod fix"));
    assertU(adoc(id, "26", FIELD1, "bmw usa 328i"));
    assertU(adoc(id, "27", FIELD1, "bmw usa 535i"));
    assertU(adoc(id, "28", FIELD1, "bmw 750Li"));
    assertU(adoc(id, "29", FIELD1, "bmw usa",
        FIELD2, "red green blue"));
    assertU(adoc(id, "30", FIELD1, "The quote red fox jumped over the lazy brown dogs.",
        FIELD2, "red green yellow"));
    assertU(adoc(id, "31", FIELD1, "The fat red fox jumped over the lazy brown dogs.",
        FIELD2, "green blue yellow"));
    assertU(adoc(id, "32", FIELD1, "The slim red fox jumped over the lazy brown dogs.",
        FIELD2, "yellow white black"));

    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}17");
    assertQ(req(params),
        "//result/doc[1]/int[@name='id'][.='13']",
        "//result/doc[2]/int[@name='id'][.='14']",
        "//result/doc[3]/int[@name='id'][.='15']",
        "//result/doc[4]/int[@name='id'][.='16']",
        "//result/doc[5]/int[@name='id'][.='18']",
        "//result/doc[6]/int[@name='id'][.='19']",
        "//result/doc[7]/int[@name='id'][.='20']",
        "//result/doc[8]/int[@name='id'][.='21']",
        "//result/doc[9]/int[@name='id'][.='22']",
        "//result/doc[10]/int[@name='id'][.='23']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt boost=true}17");
    assertQ(req(params),
        "//result/doc[1]/int[@name='id'][.='13']",
        "//result/doc[2]/int[@name='id'][.='14']",
        "//result/doc[3]/int[@name='id'][.='15']",
        "//result/doc[4]/int[@name='id'][.='16']",
        "//result/doc[5]/int[@name='id'][.='18']",
        "//result/doc[6]/int[@name='id'][.='19']",
        "//result/doc[7]/int[@name='id'][.='20']",
        "//result/doc[8]/int[@name='id'][.='21']",
        "//result/doc[9]/int[@name='id'][.='22']",
        "//result/doc[10]/int[@name='id'][.='23']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt,lowerfilt1^1000 boost=false mintf=0 mindf=0}30");
    assertQ(req(params),
        "//result/doc[1]/int[@name='id'][.='31']",
        "//result/doc[2]/int[@name='id'][.='13']",
        "//result/doc[3]/int[@name='id'][.='14']",
        "//result/doc[4]/int[@name='id'][.='18']",
        "//result/doc[5]/int[@name='id'][.='20']",
        "//result/doc[6]/int[@name='id'][.='22']",
        "//result/doc[7]/int[@name='id'][.='23']",
        "//result/doc[8]/int[@name='id'][.='32']",
        "//result/doc[9]/int[@name='id'][.='15']",
        "//result/doc[10]/int[@name='id'][.='16']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt,lowerfilt1^1000 boost=true mintf=0 mindf=0}30");
    assertQ(req(params),
        "//result/doc[1]/int[@name='id'][.='29']",
        "//result/doc[2]/int[@name='id'][.='31']",
        "//result/doc[3]/int[@name='id'][.='32']",
        "//result/doc[4]/int[@name='id'][.='13']",
        "//result/doc[5]/int[@name='id'][.='14']",
        "//result/doc[6]/int[@name='id'][.='18']",
        "//result/doc[7]/int[@name='id'][.='20']",
        "//result/doc[8]/int[@name='id'][.='22']",
        "//result/doc[9]/int[@name='id'][.='23']",
        "//result/doc[10]/int[@name='id'][.='15']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt mindf=0 mintf=1}26");
    assertQ(req(params),
        "//result/doc[1]/int[@name='id'][.='29']",
        "//result/doc[2]/int[@name='id'][.='27']",
        "//result/doc[3]/int[@name='id'][.='28']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt mindf=10 mintf=1}26");
    assertQ(req(params),
        "//result[@numFound='0']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt minwl=3 mintf=1 mindf=1}26");
    assertQ(req(params),
        "//result[@numFound='3']"
    );

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt minwl=4 mintf=1 mindf=1}26");
    params.set(CommonParams.DEBUG, "true");
    assertQ(req(params),
        "//result[@numFound='0']"
    );
  }

}
