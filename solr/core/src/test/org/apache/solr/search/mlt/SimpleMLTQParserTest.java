package org.apache.solr.search.mlt;

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
    delQ("*:*");
    assertU(adoc(id, "1", "lowerfilt", "toyota"));
    assertU(adoc(id, "2", "lowerfilt", "chevrolet"));
    assertU(adoc(id, "3", "lowerfilt", "suzuki"));
    assertU(adoc(id, "4", "lowerfilt", "ford"));
    assertU(adoc(id, "5", "lowerfilt", "ferrari"));
    assertU(adoc(id, "6", "lowerfilt", "jaguar"));
    assertU(adoc(id, "7", "lowerfilt", "mclaren moon or the moon and moon moon shine " +
        "and the moon but moon was good foxes too"));
    assertU(adoc(id, "8", "lowerfilt", "sonata"));
    assertU(adoc(id, "9", "lowerfilt", "The quick red fox jumped over the lazy big " +
        "and large brown dogs."));
    assertU(adoc(id, "10", "lowerfilt", "blue"));
    assertU(adoc(id, "12", "lowerfilt", "glue"));
    assertU(adoc(id, "13", "lowerfilt", "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "14", "lowerfilt", "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "15", "lowerfilt", "The fat red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "16", "lowerfilt", "The slim red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "17", "lowerfilt", "The quote red fox jumped moon over the lazy " +
        "brown dogs moon. Of course moon. Foxes and moon come back to the foxes and moon"));
    assertU(adoc(id, "18", "lowerfilt", "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "19", "lowerfilt", "The hose red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "20", "lowerfilt", "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "21", "lowerfilt", "The court red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "22", "lowerfilt", "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "23", "lowerfilt", "The quote red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "24", "lowerfilt", "The file red fox jumped over the lazy brown dogs."));
    assertU(adoc(id, "25", "lowerfilt", "rod fix"));

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
  }

}
