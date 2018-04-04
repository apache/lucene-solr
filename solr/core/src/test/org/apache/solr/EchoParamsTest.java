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
import org.junit.BeforeClass;
import org.junit.Test;

/** Test SOLR-59, echo of query parameters */

public class EchoParamsTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
  }

  private static final String HEADER_XPATH = "/response/lst[@name='responseHeader']";
  
  @Test
  public void test() {
    defaultEchoParams();
    defaultEchoParamsDefaultVersion();
    explicitEchoParams();
    allEchoParams();
  }

  // the following test methods rely on their order, which is no longer guaranteed by Java 7, so call them directly above:
  
  private void defaultEchoParams() {
    lrf.args.put("wt", "xml");
    lrf.args.put(CommonParams.VERSION, "2.2");    
    assertQ(req("foo"),HEADER_XPATH + "/int[@name='status']");
    assertQ(req("foo"),"not(//lst[@name='params'])");
  }

  private void defaultEchoParamsDefaultVersion() {
    lrf.args.put("wt", "xml");
    lrf.args.remove(CommonParams.VERSION);    
    assertQ(req("foo"),HEADER_XPATH + "/int[@name='status']");
    assertQ(req("foo"),"not(//lst[@name='params'])");
  }

  private void explicitEchoParams() {
    lrf.args.put("wt", "xml");
    lrf.args.put(CommonParams.VERSION, "2.2");
    lrf.args.put("echoParams", "explicit");
    assertQ(req("foo"),HEADER_XPATH + "/int[@name='status']");
    assertQ(req("foo"),HEADER_XPATH + "/lst[@name='params']");
    assertQ(req("foo"),HEADER_XPATH + "/lst[@name='params']/str[@name='wt'][.='xml']");
  }

  private void allEchoParams() {
    lrf = h.getRequestFactory
      ("/crazy_custom_qt", 0, 20,
       CommonParams.VERSION,"2.2",
       "wt","xml",
       "echoParams", "all",
       "echoHandler","true"
       );

    assertQ(req("foo"),HEADER_XPATH + "/lst[@name='params']/str[@name='fl'][.='implicit']");
    assertQ(req("foo"),HEADER_XPATH + "/str[@name='handler'][.='org.apache.solr.handler.component.SearchHandler']");
  }

}
