package org.apache.solr.schema;
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
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Tests currency field type using FileExchangeRateProvider
 */
public class CurrencyFieldXmlFileTest extends AbstractCurrencyFieldTest {

  public String field() {
    return "amount";
  }

  @Test
  public void testAsymetricPointQuery() throws Exception {
    clearIndex();
    assertU(adoc("id", "" + 1, field(), "10.00,USD"));
    assertU(adoc("id", "" + 2, field(), "15.00,EUR"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", field()+":15.00,EUR"), "//int[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", field()+":7.50,USD"), "//int[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", field()+":7.49,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", field()+":7.51,USD"), "//*[@numFound='0']");
  }

}
