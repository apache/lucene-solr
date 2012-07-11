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

import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.Set;

/**
 * Tests currency field type.
 */
public class CurrencyFieldTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testCurrencySchema() throws Exception {
    IndexSchema schema = h.getCore().getSchema();

    SchemaField amount = schema.getField("amount");
    assertNotNull(amount);
    assertTrue(amount.isPolyField());

    SchemaField[] dynFields = schema.getDynamicFieldPrototypes();
    boolean seenCurrency = false;
    boolean seenAmount = false;

    for (SchemaField dynField : dynFields) {
      if (dynField.getName().equals("*" + FieldType.POLY_FIELD_SEPARATOR + CurrencyField.FIELD_SUFFIX_CURRENCY)) {
        seenCurrency = true;
      }

      if (dynField.getName().equals("*" + FieldType.POLY_FIELD_SEPARATOR + CurrencyField.FIELD_SUFFIX_AMOUNT_RAW)) {
        seenAmount = true;
      }
    }

    assertTrue("Didn't find the expected currency code dynamic field", seenCurrency);
    assertTrue("Didn't find the expected value dynamic field", seenAmount);
  }

  @Test
  public void testCurrencyFieldType() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    SchemaField amount = schema.getField("amount");
    assertNotNull(amount);
    assertTrue("amount is not a poly field", amount.isPolyField());
    FieldType tmp = amount.getType();
    assertTrue(tmp instanceof CurrencyField);
    String currencyValue = "1.50,EUR";
    IndexableField[] fields = amount.createFields(currencyValue, 2);
    assertEquals(fields.length, 3);

    // First field is currency code, second is value, third is stored.
    for (int i = 0; i < 3; i++) {
      boolean hasValue = fields[i].readerValue() != null
              || fields[i].numericValue() != null
              || fields[i].stringValue() != null;
      assertTrue("Doesn't have a value: " + fields[i], hasValue);
    }

    assertEquals(schema.getFieldTypeByName("string").toExternal(fields[2]), "1.50,EUR");
    
    // A few tests on the provider directly
    ExchangeRateProvider p = ((CurrencyField) tmp).getProvider();
    Set<String> availableCurrencies = p.listAvailableCurrencies();
    assert(availableCurrencies.size() == 4);
    assert(p.reload() == true);
    assert(p.getExchangeRate("USD", "EUR") == 2.5);
  }

  @Test
  public void testMockExchangeRateProvider() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    SchemaField amount = schema.getField("mock_amount");

    // A few tests on the provider directly
    ExchangeRateProvider p = ((CurrencyField)amount.getType()).getProvider();
    Set<String> availableCurrencies = p.listAvailableCurrencies();
    assert(availableCurrencies.size() == 3);
    assert(p.reload() == true);
    assert(p.getExchangeRate("USD", "EUR") == 0.8);
  }

  @Test
  public void testCurrencyRangeSearch() throws Exception {
    for (int i = 1; i <= 10; i++) {
      assertU(adoc("id", "" + i, "amount", i + ",USD"));
    }

    assertU(commit());

    assertQ(req("fl", "*,score", "q",
            "amount:[2.00,USD TO 5.00,USD]"),
            "//*[@numFound='4']");

    assertQ(req("fl", "*,score", "q",
            "amount:[0.50,USD TO 1.00,USD]"),
            "//*[@numFound='1']");

    assertQ(req("fl", "*,score", "q",
            "amount:[24.00,USD TO 25.00,USD]"),
            "//*[@numFound='0']");

    // "GBP" currency code is 1/2 of a USD dollar, for testing.
    assertQ(req("fl", "*,score", "q",
            "amount:[0.50,GBP TO 1.00,GBP]"),
            "//*[@numFound='2']");

    // "EUR" currency code is 2.5X of a USD dollar, for testing.
    assertQ(req("fl", "*,score", "q",
            "amount:[24.00,EUR TO 25.00,EUR]"),
            "//*[@numFound='1']");

    // Slight asymmetric rate should work.
    assertQ(req("fl", "*,score", "q",
            "amount:[24.99,EUR TO 25.01,EUR]"),
            "//*[@numFound='1']");
  }

  @Test
  public void testCurrencyPointQuery() throws Exception {
    assertU(adoc("id", "" + 1, "amount", "10.00,USD"));
    assertU(adoc("id", "" + 2, "amount", "15.00,EUR"));
    assertU(commit());
    assertQ(req("fl", "*,score", "q", "amount:10.00,USD"), "//int[@name='id']='1'");
    assertQ(req("fl", "*,score", "q", "amount:9.99,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", "amount:10.01,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", "amount:15.00,EUR"), "//int[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", "amount:7.50,USD"), "//int[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", "amount:7.49,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", "amount:7.51,USD"), "//*[@numFound='0']");
  }

  @Ignore
  public void testPerformance() throws Exception {
    Random r = new Random();
    int initDocs = 200000;

    for (int i = 1; i <= initDocs; i++) {
      assertU(adoc("id", "" + i, "amount", (r.nextInt(10) + 1.00) + ",USD"));
      if (i % 1000 == 0)
        System.out.println(i);
    }

    assertU(commit());
    for (int i = 0; i < 1000; i++) {
      double lower = r.nextInt(10) + 1.00;
      assertQ(req("fl", "*,score", "q", "amount:[" +  lower + ",USD TO " + (lower + 10.00) + ",USD]"), "//*");
      assertQ(req("fl", "*,score", "q", "amount:[" +  lower + ",EUR TO " + (lower + 10.00) + ",EUR]"), "//*");
    }

    for (int j = 0; j < 3; j++) {
      long t1 = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        double lower = r.nextInt(10) + 1.00;
        assertQ(req("fl", "*,score", "q", "amount:[" +  lower + ",USD TO " + (lower + (9.99 - (j * 0.01))) + ",USD]"), "//*");
      }

      System.out.println(System.currentTimeMillis() - t1);
    }

    System.out.println("---");

    for (int j = 0; j < 3; j++) {
      long t1 = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        double lower = r.nextInt(10) + 1.00;
        assertQ(req("fl", "*,score", "q", "amount:[" +  lower + ",EUR TO " + (lower + (9.99 - (j * 0.01))) + ",EUR]"), "//*");
      }

      System.out.println(System.currentTimeMillis() - t1);
    }
  }

  @Test
  public void testCurrencySort() throws Exception {
    assertU(adoc("id", "" + 1, "amount", "10.00,USD"));
    assertU(adoc("id", "" + 2, "amount", "15.00,EUR"));
    assertU(adoc("id", "" + 3, "amount", "7.00,EUR"));
    assertU(adoc("id", "" + 4, "amount", "6.00,GBP"));
    assertU(adoc("id", "" + 5, "amount", "2.00,GBP"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*", "sort", "amount desc", "limit", "1"), "//int[@name='id']='4'");
    assertQ(req("fl", "*,score", "q", "*:*", "sort", "amount asc", "limit", "1"), "//int[@name='id']='3'");
  }

  @Test
  public void testMockFieldType() throws Exception {
    assertU(adoc("id", "1", "mock_amount", "1.00,USD"));
    assertU(adoc("id", "2", "mock_amount", "1.00,EUR"));
    assertU(adoc("id", "3", "mock_amount", "1.00,NOK"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "mock_amount:5.0,NOK"),   "//*[@numFound='1']", "//int[@name='id']='1'");
    assertQ(req("fl", "*,score", "q", "mock_amount:1.2,USD"), "//*[@numFound='1']",   "//int[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", "mock_amount:0.2,USD"), "//*[@numFound='1']",   "//int[@name='id']='3'");
    assertQ(req("fl", "*,score", "q", "mock_amount:99,USD"),  "//*[@numFound='0']");
  }
}
