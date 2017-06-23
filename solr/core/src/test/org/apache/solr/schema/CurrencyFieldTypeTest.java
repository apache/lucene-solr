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

package org.apache.solr.schema;

import java.util.Arrays;
import java.util.Currency;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RTimer;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/** Tests CurrencyField and CurrencyFieldType. */
public class CurrencyFieldTypeTest extends SolrTestCaseJ4 {
  private final String fieldName;
  private final Class<? extends ExchangeRateProvider> expectedProviderClass;
  
  public CurrencyFieldTypeTest(String fieldName, Class<? extends ExchangeRateProvider> expectedProviderClass) {
    this.fieldName = fieldName;
    this.expectedProviderClass = expectedProviderClass;
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {"amount", FileExchangeRateProvider.class},    // CurrencyField
        {"mock_amount", MockExchangeRateProvider.class},                 // CurrencyField
        {"oer_amount", OpenExchangeRatesOrgProvider.class},              // CurrencyField
        {"amount_CFT", FileExchangeRateProvider.class},  // CurrencyFieldType
        {"mock_amount_CFT", MockExchangeRateProvider.class},               // CurrencyFieldType
        {"oer_amount_CFT", OpenExchangeRatesOrgProvider.class}             // CurrencyFieldType
      });
  }
  
  /**
   * "Assumes" that the specified list of currency codes are
   * supported in this JVM
   */
  public static void assumeCurrencySupport(String... codes) {
    try {
      // each JDK might have a diff list of supported currencies,
      // these are the ones needed for this test to work.
      for (String code : codes) {
        Currency obj = Currency.getInstance(code);
        assertNotNull(code, obj);
      }
    } catch (IllegalArgumentException e) {
      Assume.assumeNoException(e);
    }

  }
  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeCurrencySupport("USD", "EUR", "MXN", "GBP", "JPY", "NOK");
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testCurrencySchema() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();

    SchemaField amount = schema.getField(fieldName);
    assertNotNull(amount);
    assertTrue(amount.isPolyField());

    CurrencyFieldType type = (CurrencyFieldType)amount.getType();
    String currencyDynamicField
        = "*" + (type instanceof CurrencyField ? FieldType.POLY_FIELD_SEPARATOR : "") + type.fieldSuffixCurrency;
    String amountDynamicField 
        = "*" + (type instanceof CurrencyField ? FieldType.POLY_FIELD_SEPARATOR : "") + type.fieldSuffixAmountRaw;

    SchemaField[] dynFields = schema.getDynamicFieldPrototypes();
    boolean seenCurrency = false;
    boolean seenAmount = false;

    for (SchemaField dynField : dynFields) {
      if (dynField.getName().equals(amountDynamicField)) {
        seenAmount = true;
      }

      if (dynField.getName().equals(currencyDynamicField)) {
        seenCurrency = true;
      }
    }

    assertTrue("Didn't find the expected currency code dynamic field " + currencyDynamicField, seenCurrency);
    assertTrue("Didn't find the expected value dynamic field " + amountDynamicField, seenAmount);
  }

  @Test
  public void testCurrencyFieldType() throws Exception {
    assumeTrue("This test is only applicable to the XML file based exchange rate provider",
        expectedProviderClass.equals(FileExchangeRateProvider.class));

    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    SchemaField amount = schema.getField(fieldName);
    assertNotNull(amount);
    assertTrue(fieldName + " is not a poly field", amount.isPolyField());
    FieldType tmp = amount.getType();
    assertTrue(fieldName + " is not an instance of CurrencyFieldType", tmp instanceof CurrencyFieldType);
    String currencyValue = "1.50,EUR";
    List<IndexableField> fields = amount.createFields(currencyValue);
    assertEquals(fields.size(), 3);

    // First field is currency code, second is value, third is stored.
    for (int i = 0; i < 3; i++) {
      boolean hasValue = fields.get(i).readerValue() != null
              || fields.get(i).numericValue() != null
              || fields.get(i).stringValue() != null;
      assertTrue("Doesn't have a value: " + fields.get(i), hasValue);
    }

    assertEquals(schema.getFieldTypeByName("string").toExternal(fields.get(2)), "1.50,EUR");
    
    // A few tests on the provider directly
    ExchangeRateProvider p = ((CurrencyFieldType)tmp).getProvider();
    Set<String> availableCurrencies = p.listAvailableCurrencies();
    assertEquals(5, availableCurrencies.size());
    assertTrue(p.reload());
    assertEquals(2.5, p.getExchangeRate("USD", "EUR"), 0.00000000001);
  }

  @Test
  public void testMockExchangeRateProvider() throws Exception {
    assumeTrue("This test is only applicable to the mock exchange rate provider",
        expectedProviderClass.equals(MockExchangeRateProvider.class));
    
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    SchemaField field = schema.getField(fieldName);
    FieldType fieldType = field.getType();
    ExchangeRateProvider provider = ((CurrencyFieldType)fieldType).getProvider();

    // A few tests on the provider directly
    assertEquals(3, provider.listAvailableCurrencies().size());
    assertTrue(provider.reload());
    assertEquals(0.8, provider.getExchangeRate("USD", "EUR"), 0.00000000001);
  }

  @Test
  public void testCurrencyRangeSearch() throws Exception {
    assumeTrue("This test is only applicable to the XML file based exchange rate provider",
        expectedProviderClass.equals(FileExchangeRateProvider.class));
    
    clearIndex();
    final int emptyDocs = atLeast(50); // times 2
    final int negDocs = atLeast(5);
    
    assertU(adoc("id", "0", fieldName, "0,USD")); // 0
    // lots of docs w/o values
    for (int i = 100; i <= 100 + emptyDocs; i++) {
      assertU(adoc("id", "" + i));
    }
    // docs with values in ranges we'll query
    for (int i = 1; i <= 10; i++) {
      assertU(adoc("id", "" + i, fieldName, i + ",USD"));
    }
    // more docs w/o values
    for (int i = 500; i <= 500 + emptyDocs; i++) {
      assertU(adoc("id", "" + i));
    }
    // some negative values
    for (int i = -100; i > -100 - negDocs; i--) {
      assertU(adoc("id", "" + i, fieldName, i + ",USD"));
    }
    assertU(adoc("id", "40", fieldName, "0,USD")); // 0

    assertU(commit());

    assertQ(req("fl", "*,score", "q",
            fieldName+":[2.00,USD TO 5.00,USD]"),
            "//*[@numFound='4']");

    assertQ(req("fl", "*,score", "q",
            fieldName+":[0.50,USD TO 1.00,USD]"),
            "//*[@numFound='1']");

    assertQ(req("fl", "*,score", "q",
            fieldName+":[24.00,USD TO 25.00,USD]"),
            "//*[@numFound='0']");

    // "GBP" currency code is 1/2 of a USD dollar, for testing.
    assertQ(req("fl", "*,score", "q",
            fieldName+":[0.50,GBP TO 1.00,GBP]"),
            "//*[@numFound='2']");

    // "EUR" currency code is 2.5X of a USD dollar, for testing.
    assertQ(req("fl", "*,score", "q",
            fieldName+":[24.00,EUR TO 25.00,EUR]"),
            "//*[@numFound='1']");

    // Slight asymmetric rate should work.
    assertQ(req("fl", "*,score", "q",
            fieldName+":[24.99,EUR TO 25.01,EUR]"),
            "//*[@numFound='1']");
    
    // Open ended ranges without currency
    assertQ(req("fl", "*,score", "q",
            fieldName+":[* TO *]"),
            "//*[@numFound='" + (2 + 10 + negDocs) + "']");
    
    // Open ended ranges with currency
    assertQ(req("fl", "*,score", "q",
            fieldName+":[*,EUR TO *,EUR]"),
            "//*[@numFound='" + (2 + 10 + negDocs) + "']");

    // Open ended start range without currency
    assertQ(req("fl", "*,score", "q",
            fieldName+":[* TO 5,USD]"),
            "//*[@numFound='" + (2 + 5 + negDocs) + "']");

    // Open ended start range with currency (currency for the * won't matter)
    assertQ(req("fl", "*,score", "q",
            fieldName+":[*,USD TO 5,USD]"),
            "//*[@numFound='" + (2 + 5 + negDocs) + "']");

    // Open ended end range
    assertQ(req("fl", "*,score", "q",
            fieldName+":[3 TO *]"),
            "//*[@numFound='8']");
  }

  @Test
  public void testBogusCurrency() throws Exception {
    ignoreException("HOSS");

    // bogus currency
    assertQEx("Expected exception for invalid currency",
              req("fl", "*,score", "q",
                  fieldName+":[3,HOSS TO *]"),
              400);
  }

  @Test
  public void testCurrencyPointQuery() throws Exception {
    assumeTrue("This test is only applicable to the XML file based exchange rate provider",
        expectedProviderClass.equals(FileExchangeRateProvider.class));

    clearIndex();
    assertU(adoc("id", "" + 1, fieldName, "10.00,USD"));
    assertU(adoc("id", "" + 2, fieldName, "15.00,MXN"));
    assertU(commit());
    assertQ(req("fl", "*,score", "q", fieldName+":10.00,USD"), "//str[@name='id']='1'");
    assertQ(req("fl", "*,score", "q", fieldName+":9.99,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", fieldName+":10.01,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", fieldName+":15.00,MXN"), "//str[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", fieldName+":7.50,USD"), "//str[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", fieldName+":7.49,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", fieldName+":7.51,USD"), "//*[@numFound='0']");
  }

  @Ignore
  public void testPerformance() throws Exception {
    clearIndex();

    Random r = random();
    int initDocs = 200000;

    for (int i = 1; i <= initDocs; i++) {
      assertU(adoc("id", "" + i, fieldName, (r.nextInt(10) + 1.00) + ",USD"));
      if (i % 1000 == 0)
        System.out.println(i);
    }

    assertU(commit());
    for (int i = 0; i < 1000; i++) {
      double lower = r.nextInt(10) + 1.00;
      assertQ(req("fl", "*,score", "q", fieldName+":[" +  lower + ",USD TO " + (lower + 10.00) + ",USD]"), "//*");
      assertQ(req("fl", "*,score", "q", fieldName+":[" +  lower + ",EUR TO " + (lower + 10.00) + ",EUR]"), "//*");
    }

    for (int j = 0; j < 3; j++) {
      final RTimer timer = new RTimer();
      for (int i = 0; i < 1000; i++) {
        double lower = r.nextInt(10) + 1.00;
        assertQ(req("fl", "*,score", "q", fieldName+":[" +  lower + ",USD TO " + (lower + (9.99 - (j * 0.01))) + ",USD]"), "//*");
      }

      System.out.println(timer.getTime());
    }

    System.out.println("---");

    for (int j = 0; j < 3; j++) {
      final RTimer timer = new RTimer();
      for (int i = 0; i < 1000; i++) {
        double lower = r.nextInt(10) + 1.00;
        assertQ(req("fl", "*,score", "q", fieldName+":[" +  lower + ",EUR TO " + (lower + (9.99 - (j * 0.01))) + ",EUR]"), "//*");
      }

      System.out.println(timer.getTime());
    }
  }

  @Test
  public void testCurrencySort() throws Exception {
    assumeTrue("This test is only applicable to the XML file based exchange rate provider",
        expectedProviderClass.equals(FileExchangeRateProvider.class));

    clearIndex();

    assertU(adoc("id", "" + 1, fieldName, "10.00,USD"));
    assertU(adoc("id", "" + 2, fieldName, "15.00,EUR"));
    assertU(adoc("id", "" + 3, fieldName, "7.00,EUR"));
    assertU(adoc("id", "" + 4, fieldName, "6.00,GBP"));
    assertU(adoc("id", "" + 5, fieldName, "2.00,GBP"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*", "sort", fieldName+" desc", "limit", "1"), "//str[@name='id']='4'");
    assertQ(req("fl", "*,score", "q", "*:*", "sort", fieldName+" asc", "limit", "1"), "//str[@name='id']='3'");
  }

  public void testExpectedProvider() {
      SolrCore core = h.getCore();
      IndexSchema schema = core.getLatestSchema();
      SchemaField field = schema.getField(fieldName);
      FieldType fieldType = field.getType();
      ExchangeRateProvider provider = ((CurrencyFieldType)fieldType).getProvider();
      assertEquals(expectedProviderClass, provider.getClass());
    }
  
  public void testFunctionUsage() throws Exception {
    assumeTrue("This test is only applicable to the XML file based exchange rate provider",
        expectedProviderClass.equals(FileExchangeRateProvider.class));

    clearIndex();
    for (int i = 1; i <= 8; i++) {
      // "GBP" currency code is 1/2 of a USD dollar, for testing.
      assertU(adoc("id", "" + i, fieldName, (((float)i)/2) + ",GBP"));
    }
    for (int i = 9; i <= 11; i++) {
      assertU(adoc("id", "" + i, fieldName, i + ",USD"));
    }

    assertU(commit());

    // direct value source usage, gets "raw" form od default currency
    // default==USD, so raw==penies
    assertQ(req("fl", "id,func:field($f)",
                "f", fieldName,
                "q", "id:5"),
            "//*[@numFound='1']",
            "//doc/float[@name='func' and .=500]");
    assertQ(req("fl", "id,func:field($f)",
                "f", fieldName,
                "q", "id:10"),
            "//*[@numFound='1']",
            "//doc/float[@name='func' and .=1000]");
    assertQ(req("fl", "id,score,"+fieldName, 
                "q", "{!frange u=500}"+fieldName)
            ,"//*[@numFound='5']"
            ,"//str[@name='id']='1'"
            ,"//str[@name='id']='2'"
            ,"//str[@name='id']='3'"
            ,"//str[@name='id']='4'"
            ,"//str[@name='id']='5'"
            );
    assertQ(req("fl", "id,score,"+fieldName, 
                "q", "{!frange l=500 u=1000}"+fieldName)
            ,"//*[@numFound='6']"
            ,"//str[@name='id']='5'"
            ,"//str[@name='id']='6'"
            ,"//str[@name='id']='7'"
            ,"//str[@name='id']='8'"
            ,"//str[@name='id']='9'"
            ,"//str[@name='id']='10'"
            );

    // use the currency function to convert to default (USD)
    assertQ(req("fl", "id,func:currency($f)",
                "f", fieldName,
                "q", "id:10"),
            "//*[@numFound='1']",
            "//doc/float[@name='func' and .=10]");
    assertQ(req("fl", "id,func:currency($f)",
                "f", fieldName,
                "q", "id:5"),
            "//*[@numFound='1']",
            "//doc/float[@name='func' and .=5]");
    assertQ(req("fl", "id,score"+fieldName, 
                "f", fieldName,
                "q", "{!frange u=5}currency($f)")
            ,"//*[@numFound='5']"
            ,"//str[@name='id']='1'"
            ,"//str[@name='id']='2'"
            ,"//str[@name='id']='3'"
            ,"//str[@name='id']='4'"
            ,"//str[@name='id']='5'"
            );
    assertQ(req("fl", "id,score"+fieldName, 
                "f", fieldName,
                "q", "{!frange l=5 u=10}currency($f)")
            ,"//*[@numFound='6']"
            ,"//str[@name='id']='5'"
            ,"//str[@name='id']='6'"
            ,"//str[@name='id']='7'"
            ,"//str[@name='id']='8'"
            ,"//str[@name='id']='9'"
            ,"//str[@name='id']='10'"
            );
    
    // use the currency function to convert to MXN
    assertQ(req("fl", "id,func:currency($f,MXN)",
                "f", fieldName,
                "q", "id:5"),
            "//*[@numFound='1']",
            "//doc/float[@name='func' and .=10]");
    assertQ(req("fl", "id,func:currency($f,MXN)",
                "f", fieldName,
                "q", "id:10"),
            "//*[@numFound='1']",
            "//doc/float[@name='func' and .=20]");
    assertQ(req("fl", "*,score,"+fieldName, 
                "f", fieldName,
                "q", "{!frange u=10}currency($f,MXN)")
            ,"//*[@numFound='5']"
            ,"//str[@name='id']='1'"
            ,"//str[@name='id']='2'"
            ,"//str[@name='id']='3'"
            ,"//str[@name='id']='4'"
            ,"//str[@name='id']='5'"
            );
    assertQ(req("fl", "*,score,"+fieldName, 
                "f", fieldName,
                "q", "{!frange l=10 u=20}currency($f,MXN)")
            ,"//*[@numFound='6']"
            ,"//str[@name='id']='5'"
            ,"//str[@name='id']='6'"
            ,"//str[@name='id']='7'"
            ,"//str[@name='id']='8'"
            ,"//str[@name='id']='9'"
            ,"//str[@name='id']='10'"
            );

  }

  @Test
  public void testMockFieldType() throws Exception {
    assumeTrue("This test is only applicable to the mock exchange rate provider",
        expectedProviderClass.equals(MockExchangeRateProvider.class));

    clearIndex();

    assertU(adoc("id", "1", fieldName, "1.00,USD"));
    assertU(adoc("id", "2", fieldName, "1.00,EUR"));
    assertU(adoc("id", "3", fieldName, "1.00,NOK"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", fieldName+":5.0,NOK"),   "//*[@numFound='1']", "//str[@name='id']='1'");
    assertQ(req("fl", "*,score", "q", fieldName+":1.2,USD"), "//*[@numFound='1']",   "//str[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", fieldName+":0.2,USD"), "//*[@numFound='1']",   "//str[@name='id']='3'");
    assertQ(req("fl", "*,score", "q", fieldName+":99,USD"),  "//*[@numFound='0']");
  }

  @Test
  public void testAsymmetricPointQuery() throws Exception {
    assumeTrue("This test is only applicable to the XML file based exchange rate provider",
        expectedProviderClass.equals(FileExchangeRateProvider.class));

    clearIndex();
    assertU(adoc("id", "" + 1, fieldName, "10.00,USD"));
    assertU(adoc("id", "" + 2, fieldName, "15.00,EUR"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", fieldName+":15.00,EUR"), "//str[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", fieldName+":7.50,USD"), "//str[@name='id']='2'");
    assertQ(req("fl", "*,score", "q", fieldName+":7.49,USD"), "//*[@numFound='0']");
    assertQ(req("fl", "*,score", "q", fieldName+":7.51,USD"), "//*[@numFound='0']");
  }
}
