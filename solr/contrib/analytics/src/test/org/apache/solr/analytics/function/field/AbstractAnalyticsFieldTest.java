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
package org.apache.solr.analytics.function.field;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.ExpressionFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractAnalyticsFieldTest extends SolrTestCaseJ4 {

  private static IndexSchema indexSchema;
  private static SolrIndexSearcher searcher;
  private static RefCounted<SolrIndexSearcher> ref;

  protected static Map<String,Integer> singleInts;
  protected static Map<String,Map<Integer,Integer>> multiInts;
  protected static Map<String,Long> singleLongs;
  protected static Map<String,Map<Long,Integer>> multiLongs;
  protected static Map<String,Float> singleFloats;
  protected static Map<String,Map<Float,Integer>> multiFloats;
  protected static Map<String,Double> singleDoubles;
  protected static Map<String,Map<Double,Integer>> multiDoubles;
  protected static Map<String,Long> singleDates;
  protected static Map<String,Map<Long,Integer>> multiDates;
  protected static Map<String,String> singleStrings;
  protected static Map<String,Map<String,Integer>> multiStrings;
  protected static Map<String,Boolean> singleBooleans;
  protected static Map<String,Map<Boolean,Integer>> multiBooleans;

  private static List<String> missingDocuments;

  @BeforeClass
  public static void createSchemaAndFields() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");

    singleInts = new HashMap<>();
    multiInts = new HashMap<>();
    singleLongs = new HashMap<>();
    multiLongs = new HashMap<>();
    singleFloats = new HashMap<>();
    multiFloats = new HashMap<>();
    singleDoubles = new HashMap<>();
    multiDoubles = new HashMap<>();
    singleDates = new HashMap<>();
    multiDates = new HashMap<>();
    singleStrings = new HashMap<>();
    multiStrings = new HashMap<>();
    singleBooleans = new HashMap<>();
    multiBooleans = new HashMap<>();

    missingDocuments = new ArrayList<>();

    assertU(adoc("id", "-2"));
    missingDocuments.add("-2");
    assertU(adoc("id", "5"));
    missingDocuments.add("5");
    for (int i = -1; i < 5; ++i) {
      assertU(adoc(
          "id", "" + i,

          "int_i_t", "" + i,
          "int_im_t", "" + i,
          "int_im_t", "" + (i + 10),
          "int_im_t", "" + (i + 10),
          "int_im_t", "" + (i + 20),

          "int_i_p", "" + i,
          "int_im_p", "" + i,
          "int_im_p", "" + (i + 10),
          "int_im_p", "" + (i + 10),
          "int_im_p", "" + (i + 20),

          "long_l_t", "" + i,
          "long_lm_t", "" + i,
          "long_lm_t", "" + (i + 10),
          "long_lm_t", "" + (i + 10),
          "long_lm_t", "" + (i + 20),

          "long_l_p", "" + i,
          "long_lm_p", "" + i,
          "long_lm_p", "" + (i + 10),
          "long_lm_p", "" + (i + 10),
          "long_lm_p", "" + (i + 20),

          "float_f_t", "" + (i + .75F),
          "float_fm_t", "" + (i + .75F),
          "float_fm_t", "" + (i + 10.75F),
          "float_fm_t", "" + (i + 10.75F),
          "float_fm_t", "" + (i + 20.75F),

          "float_f_p", "" + (i + .75F),
          "float_fm_p", "" + (i + .75F),
          "float_fm_p", "" + (i + 10.75F),
          "float_fm_p", "" + (i + 10.75F),
          "float_fm_p", "" + (i + 20.75F),

          "double_d_t", "" + (i + .5),
          "double_dm_t", "" + (i + .5),
          "double_dm_t", "" + (i + 10.5),
          "double_dm_t", "" + (i + 10.5),
          "double_dm_t", "" + (i + 20.5),

          "double_d_p", "" + (i + .5),
          "double_dm_p", "" + (i + .5),
          "double_dm_p", "" + (i + 10.5),
          "double_dm_p", "" + (i + 10.5),
          "double_dm_p", "" + (i + 20.5),

          "date_dt_t", (1800 + i) + "-12-31T23:59:59Z",
          "date_dtm_t", (1800 + i) + "-12-31T23:59:59Z",
          "date_dtm_t", (1800 + i + 10) + "-12-31T23:59:59Z",
          "date_dtm_t", (1800 + i + 10) + "-12-31T23:59:59Z",
          "date_dtm_t", (1800 + i + 20) + "-12-31T23:59:59Z",

          "date_dt_p", (1800 + i) + "-12-31T23:59:59Z",
          "date_dtm_p", (1800 + i) + "-12-31T23:59:59Z",
          "date_dtm_p", (1800 + i + 10) + "-12-31T23:59:59Z",
          "date_dtm_p", (1800 + i + 10) + "-12-31T23:59:59Z",
          "date_dtm_p", (1800 + i + 20) + "-12-31T23:59:59Z",

          "string_s", "abc" + i,
          "string_sm", "abc" + i,
          "string_sm", "def" + i,
          "string_sm", "def" + i,
          "string_sm", "ghi" + i,

          "boolean_b", Boolean.toString(i % 3 == 0),
          "boolean_bm", "false",
          "boolean_bm", "true",
          "boolean_bm", "false"
      ));

      singleInts.put(""+i, i);
      Map<Integer, Integer> ints = new HashMap<>();
      ints.put(i, 1);
      ints.put(i + 10, 2);
      ints.put(i + 20, 1);
      multiInts.put(""+i, ints);

      singleLongs.put(""+i, (long) i);
      Map<Long, Integer> longs = new HashMap<>();
      longs.put((long) i, 1);
      longs.put(i + 10L, 2);
      longs.put(i + 20L, 1);
      multiLongs.put(""+i, longs);

      singleFloats.put(""+i, i + .75F);
      Map<Float, Integer> floats = new HashMap<>();
      floats.put(i + .75F, 1);
      floats.put(i + 10.75F, 2);
      floats.put(i + 20.75F, 1);
      multiFloats.put(""+i, floats);

      singleDoubles.put(""+i, i + .5);
      Map<Double, Integer> doubles = new HashMap<>();
      doubles.put(i + .5, 1);
      doubles.put(i + 10.5, 2);
      doubles.put(i + 20.5, 1);
      multiDoubles.put(""+i, doubles);

      singleDates.put(""+i, Instant.parse((1800 + i) + "-12-31T23:59:59Z").toEpochMilli());
      Map<Long, Integer> dates = new HashMap<>();
      dates.put(Instant.parse((1800 + i) + "-12-31T23:59:59Z").toEpochMilli(), 1);
      dates.put(Instant.parse((1800 + i + 10) + "-12-31T23:59:59Z").toEpochMilli(), 2);
      dates.put(Instant.parse((1800 + i + 20) + "-12-31T23:59:59Z").toEpochMilli(), 1);
      multiDates.put(""+i, dates);

      singleStrings.put(""+i, "abc" + i);
      Map<String, Integer> strings = new HashMap<>();
      strings.put("abc" + i, 1);
      strings.put("def" + i, 2);
      strings.put("ghi" + i, 1);
      multiStrings.put(""+i, strings);

      singleBooleans.put(""+i, i % 3 == 0);
      Map<Boolean, Integer> booleans = new HashMap<>();
      booleans.put(true, 1);
      booleans.put(false, 2);
      multiBooleans.put(""+i, booleans);
    }
    assertU(commit());

    ref = h.getCore().getSearcher();
    searcher = ref.get();

    indexSchema = h.getCore().getLatestSchema();
  }

  protected ExpressionFactory getExpressionFactory() {
    ExpressionFactory fact = new ExpressionFactory(indexSchema);
    fact.startRequest();
    return fact;
  }

  @AfterClass
  public static void closeSearcher() throws IOException {
    if (null != ref) {
      ref.decref();
      ref = null;
    }
    indexSchema = null;
    searcher = null;
    ref = null;
  }

  protected <T> void checkSingleFieldValues(Map<String,T> expected, Map<String,T> found, Set<String> missing) {
    expected.forEach( (id, value) -> {
      assertTrue("Field does not contain value for id '" + id + "'.", found.containsKey(id));
      assertEquals(value, found.get(id));
    });
    assertEquals(expected.size(), found.size());

    missingDocuments.forEach( id -> assertTrue("Field does not have correct information for missing id '" + id + "'.", missing.contains(id)));
    assertEquals(missingDocuments.size(), missing.size());
  }

  protected <T> void checkMultiFieldValues(Map<String,Map<T,Integer>> expected, Map<String,Map<T,Integer>> found, Set<String> missing, boolean deduplicated) {
    expected.forEach( (id, expectedValues) -> {
      assertTrue("Field does not contain values for id '" + id + "'.", found.containsKey(id));
      Map<T,Integer> foundValues = found.get(id);
      expectedValues.forEach( (value, count) -> {
        assertTrue("Value '" + value + "' not found for document with id '" + id + "'.", foundValues.containsKey(value));
        if (deduplicated) {
          assertEquals(1, foundValues.get(value).intValue());
        } else {
          assertEquals(count, foundValues.get(value));
        }
      });
      assertEquals(expectedValues.size(), foundValues.size());
    });
    assertEquals(expected.size(), found.size());

    missingDocuments.forEach( id -> assertTrue("Field does not have correct information for missing id '" + id + "'.", missing.contains(id)));
    assertEquals(missingDocuments.size(), missing.size());
  }

  protected boolean emptyValueFound;

  protected Set<String> collectFieldValues(AnalyticsField testField, Predicate<String> valuesFiller) throws IOException {
    StringField idField = new StringField("id");
    Set<String> missing = new HashSet<>();

    List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();
    for (LeafReaderContext context : contexts) {
      testField.doSetNextReader(context);
      idField.doSetNextReader(context);
      Bits liveDocs = context.reader().getLiveDocs();
      for (int doc = 0; doc < context.reader().maxDoc(); doc++) {
        if (liveDocs != null && !liveDocs.get(doc)) {
          continue;
        }
        // Add a document to the statistics being generated
        testField.collect(doc);
        idField.collect(doc);

        String id = idField.getString();
        if (!valuesFiller.test(id)) {
          missing.add(id);
        }
      }
    }
    return missing;
  }
}
