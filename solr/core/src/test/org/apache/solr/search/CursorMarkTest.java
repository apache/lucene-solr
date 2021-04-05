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
package org.apache.solr.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.CursorPagingTest;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.junit.BeforeClass;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Primarily a test of parsing and serialization of the CursorMark values.
 *
 * NOTE: this class Reuses some utilities from {@link CursorPagingTest} that assume the same schema and configs.
 *
 * @see CursorPagingTest 
 */
public class CursorMarkTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("solr.test.useFilterForSortedQuery", Boolean.toString(random().nextBoolean()));
    initCore(CursorPagingTest.TEST_SOLRCONFIG_NAME, CursorPagingTest.TEST_SCHEMAXML_NAME);
  }

  public void testNextCursorMark() throws IOException {
    final Collection<String> allFieldNames = getAllFieldNames();
    final SolrQueryRequest req = req();
    final IndexSchema schema = req.getSchema();

    final String randomSortString = CursorPagingTest.buildRandomSort(allFieldNames);
    final SortSpec ss = SortSpecParsing.parseSortSpec(randomSortString, req);

    final CursorMark previous = new CursorMark(schema, ss);
    previous.parseSerializedTotem(CURSOR_MARK_START);

    List<Object> nextValues = Arrays.<Object>asList(buildRandomSortObjects(ss));
    final CursorMark next = previous.createNext(nextValues);
    assertEquals("next values not correct", nextValues, next.getSortValues());
    assertEquals("next SortSpec not correct", ss, next.getSortSpec());

    SolrException e = expectThrows(SolrException.class,
                                   "didn't fail on next with incorrect num of sortvalues",
                                   () -> {
        // append to our random sort string so we know it has wrong num clauses
        final SortSpec otherSort = SortSpecParsing.parseSortSpec(randomSortString+",id asc", req);
        CursorMark trash = previous.createNext(Arrays.<Object>asList
                                               (buildRandomSortObjects(otherSort)));
                                   });
    assertEquals(500, e.code());
    assertThat(e.getMessage(), containsString("sort values != sort length"));
  }

  public void testInvalidUsage() {
    final SolrQueryRequest req = req();
    final IndexSchema schema = req.getSchema();

    try {
      final SortSpec ss = SortSpecParsing.parseSortSpec("str desc, score desc", req);
      final CursorMark totem = new CursorMark(schema, ss);
      fail("no failure from sort that doesn't include uniqueKey field");
    } catch (SolrException e) {
      assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(0 < e.getMessage().indexOf("uniqueKey"));
    }

    for (final String dir : Arrays.asList("asc", "desc")) {
      try {
        final SortSpec ss = SortSpecParsing.parseSortSpec("score " + dir, req);
        final CursorMark totem = new CursorMark(schema, ss);
        fail("no failure from score only sort: " + dir);
      } catch (SolrException e) {
        assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue(0 < e.getMessage().indexOf("uniqueKey"));
      }
      
      try {
        final SortSpec ss = SortSpecParsing.parseSortSpec("_docid_ "+dir+", id desc", req);
        final CursorMark totem = new CursorMark(schema, ss);
        fail("no failure from sort that includes _docid_: " + dir);
      } catch (SolrException e) {
        assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue(0 < e.getMessage().indexOf("_docid_"));
      }
    }
  }


  public void testGarbageParsing() throws IOException {
    final SolrQueryRequest req = req();
    final IndexSchema schema = req.getSchema();
    final SortSpec ss = SortSpecParsing.parseSortSpec("str asc, float desc, id asc", req);
    final CursorMark totem = new CursorMark(schema, ss);

    // totem string that isn't even valid base64
    try {
      totem.parseSerializedTotem("all the documents please");
      fail("didn't fail on invalid base64 totem");
    } catch (SolrException e) {
      assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(e.getMessage().contains("Unable to parse 'cursorMark'"));
    }

    // empty totem string
    try {
      totem.parseSerializedTotem("");
      fail("didn't fail on empty totem");
    } catch (SolrException e) {
      assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(e.getMessage().contains("Unable to parse 'cursorMark'"));
    }

    // whitespace-only totem string
    try {
      totem.parseSerializedTotem("       ");
      fail("didn't fail on whitespace-only totem");
    } catch (SolrException e) {
      assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(e.getMessage().contains("Unable to parse 'cursorMark'"));
    }

    // totem string from sort with diff num clauses
    try {
      final SortSpec otherSort = SortSpecParsing.parseSortSpec("double desc, id asc", req);
      final CursorMark otherTotem = new CursorMark(schema, otherSort);
      otherTotem.setSortValues(Arrays.<Object>asList(buildRandomSortObjects(otherSort)));
      
      totem.parseSerializedTotem(otherTotem.getSerializedTotem());
      fail("didn't fail on totem from incorrect sort (num clauses)");
    } catch (SolrException e) {
      assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(e.getMessage().contains("wrong size"));
    }
  }

  public void testRoundTripParsing() throws IOException {

    // for any valid SortSpec, and any legal values, we should be able to round 
    // trip serialize the totem and get the same values back.

    final Collection<String> allFieldNames = getAllFieldNames();
    final SolrQueryRequest req = req();
    final IndexSchema schema = req.getSchema();
    final int numRandomSorts = atLeast(50);
    final int numRandomValIters = atLeast(10);
    for (int i = 0; i < numRandomSorts; i++) {
      final SortSpec ss = SortSpecParsing.parseSortSpec
        (CursorPagingTest.buildRandomSort(allFieldNames), req);
      final CursorMark totemIn = new CursorMark(schema, ss);
      final CursorMark totemOut = new CursorMark(schema, ss);

      // trivial case: regardless of sort, "*" should be valid and roundtrippable
      totemIn.parseSerializedTotem(CURSOR_MARK_START);
      assertEquals(CURSOR_MARK_START, totemIn.getSerializedTotem());
      // values should be null (and still roundtrippable)
      assertNull(totemIn.getSortValues());
      totemOut.setSortValues(null);
      assertEquals(CURSOR_MARK_START, totemOut.getSerializedTotem());

      for (int j = 0; j < numRandomValIters; j++) {
        final Object[] inValues = buildRandomSortObjects(ss);
        totemIn.setSortValues(Arrays.<Object>asList(inValues));
        totemOut.parseSerializedTotem(totemIn.getSerializedTotem());
        final List<Object> out = totemOut.getSortValues();
        assertNotNull(out);
        final Object[] outValues = out.toArray();
        assertArrayEquals(inValues, outValues);
      }
    }
  }

  private static Object[] buildRandomSortObjects(SortSpec ss) throws IOException {
    List<SchemaField> fields = ss.getSchemaFields();
    assertNotNull(fields);
    Object[] results = new Object[fields.size()];
    for (int i = 0; i < results.length; i++) {
      SchemaField sf = fields.get(i);
      if (null == sf) {
        // score or function
        results[i] = (Float) random().nextFloat() * random().nextInt(); break;
      } else if (0 == TestUtil.nextInt(random(), 0, 7)) {
        // emulate missing value for doc
        results[i] = null;
      } else {
        final String fieldName = sf.getName();
        assertNotNull(fieldName);

        // Note: In some cases we build a human readable version of the sort value and then 
        // unmarshall it into the raw, real, sort values that are expected by the FieldTypes.
        // In other cases we just build the raw value to begin with because it's easier

        Object val = null;
        if (fieldName.equals("id")) {
          val = sf.getType().unmarshalSortValue(TestUtil.randomSimpleString(random()));
        } else if (fieldName.startsWith("str")) {
          val = sf.getType().unmarshalSortValue(TestUtil.randomRealisticUnicodeString(random()));
        } else if (fieldName.startsWith("bin")) {
          byte[] randBytes = new byte[TestUtil.nextInt(random(), 1, 50)];
          random().nextBytes(randBytes);
          val = new BytesRef(randBytes);
        } else if (fieldName.contains("int")) {
          val = random().nextInt();
        } else if (fieldName.contains("long")) {
          val = random().nextLong();
        } else if (fieldName.contains("float")) {
          val = random().nextFloat() * random().nextInt();
        } else if (fieldName.contains("double")) {
          val = random().nextDouble() * random().nextInt();
        } else if (fieldName.contains("date")) {
          val = random().nextLong();
        } else if (fieldName.startsWith("currency")) {
          val = random().nextDouble();
        } else if (fieldName.startsWith("uuid")) {
          val = sf.getType().unmarshalSortValue(UUID.randomUUID().toString());
        } else if (fieldName.startsWith("bool")) {
          val = sf.getType().unmarshalSortValue(random().nextBoolean() ? "t" : "f");
        } else if (fieldName.startsWith("enum")) {
          val = random().nextInt(CursorPagingTest.SEVERITY_ENUM_VALUES.length);
        } else if (fieldName.contains("collation")) {
          val = getRandomCollation(sf);
        } else {
          fail("fell through the rabbit hole, new field in schema? = " + fieldName);
        }
        
        results[i] = val;

      }
    }
    return results;
  }

  private static Object getRandomCollation(SchemaField sf) throws IOException {
    Object val;
    Analyzer analyzer = sf.getType().getIndexAnalyzer();
    String term = TestUtil.randomRealisticUnicodeString(random());
    try (TokenStream ts = analyzer.tokenStream("fake", term)) {
      TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
      ts.reset();
      assertTrue(ts.incrementToken());
      val = BytesRef.deepCopyOf(termAtt.getBytesRef());
      assertFalse(ts.incrementToken());
      ts.end();
    }
    return val;
  }
  
  /**
   * a list of the fields in the schema - excluding _version_
   */
  private Collection<String> getAllFieldNames() {
    ArrayList<String> names = new ArrayList<>(37);
    for (String f : h.getCore().getLatestSchema().getFields().keySet()) {
      if (! f.equals("_version_")) {
        names.add(f);
      }
    }
    return Collections.<String>unmodifiableCollection(names);
  }


}
