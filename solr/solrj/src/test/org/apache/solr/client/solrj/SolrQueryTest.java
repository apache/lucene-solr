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
package org.apache.solr.client.solrj;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import junit.framework.Assert;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;

/**
 * 
 *
 * @since solr 1.3
 */
public class SolrQueryTest extends LuceneTestCase {
  
  public void testSolrQueryMethods() {
    SolrQuery q = new SolrQuery("dog");
    boolean b = false;
    
    q.setFacetLimit(10);
    q.addFacetField("price");
    q.addFacetField("state");
    Assert.assertEquals(q.getFacetFields().length, 2);
    q.addFacetQuery("instock:true");
    q.addFacetQuery("instock:false");
    q.addFacetQuery("a:b");
    Assert.assertEquals(q.getFacetQuery().length, 3);
    
    b = q.removeFacetField("price");
    Assert.assertEquals(b, true);
    b = q.removeFacetField("price2");
    Assert.assertEquals(b, false);
    b = q.removeFacetField("state");
    Assert.assertEquals(b, true);
    Assert.assertEquals(null, q.getFacetFields());
    
    b = q.removeFacetQuery("instock:true");
    Assert.assertEquals(b, true);
    b = q.removeFacetQuery("instock:false");
    b = q.removeFacetQuery("a:c");
    Assert.assertEquals(b, false);
    b = q.removeFacetQuery("a:b");
    Assert.assertEquals(null, q.getFacetQuery());   
    
    q.addSort("price", SolrQuery.ORDER.asc);
    q.addSort("date", SolrQuery.ORDER.desc);
    q.addSort("qty", SolrQuery.ORDER.desc);
    q.removeSort(new SortClause("date", SolrQuery.ORDER.desc));
    Assert.assertEquals(2, q.getSorts().size());
    q.removeSort(new SortClause("price", SolrQuery.ORDER.asc));
    q.removeSort(new SortClause("qty", SolrQuery.ORDER.desc));
    Assert.assertEquals(0, q.getSorts().size());
    
    q.addHighlightField("hl1");
    q.addHighlightField("hl2");
    q.setHighlightSnippets(2);
    Assert.assertEquals(2, q.getHighlightFields().length);
    Assert.assertEquals(100, q.getHighlightFragsize());
    Assert.assertEquals(q.getHighlightSnippets(), 2);
    q.removeHighlightField("hl1");
    q.removeHighlightField("hl3");
    Assert.assertEquals(1, q.getHighlightFields().length);
    q.removeHighlightField("hl2");
    Assert.assertEquals(null, q.getHighlightFields());
    
    // check to see that the removes are properly clearing the cgi params
    Assert.assertEquals(q.toString(), "q=dog");

    //Add time allowed param
    q.setTimeAllowed(1000);
    Assert.assertEquals((Integer)1000, q.getTimeAllowed() );
    //Adding a null should remove it
    q.setTimeAllowed(null);
    Assert.assertEquals(null, q.getTimeAllowed() ); 
    
    // System.out.println(q);
  }

  /*
   *  Verifies that you can use removeSortField() twice, which
   *  did not work in 4.0
   */
  public void testSortFieldRemoveAfterRemove() {
    SolrQuery q = new SolrQuery("dog");
    q.addSort("price", SolrQuery.ORDER.asc);
    q.addSort("date", SolrQuery.ORDER.desc);
    q.addSort("qty", SolrQuery.ORDER.desc);
    q.removeSort("date");
    Assert.assertEquals(2, q.getSorts().size());
    q.removeSort("qty");
    Assert.assertEquals(1, q.getSorts().size());
  }

  /*
   * Verifies that you can remove the last sort field, which
   * did not work in 4.0
   */
  public void testSortFieldRemoveLast() {
    SolrQuery q = new SolrQuery("dog");
    q.addSort("date", SolrQuery.ORDER.desc);
    q.addSort("qty", SolrQuery.ORDER.desc);
    q.removeSort("qty");
    Assert.assertEquals("date desc", q.getSortField());
  }

  /*
   * Verifies that getSort() returns an immutable map,
   * for both empty and non-empty situations
   */
  public void testGetSortImmutable() {
    SolrQuery q = new SolrQuery("dog");

    try {
      q.getSorts().add(new SortClause("price",  SolrQuery.ORDER.asc));
      fail("The returned (empty) map should be immutable; put() should fail!");
    } catch (UnsupportedOperationException uoe) {
      // pass
    }

    q.addSort("qty", SolrQuery.ORDER.desc);
    try {
      q.getSorts().add(new SortClause("price",  SolrQuery.ORDER.asc));
      fail("The returned (non-empty) map should be immutable; put() should fail!");
    } catch (UnsupportedOperationException uoe) {
      // pass
    }

    // Should work even when setSorts passes an Immutable List
    q.setSorts(Arrays.asList(new SortClause("price",  SolrQuery.ORDER.asc)));
    q.addSort(new SortClause("price",  SolrQuery.ORDER.asc));
  }

  public void testSortClause() {
    new SolrQuery.SortClause("rating", SolrQuery.ORDER.desc);
    new SolrQuery.SortClause("rating", SolrQuery.ORDER.valueOf("desc"));
    new SolrQuery.SortClause("rating", SolrQuery.ORDER.valueOf("desc"));
    SolrQuery.SortClause.create("rating", SolrQuery.ORDER.desc);
    SolrQuery.SortClause.create("rating", SolrQuery.ORDER.desc);
    SolrQuery.SortClause.create("rating", SolrQuery.ORDER.desc);

    SolrQuery.SortClause sc1a = SolrQuery.SortClause.asc("sc1");
    SolrQuery.SortClause sc1b = SolrQuery.SortClause.asc("sc1");
    Assert.assertEquals(sc1a, sc1b);
    Assert.assertEquals(sc1a.hashCode(), sc1b.hashCode());

    SolrQuery.SortClause sc2a = SolrQuery.SortClause.asc("sc2");
    SolrQuery.SortClause sc2b = SolrQuery.SortClause.desc("sc2");
    Assert.assertFalse(sc2a.equals(sc2b));

    SolrQuery.SortClause sc3a = SolrQuery.SortClause.asc("sc2");
    SolrQuery.SortClause sc3b = SolrQuery.SortClause.asc("not sc2");
    Assert.assertFalse(sc3a.equals(sc3b));
  }

  /*
   * Verifies the symbolic sort operations
   */
  public void testSort() throws IOException {

    SolrQuery q = new SolrQuery("dog");

    // Simple adds
    q.addSort("price", SolrQuery.ORDER.asc);
    q.addSort("date", SolrQuery.ORDER.desc);
    q.addSort("qty", SolrQuery.ORDER.desc);
    Assert.assertEquals(3, q.getSorts().size());
    Assert.assertEquals("price asc,date desc,qty desc", q.get(CommonParams.SORT));

    // Remove one (middle)
    q.removeSort("date");
    Assert.assertEquals(2, q.getSorts().size());
    Assert.assertEquals("price asc,qty desc", q.get(CommonParams.SORT));

    // Remove remaining (last, first)
    q.removeSort("price");
    q.removeSort("qty");
    Assert.assertTrue(q.getSorts().isEmpty());
    Assert.assertNull(q.get(CommonParams.SORT));

    // Clear sort
    q.addSort("price", SolrQuery.ORDER.asc);
    q.clearSorts();
    Assert.assertTrue(q.getSorts().isEmpty());
    Assert.assertNull(q.get(CommonParams.SORT));

    // Add vs update
    q.clearSorts();
    q.addSort("1", SolrQuery.ORDER.asc);
    q.addSort("2", SolrQuery.ORDER.asc);
    q.addSort("3", SolrQuery.ORDER.asc);
    q.addOrUpdateSort("2", SolrQuery.ORDER.desc);
    q.addOrUpdateSort("4", SolrQuery.ORDER.desc);
    Assert.assertEquals("1 asc,2 desc,3 asc,4 desc", q.get(CommonParams.SORT));

    // Using SortClause
    q.clearSorts();
    q.addSort(new SortClause("1", SolrQuery.ORDER.asc));
    q.addSort(new SortClause("2", SolrQuery.ORDER.asc));
    q.addSort(new SortClause("3", SolrQuery.ORDER.asc));
    q.addOrUpdateSort(SortClause.desc("2"));
    q.addOrUpdateSort(SortClause.asc("4"));
    Assert.assertEquals("1 asc,2 desc,3 asc,4 asc", q.get(CommonParams.SORT));
    q.setSort(SortClause.asc("A"));
    q.addSort(SortClause.asc("B"));
    q.addSort(SortClause.asc("C"));
    q.addSort(SortClause.asc("D"));
    Assert.assertEquals("A asc,B asc,C asc,D asc", q.get(CommonParams.SORT));

    // removeSort should ignore the ORDER
    q.setSort(SortClause.asc("A"));
    q.addSort(SortClause.asc("B"));
    q.addSort(SortClause.asc("C"));
    q.addSort(SortClause.asc("D"));
    q.removeSort("A");
    q.removeSort(SortClause.asc("C"));
    q.removeSort(SortClause.desc("B"));
    Assert.assertEquals("D asc", q.get(CommonParams.SORT));

    // Verify that a query containing a SortClause is serializable
    q.clearSorts();
    q.addSort("1", SolrQuery.ORDER.asc);
    ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream());
    out.writeObject(q);
    out.close();
  }

  public void testFacetSort() {
    SolrQuery q = new SolrQuery("dog");
    assertEquals("count", q.getFacetSortString());
    q.setFacetSort("index");
    assertEquals("index", q.getFacetSortString());
  }

  public void testFacetSortLegacy() {
    SolrQuery q = new SolrQuery("dog");
    assertEquals("expected default value to be SORT_COUNT", FacetParams.FACET_SORT_COUNT, q.getFacetSortString());
    q.setFacetSort(FacetParams.FACET_SORT_INDEX);
    assertEquals("expected set value to be SORT_INDEX", FacetParams.FACET_SORT_INDEX, q.getFacetSortString());
  }

  public void testFacetNumericRange() {
    SolrQuery q = new SolrQuery("dog");
    q.addNumericRangeFacet("field", 1, 10, 1);
    assertEquals("true", q.get(FacetParams.FACET));
    assertEquals("field", q.get(FacetParams.FACET_RANGE));
    assertEquals("1", q.get("f.field." + FacetParams.FACET_RANGE_START));
    assertEquals("10", q.get("f.field." + FacetParams.FACET_RANGE_END));
    assertEquals("1", q.get("f.field." + FacetParams.FACET_RANGE_GAP));

    q = new SolrQuery("dog");
    q.addNumericRangeFacet("field", 1.0d, 10.0d, 1.0d);
    assertEquals("true", q.get(FacetParams.FACET));
    assertEquals("field", q.get(FacetParams.FACET_RANGE));
    assertEquals("1.0", q.get("f.field." + FacetParams.FACET_RANGE_START));
    assertEquals("10.0", q.get("f.field." + FacetParams.FACET_RANGE_END));
    assertEquals("1.0", q.get("f.field." + FacetParams.FACET_RANGE_GAP));

    q = new SolrQuery("dog");
    q.addNumericRangeFacet("field", 1.0f, 10.0f, 1.0f);
    assertEquals("true", q.get(FacetParams.FACET));
    assertEquals("field", q.get(FacetParams.FACET_RANGE));
    assertEquals("1.0", q.get("f.field." + FacetParams.FACET_RANGE_START));
    assertEquals("10.0", q.get("f.field." + FacetParams.FACET_RANGE_END));
    assertEquals("1.0", q.get("f.field." + FacetParams.FACET_RANGE_GAP));
  }

  public void testFacetDateRange() {
    SolrQuery q = new SolrQuery("dog");
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.UK);
    calendar.set(2010, 1, 1);
    Date start = calendar.getTime();
    calendar.set(2011, 1, 1);
    Date end = calendar.getTime();
    q.addDateRangeFacet("field", start, end, "+1MONTH");
    assertEquals("true", q.get(FacetParams.FACET));
    assertEquals("field", q.get(FacetParams.FACET_RANGE));
    assertEquals(start.toInstant().toString(), q.get("f.field." + FacetParams.FACET_RANGE_START));
    assertEquals(end.toInstant().toString(), q.get("f.field." + FacetParams.FACET_RANGE_END));
    assertEquals("+1MONTH", q.get("f.field." + FacetParams.FACET_RANGE_GAP));
  }

  public void testSettersGetters() {
      SolrQuery q = new SolrQuery("foo");
      assertEquals(10, q.setFacetLimit(10).getFacetLimit());
      assertEquals(10, q.setFacetMinCount(10).getFacetMinCount());
      assertEquals("index", q.setFacetSort("index").getFacetSortString());
      assertEquals(10, q.setHighlightSnippets(10).getHighlightSnippets());
      assertEquals(10, q.setHighlightFragsize(10).getHighlightFragsize());
      assertEquals(true, q.setHighlightRequireFieldMatch(true).getHighlightRequireFieldMatch());
      assertEquals("foo", q.setHighlightSimplePre("foo").getHighlightSimplePre());
      assertEquals("foo", q.setHighlightSimplePost("foo").getHighlightSimplePost());
      assertEquals(true, q.setHighlight(true).getHighlight());
      assertEquals("foo", q.setQuery("foo").getQuery());
      assertEquals(10, q.setRows(10).getRows().intValue());
      assertEquals(10, q.setStart(10).getStart().intValue());
      assertEquals("foo", q.setRequestHandler("foo").getRequestHandler());
      assertEquals(10, q.setTimeAllowed(10).getTimeAllowed().intValue());
      
      // non-standard
      assertEquals("foo", q.setFacetPrefix("foo").get( FacetParams.FACET_PREFIX, null ) );
      assertEquals("foo", q.setFacetPrefix("a", "foo").getFieldParam( "a", FacetParams.FACET_PREFIX, null ) );

      assertEquals( Boolean.TRUE, q.setFacetMissing(Boolean.TRUE).getBool( FacetParams.FACET_MISSING ) );
      assertEquals( Boolean.FALSE, q.setFacetMissing( Boolean.FALSE ).getBool( FacetParams.FACET_MISSING ) );      
      assertEquals( "true", q.setParam( "xxx", true ).getParams( "xxx" )[0] );

      assertEquals( "x,y", q.setFields("x","y").getFields() );    
      assertEquals( "x,y,score", q.setIncludeScore(true).getFields() );
      assertEquals( "x,y,score", q.setIncludeScore(true).getFields() ); // set twice on purpose
      assertEquals( "x,y", q.setIncludeScore(false).getFields() );
      assertEquals( "x,y", q.setIncludeScore(false).getFields() ); // remove twice on purpose

  }
  
  public void testOrder() {
    assertEquals( SolrQuery.ORDER.asc, SolrQuery.ORDER.desc.reverse() );
    assertEquals( SolrQuery.ORDER.desc, SolrQuery.ORDER.asc.reverse() );
  }
  
  public void testTerms() {
    SolrQuery q = new SolrQuery();
    
    // check getters
    assertEquals(false, q.getTerms());
    assertArrayEquals(null, q.getTermsFields());
    assertEquals("", q.getTermsLower());
    assertEquals("", q.getTermsUpper());
    assertEquals(false, q.getTermsUpperInclusive());
    assertEquals(true, q.getTermsLowerInclusive());
    assertEquals(10, q.getTermsLimit());
    assertEquals(1, q.getTermsMinCount());
    assertEquals(-1, q.getTermsMaxCount());
    assertEquals("", q.getTermsPrefix());
    assertEquals(false, q.getTermsRaw());
    assertEquals("count", q.getTermsSortString());
    assertEquals(null, q.getTermsRegex());
    assertArrayEquals(null, q.getTermsRegexFlags());

    // check setters
    q.setTerms(true);
    assertEquals(true, q.getTerms());
    q.addTermsField("testfield");
    assertEquals(1, q.getTermsFields().length);
    assertEquals("testfield", q.getTermsFields()[0]);
    q.setTermsLower("lower");
    assertEquals("lower", q.getTermsLower());
    q.setTermsUpper("upper");
    assertEquals("upper", q.getTermsUpper());
    q.setTermsUpperInclusive(true);
    assertEquals(true, q.getTermsUpperInclusive());
    q.setTermsLowerInclusive(false);
    assertEquals(false, q.getTermsLowerInclusive());
    q.setTermsLimit(5);
    assertEquals(5, q.getTermsLimit());
    q.setTermsMinCount(2);
    assertEquals(2, q.getTermsMinCount());
    q.setTermsMaxCount(5);
    assertEquals(5, q.getTermsMaxCount());
    q.setTermsPrefix("prefix");
    assertEquals("prefix", q.getTermsPrefix());
    q.setTermsRaw(true);
    assertEquals(true, q.getTermsRaw());
    q.setTermsSortString("index");
    assertEquals("index", q.getTermsSortString());
    q.setTermsRegex("a.*");
    assertEquals("a.*", q.getTermsRegex());
    q.setTermsRegexFlag("case_insensitive");
    q.setTermsRegexFlag("multiline");
    assertEquals(2, q.getTermsRegexFlags().length);
  }

  public void testAddFacetQuery() {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.addFacetQuery("field:value");
    assertTrue("Adding a Facet Query should enable facets", solrQuery.getBool(FacetParams.FACET));
  }
  
  public void testFacetInterval() {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.addIntervalFacets("field1", new String[]{});
    assertTrue(solrQuery.getBool(FacetParams.FACET));
    assertEquals("field1", solrQuery.get(FacetParams.FACET_INTERVAL));
    
    solrQuery.addIntervalFacets("field2", new String[]{"[1,10]"});
    assertArrayEquals(new String[]{"field1", "field2"}, solrQuery.getParams(FacetParams.FACET_INTERVAL));
    assertEquals("[1,10]", solrQuery.get("f.field2.facet.interval.set"));
    
    solrQuery.addIntervalFacets("field3", new String[]{"[1,10]", "(10,100]", "(100,1000]", "(1000,*]"});
    assertArrayEquals(new String[]{"field1", "field2", "field3"}, solrQuery.getParams(FacetParams.FACET_INTERVAL));
    assertArrayEquals(new String[]{"[1,10]", "(10,100]", "(100,1000]", "(1000,*]"}, solrQuery.getParams("f.field3.facet.interval.set"));
    
    //Validate adding more intervals for an existing field
    solrQuery.addIntervalFacets("field2", new String[]{"[10,100]"});
    assertArrayEquals(new String[]{"[1,10]", "[10,100]"}, solrQuery.getParams("f.field2.facet.interval.set"));
    
    assertNull(solrQuery.removeIntervalFacets("field1"));
    assertArrayEquals(new String[]{"field2", "field3", "field2"}, solrQuery.getParams(FacetParams.FACET_INTERVAL));
    assertNull(solrQuery.getParams("f.field1.facet.interval.set"));
    
    assertArrayEquals(new String[]{"[1,10]", "[10,100]"}, solrQuery.removeIntervalFacets("field2"));
    assertArrayEquals(new String[]{"field3"}, solrQuery.getParams(FacetParams.FACET_INTERVAL));
    assertNull(solrQuery.getParams("f.field2.facet.interval.set"));
    
    assertArrayEquals(new String[]{"[1,10]", "(10,100]", "(100,1000]", "(1000,*]"}, solrQuery.removeIntervalFacets("field3"));
    assertNull(solrQuery.getParams(FacetParams.FACET_INTERVAL));
    assertNull(solrQuery.getParams("f.field3.facet.interval.set"));
    
  }

  public void testMoreLikeThis() {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.addMoreLikeThisField("mlt1");
    assertTrue(solrQuery.getMoreLikeThis());

    solrQuery.addMoreLikeThisField("mlt2");
    solrQuery.addMoreLikeThisField("mlt3");
    solrQuery.addMoreLikeThisField("mlt4");
    assertEquals(4, solrQuery.getMoreLikeThisFields().length);
    solrQuery.setMoreLikeThisFields((String[])null);
    assertTrue(null == solrQuery.getMoreLikeThisFields());
    assertFalse(solrQuery.getMoreLikeThis());

    assertEquals(true, solrQuery.setMoreLikeThisBoost(true).getMoreLikeThisBoost());
    assertEquals("qf", solrQuery.setMoreLikeThisQF("qf").getMoreLikeThisQF());
    assertEquals(10, solrQuery.setMoreLikeThisMaxTokensParsed(10).getMoreLikeThisMaxTokensParsed());
    assertEquals(11, solrQuery.setMoreLikeThisMinTermFreq(11).getMoreLikeThisMinTermFreq());
    assertEquals(12, solrQuery.setMoreLikeThisMinDocFreq(12).getMoreLikeThisMinDocFreq());
    assertEquals(13, solrQuery.setMoreLikeThisMaxWordLen(13).getMoreLikeThisMaxWordLen());
    assertEquals(14, solrQuery.setMoreLikeThisMinWordLen(14).getMoreLikeThisMinWordLen());
    assertEquals(15, solrQuery.setMoreLikeThisMaxQueryTerms(15).getMoreLikeThisMaxQueryTerms());
    assertEquals(16, solrQuery.setMoreLikeThisCount(16).getMoreLikeThisCount());

  }
}
