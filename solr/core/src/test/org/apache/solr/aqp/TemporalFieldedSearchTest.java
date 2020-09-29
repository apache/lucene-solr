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

package org.apache.solr.aqp;

import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class merely demonstrates that fielded search on temporal fields through the advanced query parser supports
 * the standard lucene range syntax and Zulu time format.
 */
@SuppressWarnings("deprecation")
public class TemporalFieldedSearchTest extends AbstractAqpTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testExactDateQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "someDate_dt", "2019-03-17T14:21:39.345Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "someDate_dt", "2019-03-17T00:00:00Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "someDate_dt", "2019-03-17T00:00:00.001Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "someDate_dt", "2019-03-17T23:59:59.999Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "someDate_dt", "2019-03-18T00:00:00.000Z")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "id asc"));
    assertEquals(5, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "someDate_dt:[2019-03-17T00:00:00Z TO 2019-03-18T00:00:00Z]", "defType", "advanced"));
    assertEquals(5, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "2", "3", "4", "5");
    resp = getSolrClient().query(coll, params("q", "someDate_dt:[2019-03-17T00:00:00Z TO 2019-03-18T00:00:00Z}", "defType", "advanced"));
    assertEquals(4, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "2", "3", "4");
    resp = getSolrClient().query(coll, params("q", "someDate_dt:{2019-03-17T00:00:00Z TO 2019-03-18T00:00:00Z]", "defType", "advanced"));
    assertEquals(4, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "3", "4", "5");
    resp = getSolrClient().query(coll, params("q", "someDate_dt:{2019-03-17T00:00:00Z TO 2019-03-18T00:00:00Z}", "defType", "advanced"));
    assertEquals(3, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "3", "4");
    resp = getSolrClient().query(coll, params("q", "someDate_dt:\"2019-03-17T14:21:39.345Z\"", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));
  }

  /**
   * Queries involving dates require ISO/ZULU format
   */
  @Test(expected = org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException.class)
  public void testNonISOFails() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "someDate_dt", "2019-08-07T14:21:39.345Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "someDate_dt", "2019-08-07T14:21:39.346Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "someDate_dt", "2019-08-07T14:21:39.347Z")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "id asc"));
    assertEquals(3, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    getSolrClient().query(coll, params("q", "someDate_dt:7/8/2019", "defType", "advanced"));

  }

  /**
   * Queries involving dates require ISO/ZULU format
   */
  @Test(expected = org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException.class)
  public void testMissingTFails() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "someDate_dt", "2019-08-07T14:21:39.345Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "someDate_dt", "2019-08-07T14:21:39.346Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "someDate_dt", "2019-08-07T14:21:39.347Z")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "id asc"));
    assertEquals(3, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    getSolrClient().query(coll, params("q", "someDate_dt:2019-08-07 14:21:39.345Z", "defType", "advanced"));

  }
  /**
   * Queries involving dates require ISO/ZULU format
   */
  @Test(expected = org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException.class)
  public void testNonZTimezoneFails() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "someDate_dt", "2019-08-07T14:21:39.345Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "someDate_dt", "2019-08-07T14:21:39.346Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "someDate_dt", "2019-08-07T14:21:39.347Z")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "id asc"));
    assertEquals(3, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    getSolrClient().query(coll, params("q", "someDate_dt:2019-08-07T14:21:39.345EDT", "defType", "advanced"));

  }
  /**
   * Queries involving dates require ISO/ZULU format
   */
  @Test(expected = org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException.class)
  public void testUTCoffsetFails() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "someDate_dt", "2019-08-07T14:21:39.345Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "someDate_dt", "2019-08-07T14:21:39.346Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "someDate_dt", "2019-08-07T14:21:39.347Z")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "id asc"));
    assertEquals(3, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    getSolrClient().query(coll, params("q", "someDate_dt:2019-08-07T14:21:39.345UTC-5", "defType", "advanced"));

  }

  /**
   * Queries involving dates require ISO/ZULU format
   */
  @Test(expected = org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException.class)
  public void testMissingTZFails() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)
        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "someDate_dt", "2019-08-07T14:21:39.345Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "someDate_dt", "2019-08-07T14:21:39.346Z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "someDate_dt", "2019-08-07T14:21:39.347Z")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "id asc"));
    assertEquals(3, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    getSolrClient().query(coll, params("q", "someDate_dt:2019-08-07T14:21:39.345", "defType", "advanced"));

  }

  @Test()
  public void testDateMath() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)
        .process(getSolrClient());

    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);

    String nowUTC = "2019-08-07T14:21:39.345Z";
    LocalDateTime now = LocalDateTime.parse(nowUTC, dateFormatter);
    LocalDateTime midnightToday = now.truncatedTo(ChronoUnit.DAYS);
    LocalDateTime sixHoursAgo = now.minus(6, ChronoUnit.HOURS);
    LocalDateTime yesterday = now.minus(1, ChronoUnit.DAYS);
    LocalDateTime oneWeekAgo = now.minus(1, ChronoUnit.WEEKS);
    LocalDateTime oneMonthAgo = now.minus(1, ChronoUnit.MONTHS);
    LocalDateTime tomorrow = now.plus(1, ChronoUnit.DAYS);

    String solrNow = String.valueOf(now.toInstant(ZoneOffset.UTC).toEpochMilli());

    // index test documents
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "now", "someDate_dt", dateFormatter.format(now))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "midnightToday", "someDate_dt", dateFormatter.format(midnightToday))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "sixHoursAgo", "someDate_dt", dateFormatter.format(sixHoursAgo))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "yesterday", "someDate_dt", dateFormatter.format(yesterday))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneWeekAgo", "someDate_dt", dateFormatter.format(oneWeekAgo))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneMonthAgo", "someDate_dt", dateFormatter.format(oneMonthAgo))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneMonthMinusOneDayAgo", "someDate_dt", dateFormatter.format(oneMonthAgo.plus(1, ChronoUnit.DAYS)))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneMonthAndOneDayAgo", "someDate_dt", dateFormatter.format(oneMonthAgo.minus(1, ChronoUnit.DAYS)))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "tomorrow", "someDate_dt", dateFormatter.format(tomorrow))));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "someDate_dt desc"));
    assertEquals(9, resp.getResults().size());
    assertEquals("tomorrow", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "someDate_dt:NOW+1DAY", "NOW", solrNow, "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("tomorrow", resp.getResults().get(0).get("id"));

    resp = getSolrClient().query(coll, params("q", "someDate_dt:NOW-1DAY", "NOW", solrNow, "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("yesterday", resp.getResults().get(0).get("id"));

    resp = getSolrClient().query(coll, params("q", "someDate_dt:NOW-1DAYS", "NOW", solrNow, "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("yesterday", resp.getResults().get(0).get("id"));

    resp = getSolrClient().query(coll, params("q", "someDate_dt:NOW-7DAYS", "NOW", solrNow, "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("oneWeekAgo", resp.getResults().get(0).get("id"));

    resp = getSolrClient().query(coll, params("q", "someDate_dt:NOW-1MONTH", "NOW", solrNow, "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("oneMonthAgo", resp.getResults().get(0).get("id"));


    // date math also works with UTC date string
    resp = getSolrClient().query(coll, params("q", "someDate_dt:\"" + nowUTC + "-1DAY+1MONTH-1MONTH+1DAY\"",
        "sort", "someDate_dt desc", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("now", resp.getResults().get(0).get("id"));
  }

  @Test()
  public void testDateMathRanges() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)
        .process(getSolrClient());

    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);

    String nowUTC = "2019-08-07T14:21:39.345Z";
    LocalDateTime now = LocalDateTime.parse(nowUTC, dateFormatter);
    LocalDateTime midnightToday = now.truncatedTo(ChronoUnit.DAYS);
    LocalDateTime sixHoursAgo = now.minus(6, ChronoUnit.HOURS);
    LocalDateTime yesterday = now.minus(1, ChronoUnit.DAYS);
    LocalDateTime oneWeekAgo = now.minus(1, ChronoUnit.WEEKS);
    LocalDateTime oneMonthAgo = now.minus(1, ChronoUnit.MONTHS);
    LocalDateTime tomorrow = now.plus(1, ChronoUnit.DAYS);

    String solrNow = String.valueOf(now.toInstant(ZoneOffset.UTC).toEpochMilli());

    // index test documents
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "now", "someDate_dt", dateFormatter.format(now))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "midnightToday", "someDate_dt", dateFormatter.format(midnightToday))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "sixHoursAgo", "someDate_dt", dateFormatter.format(sixHoursAgo))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "yesterday", "someDate_dt", dateFormatter.format(yesterday))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneWeekAgo", "someDate_dt", dateFormatter.format(oneWeekAgo))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneMonthAgo", "someDate_dt", dateFormatter.format(oneMonthAgo))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneMonthMinusOneDayAgo", "someDate_dt", dateFormatter.format(oneMonthAgo.plus(1, ChronoUnit.DAYS)))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "oneMonthAndOneDayAgo", "someDate_dt", dateFormatter.format(oneMonthAgo.minus(1, ChronoUnit.DAYS)))));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "tomorrow", "someDate_dt", dateFormatter.format(tomorrow))));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "*:*", "sort", "someDate_dt desc"));
    assertEquals(9, resp.getResults().size());
    assertEquals("tomorrow", resp.getResults().get(0).get("id"));

    resp = getSolrClient().query(coll, params("q", "someDate_dt:[NOW TO NOW+1DAYS]", "NOW", solrNow,
        "sort", "someDate_dt desc", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    assertEquals("tomorrow", resp.getResults().get(0).get("id"));
    assertEquals("now", resp.getResults().get(1).get("id"));

    // matches all dates within current day
    resp = getSolrClient().query(coll, params("q", "someDate_dt:[NOW/DAY TO NOW/DAY+1DAYS]", "NOW", solrNow,
        "sort", "someDate_dt desc", "defType", "advanced"));
    assertEquals(3, resp.getResults().size());
    assertEquals("now", resp.getResults().get(0).get("id"));
    assertEquals("sixHoursAgo", resp.getResults().get(1).get("id"));
    assertEquals("midnightToday", resp.getResults().get(2).get("id"));

    // match all dates within current month
    resp = getSolrClient().query(coll, params("q", "someDate_dt:[NOW/MONTH TO NOW/MONTH+1MONTH]", "NOW", solrNow,
        "sort", "someDate_dt desc", "defType", "advanced"));
    assertEquals(5, resp.getResults().size());
    assertEquals("tomorrow", resp.getResults().get(0).get("id"));
    assertEquals("now", resp.getResults().get(1).get("id"));
    assertEquals("sixHoursAgo", resp.getResults().get(2).get("id"));
    assertEquals("midnightToday", resp.getResults().get(3).get("id"));
    assertEquals("yesterday", resp.getResults().get(4).get("id"));

    // midnight to midnight exclusive
    resp = getSolrClient().query(coll, params("q", "someDate_dt:{NOW/DAY TO NOW/DAY+1DAY]", "NOW", solrNow,
        "sort", "someDate_dt desc", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    assertEquals("now", resp.getResults().get(0).get("id"));
    assertEquals("sixHoursAgo", resp.getResults().get(1).get("id"));

  }
}
