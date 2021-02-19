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

package org.apache.solr.prometheus.exporter;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.thisptr.jackson.jq.JsonQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.prometheus.utils.Helpers;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import static org.apache.solr.prometheus.exporter.MetricsConfiguration.xpathFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MetricsQueryTemplateTest {
  @Test
  public void testTemplatesApplyDuringInit() throws Exception {
    MetricsConfiguration config = Helpers.loadConfiguration("conf/test-config-with-templates.xml");
    List<MetricsQuery> metrics = config.getMetricsConfiguration();
    List<JsonQuery> jsonQueries = metrics.get(0).getJsonQueries();
    final int expectedJqQueriesInConfig = 6;
    assertEquals(expectedJqQueriesInConfig, jsonQueries.size());
    // JsonQuery does not implement an equals so use the string expression with whitespace collapsed to a single space
    for (int q = 0; q < jsonQueries.size(); q += 2) {
      String expected = jsonQueries.get(q + 1).toString().replaceAll("\\s+", " ").trim();
      String actual = jsonQueries.get(q).toString().replaceAll("\\s+", " ").trim();
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testTemplateRegexMatchAndApply() {
    final String[] matches = new String[]{
        "$jq:jvm-item(memory_bytes,select(.key | startswith(\"memory.total.\")),object.value,\n\nGAUGE)",
        "$jq:node( client_errors_total,     select(.key | endswith(\".clientErrors\")), count )",
        "$jq:node(time_seconds_total,\nselect(.key == \"UPDATE.updateHandler.autoCommits\"), ($object.value / 1000))   ",
        "$jq:core-query( 1minRate, select(.key | endswith(\".distrib.requestTimes\")) )"
    };
    final String[] expectedApply = new String[]{
        "memory_bytes, select(.key | startswith(\"memory.total.\")), $object.value, GAUGE",
        "client_errors_total, select(.key | endswith(\".clientErrors\")), $object.value.count, COUNTER",
        "time_seconds_total, select(.key == \"UPDATE.updateHandler.autoCommits\"), ($object.value / 1000), COUNTER",
        "1minRate, select(.key | endswith(\".distrib.requestTimes\")), $object.value[\"1minRate\"], COUNTER"
    };

    MetricsQueryTemplate template =
        new MetricsQueryTemplate("test", "{UNIQUE}, {KEYSELECTOR}, {METRIC}, {TYPE}", "COUNTER");

    for (int m = 0; m < matches.length; m++) {
      Optional<Matcher> maybeMatcher = MetricsQueryTemplate.matches(matches[m]);
      assertTrue(maybeMatcher.isPresent());
      String jsonQuery = template.applyTemplate(maybeMatcher.get());
      assertEquals(expectedApply[m], jsonQuery);
    }
  }

  @Test
  public void testQueryMetricTemplate() throws Exception {
    Document config =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(SolrTestCaseJ4.getFile("conf/test-config-with-templates.xml"));
    NodeList jqTemplates =
        (NodeList) (xpathFactory.newXPath()).evaluate("/config/jq-templates/template", config, XPathConstants.NODESET);
    assertNotNull(jqTemplates);
    assertTrue(jqTemplates.getLength() > 0);
    MetricsQueryTemplate coreQueryTemplate = MetricsConfiguration.loadJqTemplates(jqTemplates).get("core-query");
    assertNotNull(coreQueryTemplate);

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode parsedMetrics = objectMapper.readTree(SolrTestCaseJ4.getFile("query-metrics.json"));
    final String[] queryMetrics = new String[]{
        "$jq:core-query(1minRate, select(.key | endswith(\".distrib.requestTimes\")), 1minRate)",
        "$jq:core-query(p75_ms, select(.key | endswith(\".distrib.requestTimes\")), p75_ms)",
        "$jq:core-query(mean_rate, select(.key | endswith(\".distrib.requestTimes\")), meanRate)",
        "$jq:core-query(local_5minRate, select(.key | endswith(\".local.requestTimes\")), 5minRate)",
        "$jq:core-query(local_median_ms, select(.key | endswith(\".local.requestTimes\")), median_ms)",
        "$jq:core-query(local_p95_ms, select(.key | endswith(\".local.requestTimes\")), p95_ms)",
        "$jq:core-query(local_count, select(.key | endswith(\".local.requestTimes\")), count, COUNTER)"
    };

    final double[] expectedMetrics = new double[]{
        5.156897804421665,
        1.31788,
        0.0031956674240800156,
        0.030666407244305586,
        0.079579,
        0.105268,
        4712
    };

    for (int m = 0; m < queryMetrics.length; m++) {
      Optional<Matcher> maybe = MetricsQueryTemplate.matches(queryMetrics[m]);
      assertTrue(maybe.isPresent());
      Matcher matcher = maybe.get();
      JsonQuery jsonQuery = JsonQuery.compile(coreQueryTemplate.applyTemplate(matcher));
      List<JsonNode> results = jsonQuery.apply(parsedMetrics);
      assertNotNull(results);
      assertTrue(results.size() == 1);
      double value = results.get(0).get("value").doubleValue();
      assertEquals(expectedMetrics[m], value, 0.0001);
    }
  }
}
