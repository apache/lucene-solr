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

package org.apache.solr.client.solrj.request.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.MapWriter;
import org.junit.Test;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Unit tests for {@link JsonQueryRequest}
 */
public class JsonQueryRequestUnitTest extends SolrTestCase {

  private static final boolean LEAVE_WHITESPACE = false;

  @Test
  public void testRejectsNullQueryString() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().setQuery((String)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsNullQueryMap() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().setQuery((Map<String, Object>)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsNullQueryMapWriter() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().setQuery((MapWriter)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testWritesProvidedQueryStringToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().setQuery("text:solr");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"query\":\"text:solr\""));
  }

  @Test
  public void testWritesProvidedQueryMapToJsonCorrectly() {
    final Map<String, Object> queryMap = new HashMap<>();
    final Map<String, Object> paramsMap = new HashMap<>();
    queryMap.put("lucene", paramsMap);
    paramsMap.put("df", "text");
    paramsMap.put("q", "*:*");
    final JsonQueryRequest request = new JsonQueryRequest().setQuery(queryMap);
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"query\":{\"lucene\":{\"q\":\"*:*\",\"df\":\"text\"}}"));
  }

  @Test
  public void testWritesProvidedQueryMapWriterToJsonCorrectly() {
    final MapWriter queryWriter = new MapWriter() {
      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("lucene", (MapWriter) ew1 -> {
          ew1.put("q", "*:*");
          ew1.put("df", "text");
        });
      }
    };
    final JsonQueryRequest request = new JsonQueryRequest().setQuery(queryWriter);
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"query\":{\"lucene\":{\"q\":\"*:*\",\"df\":\"text\"}}"));
  }

  @Test
  public void testRejectsInvalidFacetName() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withFacet(null, new HashMap<>());
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));

    thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withStatFacet(null, "avg(price)");
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsInvalidFacetMap() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withFacet("anyFacetName", (Map<String, Object>)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsNullFacetMapWriter() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withFacet("anyFacetName", (MapWriter)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsInvalidStatFacetString() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withStatFacet("anyFacetName", (String)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testWritesProvidedFacetMapToJsonCorrectly() {
    final Map<String, Object> categoryFacetMap = new HashMap<>();
    categoryFacetMap.put("type", "terms");
    categoryFacetMap.put("field", "category");
    final JsonQueryRequest request = new JsonQueryRequest().withFacet("top_categories", categoryFacetMap);
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"facet\":{\"top_categories\":{\"field\":\"category\",\"type\":\"terms\"}}"));
  }

  @Test
  public void testWritesProvidedFacetMapWriterToJsonCorrectly() {
    final MapWriter facetWriter = new MapWriter() {
      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("type", "terms");
        ew.put("field", "category");
      }
    };
    final JsonQueryRequest request = new JsonQueryRequest().withFacet("top_categories", facetWriter);
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"facet\":{\"top_categories\":{\"type\":\"terms\",\"field\":\"category\"}}"));
  }

  @Test
  public void testWritesProvidedStatFacetToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().withStatFacet("avg_price", "avg(price)");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"facet\":{\"avg_price\":\"avg(price)\"}"));
  }

  @Test
  public void testWritesMultipleFacetMapsToJsonCorrectly() {
    final Map<String, Object> facetMap1 = new HashMap<>();
    facetMap1.put("type", "terms");
    facetMap1.put("field", "a");
    final Map<String, Object> facetMap2 = new HashMap<>();
    facetMap2.put("type", "terms");
    facetMap2.put("field", "b");
    final JsonQueryRequest request = new JsonQueryRequest();

    request.withFacet("facet1", facetMap1);
    request.withFacet("facet2", facetMap2);
    final String requestBody = writeRequestToJson(request);

    assertThat(requestBody, containsString("\"facet\":{\"facet2\":{\"field\":\"b\",\"type\":\"terms\"},\"facet1\":{\"field\":\"a\",\"type\":\"terms\"}}"));
  }

  @Test
  public void testRejectsInvalidLimit() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().setLimit(-1);
    });
    assertThat(thrown.getMessage(),containsString("must be non-negative"));
  }

  @Test
  public void testWritesProvidedLimitToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().setLimit(5);
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"limit\":5"));
  }

  @Test
  public void testRejectsInvalidOffset() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().setOffset(-1);
    });
    assertThat(thrown.getMessage(),containsString("must be non-negative"));

  }

  @Test
  public void testWritesProvidedOffsetToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().setOffset(5);
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"offset\":5"));
  }

  @Test
  public void testRejectsInvalidSort() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().setSort(null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));

  }

  @Test
  public void testWritesProvidedSortToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().setSort("price asc");
    final String requestBody = writeRequestToJson(request, LEAVE_WHITESPACE);
    assertThat(requestBody, containsString("\"sort\":\"price asc"));
  }

  @Test
  public void testRejectsInvalidFilterString() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withFilter((String)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsInvalidFilterMap() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withFilter((Map<String,Object>)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testWritesProvidedFilterToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().withFilter("text:solr");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"filter\":[\"text:solr\"]"));
  }

  @Test
  public void testWritesMultipleProvidedFiltersToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().withFilter("text:solr").withFilter("text:lucene");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"filter\":[\"text:solr\",\"text:lucene\"]"));
  }

  @Test
  public void testRejectsInvalidFieldsIterable() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().returnFields((Iterable<String>)null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testWritesProvidedFieldsToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().returnFields("price");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"fields\":[\"price\"]"));
  }

  @Test
  public void testWritesMultipleProvidedFieldsToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().returnFields("price", "name");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"fields\":[\"price\",\"name\"]"));
  }

  @Test
  public void testRejectsInvalidMiscParamName() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withParam(null, "any-value");
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));
  }

  @Test
  public void testRejectsInvalidMiscParamValue() {
    Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new JsonQueryRequest().withParam("any-name", null);
    });
    assertThat(thrown.getMessage(),containsString("must be non-null"));

  }

  @Test
  public void testWritesMiscParamsToJsonCorrectly() {
    final JsonQueryRequest request = new JsonQueryRequest().withParam("fq", "inStock:true");
    final String requestBody = writeRequestToJson(request);
    assertThat(requestBody, containsString("\"params\":{\"fq\":\"inStock:true\"}"));
  }

  private String writeRequestToJson(JsonQueryRequest request, boolean trimWhitespace) {
    final RequestWriter.ContentWriter writer = request.getContentWriter(ClientUtils.TEXT_JSON);
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      writer.write(os);
      final String rawJsonString = new String(os.toByteArray(), StandardCharsets.UTF_8);
      // Trimming whitespace makes our assertions in these tests more stable (independent of JSON formatting) so we do
      // it by default.  But we leave the option open in case the JSON fields have spaces.
      if (trimWhitespace) {
        return rawJsonString.replaceAll("\n", "").replaceAll(" ","");
      } else {
        return rawJsonString;
      }
    } catch (IOException e) {
      /* Unreachable in practice, since we're not doing any I/O here */
      throw new RuntimeException(e);
    }
  }

  private String writeRequestToJson(JsonQueryRequest request) {
    return writeRequestToJson(request, true);
  }
}
