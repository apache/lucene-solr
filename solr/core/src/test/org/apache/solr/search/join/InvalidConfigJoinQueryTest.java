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
package org.apache.solr.search.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class InvalidConfigJoinQueryTest extends SolrTestCaseJ4 {

  private int id = 0;

  @BeforeClass
  public static void before() throws Exception {
    System.setProperty("solr.filterCache.async", "false");
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test(expected = SolrException.class)
  public void testInvalidFilterConfig() throws Exception {
    clearIndex();

    List<SolrInputDocument> organisations = createOrgs(10);
    List<SolrInputDocument> locations = createLocations(organisations, 10);
    List<SolrInputDocument> persons = createPersons(3000);
    associate(locations, persons);

    for (SolrInputDocument org : organisations) {
      assertU(adoc(org));
    }

    for (SolrInputDocument loc : locations) {
      assertU(adoc(loc));
    }

    for (SolrInputDocument per : persons) {
      assertU(adoc(per));
    }

    assertU(commit());

    assertJQ(
        req("q", "{!join from=id to=locid_s v=$q1}", "q1", "type_s:loc AND id:{!join from=id to=perid_s v=$q2}", "q2",
            "name:per*", "fl", "id", "sort", "id asc"),
        "/response=={'numFound':10,'start':0,'numFoundExact':true,'docs':[{'id':'0'},{'id':'1'},{'id':'2'},{'id':'3'},{'id':'4'},{'id':'5'},{'id':'6'},{'id':'7'},{'id':'8'},{'id':'9'}]}");
  }

  private List<SolrInputDocument> createOrgs(final int size) {
    final List<SolrInputDocument> result = new ArrayList<>(size);

    for (int i = 0; i < size; i++) {
      final SolrInputDocument doc = new SolrInputDocument("id", String.valueOf(id++), "type_s", "org", "t_description",
          "organisation " + i, "name", "org" + i);
      result.add(doc);
    }

    return result;
  }

  private List<SolrInputDocument> createLocations(final List<SolrInputDocument> orgs,
      final int size) {
    final List<SolrInputDocument> result = new ArrayList<>(orgs.size() * size);

    for (final SolrInputDocument org : orgs) {
      for (int i = 0; i < size; i++) {
        final SolrInputDocument doc = new SolrInputDocument("id", String.valueOf(id++), "type_s", "loc",
            "t_description",
            "location " + i, "name", "loc" + i);
        doc.setField("orgid_s", org.getFieldValue("id"));
        org.addField("locid_s", doc.getFieldValue("id"));
        result.add(doc);
      }
    }

    return result;
  }

  private List<SolrInputDocument> createPersons(final int size) {
    final List<SolrInputDocument> result = new ArrayList<>(size);

    for (int i = 0; i < size; i++) {
      final SolrInputDocument doc = new SolrInputDocument("id", String.valueOf(id++), "type_s", "per", "t_description",
          "person " + i, "name", "per" + i);
      result.add(doc);
    }

    return result;
  }

  private void associate(final List<SolrInputDocument> locations,
      final List<SolrInputDocument> persons) {

    for (final SolrInputDocument loc : locations) {
      for (final SolrInputDocument per : persons) {
        loc.addField("perid_s", per.getFieldValue("id"));
      }
    }
  }
}
