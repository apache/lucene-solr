/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.lucene.document.Document;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TaggerTestCase extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Rule
  public TestWatcher watchman = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      if (log.isInfoEnabled()) {
        log.info("{} being run...", description.getDisplayName());
      }
    }
  };

  protected final ModifiableSolrParams baseParams = new ModifiableSolrParams();

  //populated in buildNames; tested in assertTags
  protected static List<String> NAMES;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    baseParams.clear();
    baseParams.set(CommonParams.QT, "/tag");
    baseParams.set(CommonParams.WT, "xml");
  }

  protected void assertTags(String doc, String... tags) throws Exception {
    TestTag[] tts = new TestTag[tags.length];
    for (int i = 0; i < tags.length; i++) {
      tts[i] = tt(doc, tags[i]);
    }
    assertTags(reqDoc(doc), tts);
  }

  protected static void buildNames(String... names) throws Exception {
    deleteByQueryAndGetVersion("*:*", null);
    NAMES = Arrays.asList(names);
    //Collections.sort(NAMES);
    int i = 0;
    for (String n : NAMES) {
      assertU(adoc("id", ""+(i++), "name", n));
    }
    assertU(commit());
  }

  protected String lookupByName(String name) {
    for (String n : NAMES) {
      if (n.equalsIgnoreCase(name))
        return n;
    }
    return null;
  }

  protected TestTag tt(String doc, String substring) {
    int startOffset = -1, endOffset;
    int substringIndex = 0;
    for(int i = 0; i <= substringIndex; i++) {
      startOffset = doc.indexOf(substring,++startOffset);
      assert startOffset >= 0 : "The test itself is broken";
    }
    endOffset = startOffset+substring.length();//1 greater (exclusive)
    return new TestTag(startOffset, endOffset, substring, lookupByName(substring));
  }

  /** Asserts the tags.  Will call req.close(). */
  protected void assertTags(SolrQueryRequest req, TestTag... eTags) throws Exception {
    try {
      SolrQueryResponse rsp = h.queryAndResponse(req.getParams().get(CommonParams.QT), req);
      TestTag[] aTags = pullTagsFromResponse(req, rsp);

      String message;
      if (aTags.length > 10)
        message = null;
      else
        message = Arrays.asList(aTags).toString();
      Arrays.sort(eTags);
      assertSortedArrayEquals(message, eTags, aTags);

    } finally {
      req.close();
    }
  }

  @SuppressWarnings("unchecked")
  protected TestTag[] pullTagsFromResponse(SolrQueryRequest req, SolrQueryResponse rsp ) throws IOException {
    @SuppressWarnings({"rawtypes"})
    NamedList rspValues = rsp.getValues();
    Map<String, String> matchingNames = new HashMap<>();
    SolrIndexSearcher searcher = req.getSearcher();
    DocList docList = (DocList) rspValues.get("response");
    DocIterator iter = docList.iterator();
    while (iter.hasNext()) {
      int docId = iter.next();
      Document doc = searcher.doc(docId);
      String id = doc.getField("id").stringValue();
      String name = lookupByName(doc.get("name"));
      assertEquals("looking for "+name, NAMES.indexOf(name)+"", id);
      matchingNames.put(id, name);
    }

    //build TestTag[] aTags from response ('a' is actual)
    @SuppressWarnings({"rawtypes"})
    List<NamedList> mTagsList = (List<NamedList>) rspValues.get("tags");
    List<TestTag> aTags = new ArrayList<>();
    for (@SuppressWarnings({"rawtypes"})NamedList map : mTagsList) {
      List<String> foundIds = (List<String>) map.get("ids");
      for (String id  : foundIds) {
        aTags.add(new TestTag(
            ((Number)map.get("startOffset")).intValue(),
            ((Number)map.get("endOffset")).intValue(),
            null,
            matchingNames.get(id)));
      }
    }
    return aTags.toArray(new TestTag[0]);
  }

  /** REMEMBER to close() the result req object. */
  protected SolrQueryRequest reqDoc(String doc, String... moreParams) {
    return reqDoc(doc, params(moreParams));
  }

  /** REMEMBER to close() the result req object. */
  protected SolrQueryRequest reqDoc(String doc, SolrParams moreParams) {
    log.debug("Test doc: {}", doc);
    SolrParams params = SolrParams.wrapDefaults(moreParams, baseParams);
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(), params) {};
    Iterable<ContentStream> stream = Collections.singleton((ContentStream)new ContentStreamBase.StringStream(doc));
    req.setContentStreams(stream);
    return req;
  }

  /** Asserts the sorted arrays are equals, with a helpful error message when not.*/
  public void assertSortedArrayEquals(String message, Object[] expecteds, Object[] actuals) {
    AssertionError error = null;
    try {
      assertArrayEquals(null, expecteds, actuals);
    } catch (AssertionError e) {
      error = e;
    }
    if (error == null)
      return;
    TreeSet<Object> expectedRemaining = new TreeSet<>(Arrays.asList(expecteds));
    expectedRemaining.removeAll(Arrays.asList(actuals));
    if (!expectedRemaining.isEmpty())
      fail(message+": didn't find expected "+expectedRemaining.first()+" (of "+expectedRemaining.size()+"); "+ error);
    TreeSet<Object> actualsRemaining = new TreeSet<>(Arrays.asList(actuals));
    actualsRemaining.removeAll(Arrays.asList(expecteds));
    fail(message+": didn't expect "+actualsRemaining.first()+" (of "+actualsRemaining.size()+"); "+ error);
  }

  @SuppressWarnings({"rawtypes"})
  class TestTag implements Comparable {
    final int startOffset, endOffset;
    final String substring;
    final String docName;

    TestTag(int startOffset, int endOffset, String substring, String docName) {
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.substring = substring;
      this.docName = docName;
    }

    @Override
    public String toString() {
      return "TestTag{" +
          "[" + startOffset + "-" + endOffset + "]" +
          " doc=" + NAMES.indexOf(docName) + ":'" + docName + "'" +
          (docName.equals(substring) || substring == null ? "" : " substr="+substring)+
          '}';
    }

    @Override
    public boolean equals(Object obj) {
      TestTag that = (TestTag) obj;
      return new EqualsBuilder()
          .append(this.startOffset, that.startOffset)
          .append(this.endOffset, that.endOffset)
          .append(this.docName, that.docName)
          .isEquals();
    }

    @Override
    public int hashCode() {
      return startOffset;//cheesy but acceptable
    }

    @Override
    public int compareTo(Object o) {
      TestTag that = (TestTag) o;
      return new CompareToBuilder()
          .append(this.startOffset, that.startOffset)
          .append(this.endOffset, that.endOffset)
          .append(this.docName,that.docName)
          .toComparison();
    }
  }
}
