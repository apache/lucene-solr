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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * The original test for {@link TaggerRequestHandler}.
 */
public class TaggerTest extends TaggerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tagger.xml", "schema-tagger.xml");
  }

  private void indexAndBuild() throws Exception {
    N[] names = N.values();
    String[] namesStrs = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      namesStrs[i] = names[i].getName();
    }
    buildNames(namesStrs);
  }

  /** Name corpus */
  enum N {
    //keep order to retain ord()
    London, London_Business_School, Boston, City_of_London,
    of, the//filtered out of the corpus by a custom query
    ;

    String getName() { return name().replace('_',' '); }
    static N lookupByName(String name) { return N.valueOf(name.replace(' ', '_')); }
    int getId() { return ordinal(); }
  }

  public void testFormat() throws Exception {
    baseParams.set("overlaps", "NO_SUB");
    indexAndBuild();

    String rspStr = _testFormatRequest(false);
    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<response>\n" +
        "\n" +
        "<int name=\"tagsCount\">1</int>\n" +
        "<arr name=\"tags\">\n" +
        "  <lst>\n" +
        "    <int name=\"startOffset\">0</int>\n" +
        "    <int name=\"endOffset\">22</int>\n" +
        "    <arr name=\"ids\">\n" +
        "      <str>1</str>\n" +
        "    </arr>\n" +
        "  </lst>\n" +
        "</arr>\n" +
        "<result name=\"response\" numFound=\"1\" start=\"0\" numFoundExact=\"true\">\n" +
        "  <doc>\n" +
        "    <str name=\"id\">1</str>\n" +
        "    <str name=\"name\">London Business School</str>\n" +
        "    <str name=\"_root_\">1</str></doc>\n" +
        "</result>\n" +
        "</response>\n";
    assertEquals(expected, rspStr);
  }

  public void testFormatMatchText() throws Exception {
    baseParams.set("overlaps", "NO_SUB");
    indexAndBuild();

    String rspStr = _testFormatRequest(true);
    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<response>\n" +
        "\n" +
        "<int name=\"tagsCount\">1</int>\n" +
        "<arr name=\"tags\">\n" +
        "  <lst>\n" +
        "    <int name=\"startOffset\">0</int>\n" +
        "    <int name=\"endOffset\">22</int>\n" +
        "    <str name=\"matchText\">london business school</str>\n" +
        "    <arr name=\"ids\">\n" +
        "      <str>1</str>\n" +
        "    </arr>\n" +
        "  </lst>\n" +
        "</arr>\n" +
        "<result name=\"response\" numFound=\"1\" start=\"0\" numFoundExact=\"true\">\n" +
        "  <doc>\n" +
        "    <str name=\"id\">1</str>\n" +
        "    <str name=\"name\">London Business School</str>\n" +
        "    <str name=\"_root_\">1</str></doc>\n" +
        "</result>\n" +
        "</response>\n";
    assertEquals(expected, rspStr);
  }

  private String _testFormatRequest(boolean matchText) throws Exception {
    String doc = "london business school";//just one tag
    SolrQueryRequest req = reqDoc(doc, "indent", "on", "omitHeader", "on", "matchText", ""+matchText);
    String rspStr = h.query(req);
    req.close();
    return rspStr;
  }

  /** Partial matching, no sub-tags */
  @Ignore //TODO ConcatenateGraphFilter uses a special separator char that we can't put into XML (invalid char)
  public void testPartialMatching() throws Exception {
    baseParams.set("field", "name_tagPartial");
    baseParams.set("overlaps", "NO_SUB");
    baseParams.set("fq", "NOT name:(of the)");//test filtering
    indexAndBuild();

    //these match nothing
    assertTags(reqDoc("") );
    assertTags(reqDoc(" ") );
    assertTags(reqDoc("the") );

    String doc;

    //just London Business School via "school" substring
    doc = "school";
    assertTags(reqDoc(doc), tt(doc,"school", 0, N.London_Business_School));

    doc = "a school";
    assertTags(reqDoc(doc), tt(doc,"school", 0, N.London_Business_School));

    doc = "school a";
    assertTags(reqDoc(doc), tt(doc,"school", 0, N.London_Business_School));

    //More interesting

    doc = "school City";
    assertTags(reqDoc(doc),
        tt(doc, "school", 0, N.London_Business_School),
        tt(doc, "City", 0, N.City_of_London) );

    doc = "City of London Business School";
    assertTags(reqDoc(doc),   //no plain London (sub-tag)
        tt(doc, "City of London", 0, N.City_of_London),
        tt(doc, "London Business School", 0, N.London_Business_School));
  }

  /** whole matching, no sub-tags */
  public void testWholeMatching() throws Exception {
    baseParams.set("overlaps", "NO_SUB");
    baseParams.set("fq", "NOT name:(of the)");//test filtering
    indexAndBuild();

    //these match nothing
    assertTags(reqDoc(""));
    assertTags(reqDoc(" ") );
    assertTags(reqDoc("the") );

    //partial on N.London_Business_School matches nothing
    assertTags(reqDoc("school") );
    assertTags(reqDoc("a school") );
    assertTags(reqDoc("school a") );
    assertTags(reqDoc("school City") );

    String doc;

    doc = "school business london";//backwards
    assertTags(reqDoc(doc), tt(doc,"london", 0, N.London));

    doc = "of London Business School";
    assertTags(reqDoc(doc),   //no plain London (sub-tag)
        tt(doc, "London Business School", 0, N.London_Business_School));

    //More interesting
    doc = "City of London Business School";
    assertTags(reqDoc(doc),   //no plain London (sub-tag)
        tt(doc, "City of London", 0, N.City_of_London),
        tt(doc, "London Business School", 0, N.London_Business_School));

    doc = "City of London Business";
    assertTags(reqDoc(doc),   //no plain London (sub-tag) no Business (partial-match)
        tt(doc, "City of London", 0, N.City_of_London));

    doc = "London Business magazine";
    assertTags(reqDoc(doc),  //Just London; L.B.S. fails
        tt(doc, "London", 0, N.London));
  }

  /** whole matching, with sub-tags */
  public void testSubTags() throws Exception {
    baseParams.set("overlaps", "ALL");
    baseParams.set("fq", "NOT name:(of the)");//test filtering
    indexAndBuild();

    //these match nothing
    assertTags(reqDoc(""));
    assertTags(reqDoc(" ") );
    assertTags(reqDoc("the") );

    //partial on N.London_Business_School matches nothing
    assertTags(reqDoc("school") );
    assertTags(reqDoc("a school") );
    assertTags(reqDoc("school a") );
    assertTags(reqDoc("school City") );

    String doc;

    doc = "school business london";//backwards
    assertTags(reqDoc(doc), tt(doc,"london", 0, N.London));

    //More interesting
    doc = "City of London Business School";
    assertTags(reqDoc(doc),
        tt(doc, "City of London", 0, N.City_of_London),
        tt(doc, "London", 0, N.London),
        tt(doc, "London Business School", 0, N.London_Business_School));

    doc = "City of London Business";
    assertTags(reqDoc(doc),
        tt(doc, "City of London", 0, N.City_of_London),
        tt(doc, "London", 0, N.London));
  }

  public void testMultipleFilterQueries() throws Exception {
    baseParams.set("overlaps", "ALL");

    // build up the corpus with some additional fields for filtering purposes
    deleteByQueryAndGetVersion("*:*", null);

    int i = 0;
    assertU(adoc("id", ""+i++, "name", N.London.getName(), "type", "city", "country", "UK"));
    assertU(adoc("id", ""+i++, "name", N.London_Business_School.getName(), "type", "school", "country", "UK"));
    assertU(adoc("id", ""+i++, "name", N.Boston.getName(), "type", "city", "country", "US"));
    assertU(adoc("id", ""+i++, "name", N.City_of_London.getName(), "type", "org", "country", "UK"));
    assertU(commit());

    // not calling buildNames so that we can bring along extra attributes for filtering
    NAMES = Arrays.stream(N.values()).map(N::getName).collect(Collectors.toList());

    // phrase that matches everything
    String doc = "City of London Business School in Boston";

    // first do no filtering
    ModifiableSolrParams p = new ModifiableSolrParams();
    p.add(CommonParams.Q, "*:*");
    assertTags(reqDoc(doc, p),
        tt(doc, "City of London", 0, N.City_of_London),
        tt(doc, "London", 0, N.London),
        tt(doc, "London Business School", 0, N.London_Business_School),
        tt(doc, "Boston", 0, N.Boston));

    // add a single fq
    p.add(CommonParams.FQ, "type:city");
    assertTags(reqDoc(doc, p),
        tt(doc, "London", 0, N.London),
        tt(doc, "Boston", 0, N.Boston));

    // add another fq
    p.add(CommonParams.FQ, "country:US");
    assertTags(reqDoc(doc, p),
        tt(doc, "Boston", 0, N.Boston));
  }

  private TestTag tt(String doc, String substring, int substringIndex, N name) {
    assert substringIndex == 0;

    //little bit of copy-paste code from super.tt()
    int startOffset = -1, endOffset;
    int substringIndex1 = 0;
    for(int i = 0; i <= substringIndex1; i++) {
      startOffset = doc.indexOf(substring, ++startOffset);
      assert startOffset >= 0 : "The test itself is broken";
    }
    endOffset = startOffset+ substring.length();//1 greater (exclusive)
    return new TestTag(startOffset, endOffset, substring, lookupByName(name.getName()));
  }


  public void testEmptyCollection() throws Exception {
    //SOLR-14396: Ensure tagger handler doesn't fail on empty collections
    SolrQueryRequest req = reqDoc("anything", "indent", "on", "omitHeader", "on", "matchText", "false");
    String rspStr = h.query(req);
    req.close();

    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<response>\n" +
        "\n" +
        "<int name=\"tagsCount\">0</int>\n" +
        "<arr name=\"tags\"/>\n" +
        "<result name=\"response\" numFound=\"0\" start=\"0\" numFoundExact=\"true\">\n" +
        "</result>\n" +
        "</response>\n";
    assertEquals(expected, rspStr);
  }

}
