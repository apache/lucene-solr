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

package org.apache.solr.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.TestUtil;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.NestedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestNestedUpdateProcessor extends SolrTestCaseJ4 {

  private static final char PATH_SEP_CHAR = '/';
  private static final char NUM_SEP_CHAR = '#';
  private static final String SINGLE_VAL_CHAR = "";
  private static final String grandChildId = "4";
  private static final String secondChildList = "anotherChildList";
  private static final String jDoc = "{\n" +
      "    \"add\": {\n" +
      "        \"doc\": {\n" +
      "            \"id\": \"1\",\n" +
      "            \"children\": [\n" +
      "                {\n" +
      "                    \"id\": \"2\",\n" +
      "                    \"foo_s\": \"Yaz\"\n" +
      "                    \"grandChild\": \n" +
      "                          {\n" +
      "                             \"id\": \""+ grandChildId + "\",\n" +
      "                             \"foo_s\": \"Jazz\"\n" +
      "                          },\n" +
      "                },\n" +
      "                {\n" +
      "                    \"id\": \"3\",\n" +
      "                    \"foo_s\": \"Bar\"\n" +
      "                }\n" +
      "            ]\n" +
                   secondChildList + ": [{\"id\": \"4\", \"last_s\": \"Smith\"}],\n" +
      "        }\n" +
      "    }\n" +
      "}";

  private static final String errDoc = "{\n" +
      "    \"add\": {\n" +
      "        \"doc\": {\n" +
      "            \"id\": \"1\",\n" +
      "            \"children" + PATH_SEP_CHAR + "a\": [\n" +
      "                {\n" +
      "                    \"id\": \"2\",\n" +
      "                    \"foo_s\": \"Yaz\"\n" +
      "                    \"grandChild\": \n" +
      "                          {\n" +
      "                             \"id\": \""+ grandChildId + "\",\n" +
      "                             \"foo_s\": \"Jazz\"\n" +
      "                          },\n" +
      "                },\n" +
      "                {\n" +
      "                    \"id\": \"3\",\n" +
      "                    \"foo_s\": \"Bar\"\n" +
      "                }\n" +
      "            ]\n" +
      "        }\n" +
      "    }\n" +
      "}";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-nest.xml");
  }

  @Before
  public void before() throws Exception {
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testDeeplyNestedURPGrandChild() throws Exception {
    final String[] tests = {
        "/response/docs/[0]/id=='4'",
        "/response/docs/[0]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/children#0/grandChild#'"
    };
    indexSampleData(jDoc);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":*/grandChild",
        "fl","*, _nest_path_",
        "sort","id desc",
        "wt","json"),
        tests);
  }

  @Test
  public void testNumberInName() throws Exception {
    // child named "grandChild99"  (has a number in it)
    indexSampleData(jDoc.replace("grandChild", "grandChild99"));
    //assertQ(req("qt", "/terms", "terms", "true", "terms.fl", IndexSchema.NEST_PATH_FIELD_NAME), "false"); // for debugging

    // find it
    assertJQ(req("q", "{!field f=" + IndexSchema.NEST_PATH_FIELD_NAME + "}/children/grandChild99"),
        "/response/numFound==1");
    // should *NOT* find it; different number
    assertJQ(req("q", "{!field f=" + IndexSchema.NEST_PATH_FIELD_NAME + "}/children/grandChild22"),
        "/response/numFound==0");

  }

  @Test
  public void testDeeplyNestedURPChildren() throws Exception {
    final String[] childrenTests = {
        "/response/docs/[0]/id=='2'",
        "/response/docs/[1]/id=='3'",
        "/response/docs/[0]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/children#0'",
        "/response/docs/[1]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/children#1'"
    };
    indexSampleData(jDoc);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":\\/children",
        "fl","*, _nest_path_",
        "sort","id asc",
        "wt","json"),
        childrenTests);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":\\/anotherChildList",
        "fl","*, _nest_path_",
        "sort","id asc",
        "wt","json"),
        "/response/docs/[0]/id=='4'",
        "/response/docs/[0]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/anotherChildList#0'");
  }

  @Test
  public void testDeeplyNestedURPSanity() throws Exception {
    SolrInputDocument docHierarchy = sdoc("id", "1", "children", sdocs(sdoc("id", "2", "name_s", "Yaz"),
        sdoc("id", "3", "name_s", "Jazz", "grandChild", sdoc("id", "4", "name_s", "Gaz"))), "lonelyChild", sdoc("id", "5", "name_s", "Loner"));
    UpdateRequestProcessor nestedUpdate = new NestedUpdateProcessorFactory().getInstance(req(), null, null);
    AddUpdateCommand cmd = new AddUpdateCommand(req());
    cmd.solrDoc = docHierarchy;
    nestedUpdate.processAdd(cmd);
    cmd.clear();

    @SuppressWarnings({"rawtypes"})
    List children = (List) docHierarchy.get("children").getValues();

    SolrInputDocument firstChild = (SolrInputDocument) children.get(0);
    assertEquals("SolrInputDocument(fields: [id=2, name_s=Yaz, _nest_path_=/children#0, _nest_parent_=1])", firstChild.toString());

    SolrInputDocument secondChild = (SolrInputDocument) children.get(1);
    assertEquals("SolrInputDocument(fields: [id=3, name_s=Jazz, grandChild=SolrInputDocument(fields: [id=4, name_s=Gaz, _nest_path_=/children#1/grandChild#, _nest_parent_=3]), _nest_path_=/children#1, _nest_parent_=1])", secondChild.toString());

    SolrInputDocument grandChild = (SolrInputDocument)((SolrInputDocument) children.get(1)).get("grandChild").getValue();
    assertEquals("SolrInputDocument(fields: [id=4, name_s=Gaz, _nest_path_=/children#1/grandChild#, _nest_parent_=3])", grandChild.toString());

    SolrInputDocument singularChild = (SolrInputDocument) docHierarchy.get("lonelyChild").getValue();
    assertEquals("SolrInputDocument(fields: [id=5, name_s=Loner, _nest_path_=/lonelyChild#, _nest_parent_=1])", singularChild.toString());
  }

  @Test
  public void testDeeplyNestedURPChildrenWoId() throws Exception {
    final String rootId = "1";
    final String childKey = "grandChild";
    final String expectedId = rootId + "/children#1/" + childKey + NUM_SEP_CHAR + SINGLE_VAL_CHAR;
    SolrInputDocument noIdChildren = sdoc("id", rootId, "children", sdocs(sdoc("name_s", "Yaz"), sdoc("name_s", "Jazz", childKey, sdoc("name_s", "Gaz"))));
    UpdateRequestProcessor nestedUpdate = new NestedUpdateProcessorFactory().getInstance(req(), null, null);
    AddUpdateCommand cmd = new AddUpdateCommand(req());
    cmd.solrDoc = noIdChildren;
    nestedUpdate.processAdd(cmd);
    cmd.clear();
    @SuppressWarnings({"rawtypes"})
    List children = (List) noIdChildren.get("children").getValues();
    SolrInputDocument idLessChild = (SolrInputDocument)((SolrInputDocument) children.get(1)).get(childKey).getValue();
    assertTrue("Id less child did not get an Id", idLessChild.containsKey("id"));
    assertEquals("Id less child was assigned an unexpected id", expectedId, idLessChild.getFieldValue("id").toString());
  }

  @Test
  public void testDeeplyNestedURPFieldNameException() throws Exception {
    final String errMsg = "contains: '" + PATH_SEP_CHAR + "' , which is reserved for the nested URP";
    thrown.expect(SolrException.class);
    indexSampleData(errDoc);
    thrown.expectMessage(errMsg);
  }

  private void indexSampleData(String cmd) throws Exception {
    updateJ(cmd, null);
    assertU(commit());
  }

  /**
   * Randomized test to look for flaws in the documented approach for building "safe" values of the 
   * <code>of</code> / <code>which</code> params in the <code>child</code> / <code>parent</code> QParsers
   * when a specific <code>_nest_path_</code> is desired
   *
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-14687">SOLR-14687</a>
   */
  public void testRandomNestPathQueryFiltering() throws Exception {

    // First: build a bunch of complex randomly nested documents, with random "nest paths"
    // re-use the same "path segments" at various levels of nested, so as to confuse things even more
    final RandomNestedDocModel docs = new RandomNestedDocModel();
    for (int i = 0; i < 50; i++) {
      final SolrInputDocument rootDoc = docs.buildRandomDoc();
      assertU(adoc(rootDoc));
    }
    assertU(commit());

    // now do some systematic parent/child queries.
    // we're checking both for "parser errors" (ie: children matching "parent filter")
    // as well as that the test_path_s of all matching docs meets our expectations

    // *:* w/ parent parser...
    // starts at "root" parent_path and recurses until we get no (expected) results
    assertTrue(// we expected at least one query for every "real" path,
               // but there will be more because we'll try lots of sub-paths that have no docs
               docs.numDocsDescendentFromPath.keySet().size()
               < docs.recursiveCheckParentQueryOfAllChildren(Collections.<String>emptyList()));
    // sanity check: path that is garunteed not to exist...
    assertEquals(1, docs.recursiveCheckParentQueryOfAllChildren(Arrays.asList("xxx", "yyy")));

    // *:* w/ child parser...
    // starts at "root" parent_path and recurses until we get no (expected) results
    assertTrue(// we expected at least one query for every "real" path,
               // but there will be more because we'll try lots of sub-paths that have no docs
               docs.numDocsWithPathWithKids.keySet().size()
               < docs.recursiveCheckChildQueryOfAllParents(Collections.<String>emptyList()));
    // sanity check: path that is garunteed not to exist...
    assertEquals(1, docs.recursiveCheckChildQueryOfAllParents(Arrays.asList("xxx", "yyy")));

    // quering against individual child ids w/ both parent & child parser...
    docs.checkParentAndChildQueriesOfEachDocument();
  }
  
  private static class RandomNestedDocModel {
    public static final List<String> PATH_ELEMENTS = Arrays.asList("aa", "bb", "cc", "dd");

    private final Map<String,SolrInputDocument> allDocs = new HashMap<>();
    
    public final Map<String,Integer> numDocsDescendentFromPath = new HashMap<>();
    public final Map<String,Integer> numDocsWithPathWithKids = new HashMap<>();
      
    private int idCounter = 0;

    public synchronized SolrInputDocument buildRandomDoc() {
      return buildRandomDoc(null, Collections.<String>emptyList(), 15);
    }
    private static String joinPath(List<String> test_path) {
      return "/" + String.join("/", test_path);
    }
    private synchronized SolrInputDocument buildRandomDoc(SolrInputDocument parent,
                                                          List<String> test_path,
                                                          int maxDepthAndBreadth) {
      final String path_string = joinPath(test_path);
      final String id = "" + (++idCounter);
      maxDepthAndBreadth--;
      final SolrInputDocument doc = sdoc
        ("id", id,
         // may change, but we want it 0 even if we never add any
         "num_direct_kids_s", "0", 
         // conceptually matches _nest_path_ but should be easier to make assertions about (no inline position #s)
         "test_path_s", path_string);
      if (null != parent) {
        // order matters: if we add the Collection first, SolrInputDocument will try to reuse it
        doc.addField("ancestor_ids_ss", parent.getFieldValue("id"));
        if (parent.containsKey("ancestor_ids_ss")) { // sigh: getFieldValues returns null, not empty collection
          doc.addField("ancestor_ids_ss", parent.getFieldValues("ancestor_ids_ss"));
        }
      }
      
      for (int i = 0; i < test_path.size(); i++) {
        // NOTE: '<' not '<=" .. we only includes paths we are descendents of, not our full path...
        numDocsDescendentFromPath.merge(joinPath(test_path.subList(0, i)), 1, Math::addExact);
      }
      
      if (0 < maxDepthAndBreadth) {
        final int numDirectKids = TestUtil.nextInt(random(), 0, Math.min(4, maxDepthAndBreadth));
        doc.setField("num_direct_kids_s", "" + numDirectKids);
        if (0 < numDirectKids) {
          numDocsWithPathWithKids.merge(path_string, 1, Math::addExact);
        }
        maxDepthAndBreadth -= numDirectKids;
        for (int i = 0; i < numDirectKids; i++) {
          final String kidType = PATH_ELEMENTS.get(random().nextInt(PATH_ELEMENTS.size()));
          final List<String> kid_path = new ArrayList<>(test_path);
          kid_path.add(kidType);
          final SolrInputDocument kid = buildRandomDoc(doc, kid_path, maxDepthAndBreadth);
          doc.addField(kidType, kid);
          // order matters: if we add the Collection first, SolrInputDocument will try to reuse it
          doc.addField("descendent_ids_ss", kid.getFieldValue("id"));
          if (kid.containsKey("descendent_ids_ss")) {  // sigh: getFieldValues returns null, not empty collection
            doc.addField("descendent_ids_ss", kid.getFieldValues("descendent_ids_ss"));
          }
        }
      }
      allDocs.put(id, doc);
      return doc;
    }


    /** 
     * Loops over the 'model' of every document we've indexed, asserting that
     * parent/child queries wrapping an '<code>id:foo</code> using various paths
     * match the expected ancestors/descendents
     */
    public void checkParentAndChildQueriesOfEachDocument() {
      assertFalse("You didn't build any docs", allDocs.isEmpty());
      
      for (String doc_id : allDocs.keySet()) {
        final String doc_path = allDocs.get(doc_id).getFieldValue("test_path_s").toString();
        
        if ( ! doc_path.equals("/") ) {
          
          // doc_id -> descdentId must have at least one ancestor (since it's not a root level document)
          final String descendentId = doc_id;
          assert allDocs.get(descendentId).containsKey("ancestor_ids_ss");
          final List<Object> allAncestorIds = new ArrayList<>(allDocs.get(descendentId).getFieldValues("ancestor_ids_ss"));
          
          // pick a random ancestor to use in our testing...
          final String ancestorId = allAncestorIds.get(random().nextInt(allAncestorIds.size())).toString();
          final String ancestor_path = allDocs.get(ancestorId).getFieldValue("test_path_s").toString();
          
          final Collection<Object> allOfAncestorsDescendentIds
            = allDocs.get(ancestorId).getFieldValues("descendent_ids_ss");
          
          assertTrue("Sanity check " + ancestorId + " ancestor of " + descendentId,
                     allOfAncestorsDescendentIds.contains(descendentId));
          
          // now we should be able to assert that a 'parent' query wrapped around a query for the descendentId
          // using the ancestor_path should match exactly one doc: our ancestorId...
          assertQ(req(parentQueryMaker(ancestor_path, "id:" + descendentId),
                      "_trace_path_tested", ancestor_path,
                      "fl", "id",
                      "indent", "true")
                  , "//result/@numFound=1"
                  , "//doc/str[@name='id'][.='"+ancestorId+"']"
                  );
          
          // meanwhile, a 'child' query wrapped arround a query for the ancestorId, using the ancestor_path,
          // should match all of it's descendents (for simplicity we'll check just the numFound and the
          // 'descendentId' we started with)
          assertQ(req(childQueryMaker(ancestor_path, "id:" + ancestorId),
                      "_trace_path_tested", ancestor_path,
                      "rows", "9999",
                      "fl", "id",
                      "indent", "true")
                  , "//result/@numFound="+allOfAncestorsDescendentIds.size()
                  , "//doc/str[@name='id'][.='"+descendentId+"']"
                  );
          
        }
        
        // regardless of wether doc_id has an ancestor or not, a 'parent' query with a path that isn't a
        // prefix of the path of the (child) doc_id in the wrapped query should match 0 docs w/o failing
        assertQ(req(parentQueryMaker("/xxx/yyy", "id:" + doc_id),
                    "_trace_path_tested", "/xxx/yyy",
                    "indent", "true")
                , "//result/@numFound=0");
        
        // likewise: a 'child' query wrapped around a query for our doc_id (regardless of wether if has
        // any kids), using a path that doesn't start with the same prefix as doc_id, should match 0
        // docs w/o failing
        assertQ(req(childQueryMaker("/xxx/yyy", "id:" + doc_id),
                    "_trace_path_tested", "/xxx/yyy",
                    "indent", "true")
                , "//result/@numFound=0");
        
        // lastly: wrapping a child query around a query for our doc_id, using a path that "extends" 
        // the doc_id's path should always get 0 results if that path doesn't match any actual kids
        // (regardless of wether doc_id has any children/descendents)
        assertQ(req(childQueryMaker(doc_path + "/xxx/yyy", "id:" + doc_id),
                    "_trace_path_tested", doc_path + "/xxx/yyy",
                    "indent", "true")
                , "//result/@numFound=0");
      }
    }

    
    /** 
     * recursively check path permutations using <code>*:*</code> inner query, asserting that the 
     * only docs matched have the expected path, and at least one kid (since this is the "parents" parser)
     *
     * (using <code>*:*</code> as our inner query keeps the validation simple and also helps stress out 
     * risk of matching incorrect docs if the 'which' param is wrong)
     *
     * @return total number of queries checked (assuming no assertion failures)
     */
    public int recursiveCheckParentQueryOfAllChildren(List<String> parent_path) {
      final String p = joinPath(parent_path);
      final int expectedParents = numDocsWithPathWithKids.getOrDefault(p, 0);
      assertQ(req(parentQueryMaker(p, "*:*"),
                  "rows", "9999",
                  "_trace_path_tested", p,
                  "fl", "test_path_s,num_direct_kids_s",
                  "indent", "true")
              , "//result/@numFound="+expectedParents
              , "count(//doc)="+expectedParents
              , "count(//doc/str[@name='test_path_s'][.='"+p+"'])="+expectedParents
              , "0=count(//doc/str[@name='num_direct_kids_s'][.='0'])"
              );
      int numChecked = 1;

      // no point in recursing on the current path if we already have no results found...
      if (0 < expectedParents) {
        for (String next : PATH_ELEMENTS) {
          final List<String> next_path = new ArrayList<>(parent_path);
          next_path.add(next);
          numChecked += recursiveCheckParentQueryOfAllChildren(next_path);
        }
      }
      return numChecked;
    }
    
    /** 
     * This implements the "safe query based on parent path" rules we're sanity checking.
     *
     * @param parent_path the nest path of the parents to consider
     * @param inner_child_query the specific children whose ancestors we are looking for, must be simple string <code>*:*</code> or <code>id:foo</code>
     */
    private SolrParams parentQueryMaker(String parent_path, String inner_child_query) {
      assertValidPathSytax(parent_path);
      final boolean verbose = random().nextBoolean();
      
      if (parent_path.equals("/")) {
        if (verbose) {
          return params("q", "{!parent which=$parent_filt v=$child_q}",
                        "parent_filt", "(*:* -_nest_path_:*)",
                        "child_q", "(+" + inner_child_query + " +_nest_path_:*)");
        } else {
          return params("q", "{!parent which='(*:* -_nest_path_:*)'}(+" + inner_child_query + " +_nest_path_:*)");
        }
      } // else...

      if (verbose) {
        final String path = parent_path + "/";
        return params("q", "{!parent which=$parent_filt v=$child_q}",
                      "parent_filt", "(*:* -{!prefix f='_nest_path_' v='"+path+"'})",
                      "child_q", "(+" + inner_child_query + " +{!prefix f='_nest_path_' v='"+path+"'})");
      } else {
        // '/' has to be escaped other wise it will be treated as a regex query...
        // (and of course '\' escaping is the java syntax as well, we have to double it)
        final String path = (parent_path + "/").replace("/", "\\/");
        // ...and when used inside the 'which' param it has to be escaped *AGAIN* because of
        // the "quoted" localparam evaluation layer...
        return params("q", "{!parent which='(*:* -_nest_path_:" + path.replace("\\/","\\\\/") + "*)'}"
                      + "(+" + inner_child_query + " +_nest_path_:" + path + "*)");
      }
    }

    /** 
     * recursively check path permutations using <code>*:*</code> inner query, asserting that the 
     * only docs matched have paths that include the specified path as a (strict) prefix
     *
     * (using <code>*:*</code> as our inner query keeps the validation simple and also helps stress out 
     * risk of matching incorrect docs if the 'of' param is wrong)
     *
     * @return total number of queries checked (assuming no assertion failures)
     */
    public int recursiveCheckChildQueryOfAllParents(List<String> parent_path) {
      final String p = joinPath(parent_path);
      final int expectedMatches = numDocsDescendentFromPath.getOrDefault(p, 0);
      assertQ(req(childQueryMaker(p, "*:*"),
                  "rows", "9999",
                  "_trace_path_tested", p,
                  "fl", "test_path_s",
                  "indent", "true")
              , "//result/@numFound="+expectedMatches
              , "count(//doc)="+expectedMatches
              , "count(//doc/str[@name='test_path_s'][starts-with(., '"+p+"')])="+expectedMatches
              );
      int numChecked = 1;

      // no point in recursing on the current path if we already have no results found...
      if (0 < expectedMatches) {
        for (String next : PATH_ELEMENTS) {
          final List<String> next_path = new ArrayList<>(parent_path);
          next_path.add(next);
          numChecked += recursiveCheckChildQueryOfAllParents(next_path);
        }
      }
      return numChecked;
    }

    /** 
     * This implements the "safe query based on parent path" rules we're sanity checking.
     *
     * @param parent_path the nest path of the parents to consider
     * @param inner_parent_query the specific parents whose descendents we are looking for, must be simple string <code>*:*</code> or <code>id:foo</code>
     */
    private SolrParams childQueryMaker(String parent_path, String inner_parent_query) {
      assertValidPathSytax(parent_path);
      final boolean verbose = random().nextBoolean();
      
      if (parent_path.equals("/")) {
        if (verbose) {
          return params("q", "{!child of=$parent_filt v=$parent_q})",
                        "parent_filt", "(*:* -_nest_path_:*)",
                        "parent_q", "(+" + inner_parent_query + " -_nest_path_:*)");
        } else {
          return params("q", "{!child of='(*:* -_nest_path_:*)'}(+" + inner_parent_query + " -_nest_path_:*)");
        }
      } // else...
      
      if (verbose) {
        return params("q", "{!child of=$parent_filt v=$parent_q})",
                      "parent_filt", "(*:* -{!prefix f='_nest_path_' v='"+parent_path+"/'})",
                      "parent_q", "(+" + inner_parent_query + " +{!field f='_nest_path_' v='"+parent_path+"'})");
      } else {
        // '/' has to be escaped other wise it will be treated as a regex query...
        // (and of course '\' escaping is the java syntax as well, we have to double it)
        final String exact_path = parent_path.replace("/", "\\/");
        // ...and when used inside the 'which' param it has to be escaped *AGAIN* because of
        // the "quoted" localparam evaluation layer...
        final String prefix_path = (parent_path + "/").replace("/","\\\\/");
        return params("q", "{!child of='(*:* -_nest_path_:"+prefix_path+"*)'}"
                      + "(+" + inner_parent_query + " +_nest_path_:" + exact_path + ")");
      }
    }

    private void assertValidPathSytax(String path) {
      assert path.startsWith("/");
      assert (1 == path.length()) ^ ! path.endsWith("/");
    }
  }
}
