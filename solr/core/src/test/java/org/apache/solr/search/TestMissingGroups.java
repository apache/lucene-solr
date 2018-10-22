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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.lucene.util.TestUtil;

import org.junit.BeforeClass;
import org.junit.After;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;


/** Inspired by LUCENE-5790 */
public class TestMissingGroups extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema15.xml");
  }

  @After
  public void cleanup() throws Exception {
    clearIndex();
    assertU(optimize());
  }

  public void testGroupsOnMissingValues() throws Exception {


    final int numDocs = atLeast(500);

    // setup some key values for some random docs in our index
    // every other doc will have no values for these fields
    // NOTE: special values may be randomly assigned to the *same* docs
    final List<SpecialField> specials = new ArrayList<SpecialField>(7);
    specials.add(new SpecialField(numDocs, "group_s1", "xxx","yyy"));
    specials.add(new SpecialField(numDocs, "group_ti", "42","24"));
    specials.add(new SpecialField(numDocs, "group_td", "34.56","12.78"));
    specials.add(new SpecialField(numDocs, "group_tl", "66666666","999999999"));
    specials.add(new SpecialField(numDocs, "group_tf", "56.78","78.45"));
    specials.add(new SpecialField(numDocs, "group_b", "true", "false"));
    specials.add(new SpecialField(numDocs, "group_tdt", 
                                  "2009-05-10T03:30:00Z","1976-03-06T15:06:00Z"));
                                 
    // build up our index of docs
    
    for (int i = 1; i < numDocs; i++) { // NOTE: start at 1, doc#0 is below...
      SolrInputDocument d = sdoc("id", i);
      if (SpecialField.special_docids.contains(i)) {
        d.addField("special_s","special");
        for (SpecialField f : specials) {
          if (f.docX == i) {
            d.addField(f.field, f.valueX);
          } else if (f.docY == i) {
            d.addField(f.field, f.valueY);
          }
        }
      } else {
        // doc isn't special, give it a random chances of being excluded from some queries
        d.addField("filter_b", random().nextBoolean());
      }
      assertU(adoc(d));
      if (rarely()) {
        assertU(commit()); // mess with the segment counts
      }
    }
    // doc#0: at least one doc that is guaranteed not special and has no chance of being filtered
    assertU(adoc(sdoc("id","0")));
    assertU(commit());

    // sanity check
    assertQ(req("q", "*:*"), "//result[@numFound="+numDocs+"]");
           
    for (SpecialField special : specials) {
      // sanity checks
      assertQ(req("q", "{!term f=" + special.field + "}" + special.valueX),
              "//result[@numFound=1]");
      assertQ(req("q", "{!term f=" + special.field + "}" + special.valueY),
              "//result[@numFound=1]");

      // group on special field, and confirm all docs w/o group field get put into a single group
      final String xpre = "//lst[@name='grouped']/lst[@name='"+special.field+"']";
      assertQ(req("q", (random().nextBoolean() ? "*:*" : "special_s:special id:[0 TO 400]"),
                  "fq", (random().nextBoolean() ? "*:*" : "-filter_b:"+random().nextBoolean()),
                  "group","true",
                  "group.field",special.field,
                  "group.ngroups", "true")
              // basic grouping checks
              , xpre + "/int[@name='ngroups'][.='3']"
              , xpre + "/arr[@name='groups'][count(lst)=3]"
              // sanity check one group is the missing values
              , xpre + "/arr[@name='groups']/lst/null[@name='groupValue']"
              // check we have the correct groups for the special values with a single doc
              , xpre + "/arr[@name='groups']/lst/*[@name='groupValue'][.='"+special.valueX+"']/following-sibling::result[@name='doclist'][@numFound=1]/doc/str[@name='id'][.="+special.docX+"]"
              , xpre + "/arr[@name='groups']/lst/*[@name='groupValue'][.='"+special.valueY+"']/following-sibling::result[@name='doclist'][@numFound=1]/doc/str[@name='id'][.="+special.docY+"]"
              );

      // now do the same check, but exclude one special doc to force only 2 groups
      final int doc = random().nextBoolean() ? special.docX : special.docY;
      final Object val = (doc == special.docX) ? special.valueX : special.valueY;
      assertQ(req("q", (random().nextBoolean() ? "*:*" : "special_s:special id:[0 TO 400]"),
                  "fq", (random().nextBoolean() ? "*:*" : "-filter_b:"+random().nextBoolean()),
                  "fq", "-id:" + ((doc == special.docX) ? special.docY : special.docX),
                  "group","true",
                  "group.field",special.field,
                  "group.ngroups", "true")
              // basic grouping checks
              , xpre + "/int[@name='ngroups'][.='2']"
              , xpre + "/arr[@name='groups'][count(lst)=2]"
              // sanity check one group is the missing values
              , xpre + "/arr[@name='groups']/lst/null[@name='groupValue']"
              // check we have the correct group for the special value with a single doc
              , xpre + "/arr[@name='groups']/lst/*[@name='groupValue'][.='"+val+"']/following-sibling::result[@name='doclist'][@numFound=1]/doc/str[@name='id'][.="+doc+"]"
              );

      // one last check, exclude both docs and verify the only group is the missing value group
      assertQ(req("q", (random().nextBoolean() ? "*:*" : "special_s:special id:[0 TO 400]"),
                  "fq", (random().nextBoolean() ? "*:*" : "-filter_b:"+random().nextBoolean()),
                  "fq", "-id:" + special.docX,
                  "fq", "-id:" + special.docY,
                  "group","true",
                  "group.field",special.field,
                  "group.ngroups", "true")
              // basic grouping checks
              , xpre + "/int[@name='ngroups'][.='1']"
              , xpre + "/arr[@name='groups'][count(lst)=1]"
              // the only group should be the missing values
              , xpre + "/arr[@name='groups']/lst/null[@name='groupValue']"
              );
      
     }
  }

  private static final class SpecialField {
    // fast lookup of which docs are special
    public static final Set<Integer> special_docids = new HashSet<>();
    public final String field;

    public final int docX;
    public final Object valueX;

    public final int docY;
    public final Object valueY;

    public SpecialField(int numDocs, String field, Object valueX, Object valueY) {
      this.field = field;

      this.valueX = valueX;
      this.valueY = valueY;

      this.docX = TestUtil.nextInt(random(),1,numDocs-1);
      this.docY = (docX < (numDocs / 2))
        ? TestUtil.nextInt(random(),docX+1,numDocs-1)
        : TestUtil.nextInt(random(),1,docX-1);

      special_docids.add(docX);
      special_docids.add(docY);
    }
  }  
}
