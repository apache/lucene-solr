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

package org.apache.solr.update.processor;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOpenNLPExtractNamedEntitiesUpdateProcessorFactory extends UpdateProcessorTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    File testHome = createTempDir().toFile();
    FileUtils.copyDirectory(getFile("analysis-extras/solr"), testHome);
    initCore("solrconfig-opennlp-extract.xml", "schema-opennlp-extract.xml", testHome.getAbsolutePath());
  }

  @Test
  public void testSimpleExtract() throws Exception {
    SolrInputDocument doc = processAdd("extract-single",
        doc(f("id", "1"),
            f("source1_s", "Take this to Mr. Flashman.")));
    assertEquals("dest_s should have stringValue", "Flashman", doc.getFieldValue("dest_s"));
  }

  @Test
  public void testMultiExtract() throws Exception {
    SolrInputDocument doc = processAdd("extract-multi",
        doc(f("id", "1"),
            f("source1_s", "Hello Flashman."),
            f("source2_s", "Calling Flashman.")));

    assertEquals(Arrays.asList("Flashman", "Flashman"), doc.getFieldValues("dest_s"));
  }

  @Test
  public void testArrayExtract() throws Exception {
    SolrInputDocument doc = processAdd("extract-array",
        doc(f("id", "1"),
            f("source1_s", "Currently we have Flashman. Not much else."),
            f("source2_s", "Flashman. Is. Not. There.")));

    assertEquals(Arrays.asList("Flashman", "Flashman"), doc.getFieldValues("dest_s"));
  }

  @Test
  public void testSelectorExtract() throws Exception {
    SolrInputDocument doc = processAdd("extract-selector",
        doc(f("id", "1"),
            f("source0_s", "Flashman. Or not."),
            f("source1_s", "Serendipitously, he was. I mean, Flashman. And yet."),
            f("source2_s", "Correct, Flashman.")));

    assertEquals(Arrays.asList("Flashman", "Flashman"), doc.getFieldValues("dest_s"));
  }

  public void testMultipleExtracts() throws Exception {
    // test example from the javadocs
    SolrInputDocument doc = processAdd("multiple-extract",
        doc(f("id", "1"),
            f("text", "From Flashman. To Panman."),
            f("title", "It's Captain Flashman.", "Privately, Flashman."),
            f("subtitle", "Ineluctably, Flashman."),
            f("corrolary_txt", "Forsooth thou bringeth Flashman."),
            f("notes_txt", "Yes Flashman."),
            f("summary", "Many aspire to be Flashman in London."),
            f("descs", "Courage, Flashman.", "Ain't he Flashman."),
            f("descriptions", "Flashman. Flashman. Flashman.")));

    assertEquals(Arrays.asList("Flashman", "Flashman"), doc.getFieldValues("people_s"));
    assertEquals(Arrays.asList("Flashman", "Flashman", "Flashman"), doc.getFieldValues("titular_people"));
    assertEquals(Arrays.asList("Flashman", "Flashman"), doc.getFieldValues("key_desc_people"));
    assertEquals(Arrays.asList("Flashman", "Flashman", "Flashman"), doc.getFieldValues("key_description_people"));
    assertEquals("Flashman", doc.getFieldValue("summary_person_s")); // {EntityType} field name interpolation
    assertEquals("London", doc.getFieldValue("summary_location_s")); // {EntityType} field name interpolation
  }

  public void testEquivalentExtraction() throws Exception {
    SolrInputDocument d;

    // regardless of chain, all of these checks should be equivalent
    for (String chain : Arrays.asList("extract-single", "extract-single-regex",
        "extract-multi", "extract-multi-regex",
        "extract-array", "extract-array-regex",
        "extract-selector", "extract-selector-regex")) {

      // simple extract
      d = processAdd(chain,
          doc(f("id", "1111"),
              f("source0_s", "Totally Flashman."), // not extracted
              f("source1_s", "One nation under Flashman.", "Good Flashman.")));
      assertNotNull(chain, d);
      assertEquals(chain, Arrays.asList("Flashman", "Flashman"), d.getFieldValues("dest_s"));

      // append to existing values
      d = processAdd(chain,
          doc(f("id", "1111"),
              field("dest_s", "orig1", "orig2"),
              f("source0_s", "Flashman. In totality."), // not extracted
              f("source1_s", "Two nations under Flashman.", "Meh Flashman.")));
      assertNotNull(chain, d);
      assertEquals(chain, Arrays.asList("orig1", "orig2", "Flashman", "Flashman"), d.getFieldValues("dest_s"));
    }

    // should be equivalent for any chain matching source1_s and source2_s (but not source0_s)
    for (String chain : Arrays.asList("extract-multi", "extract-multi-regex",
        "extract-array", "extract-array-regex",
        "extract-selector", "extract-selector-regex")) {

      // simple extract
      d = processAdd(chain,
          doc(f("id", "1111"),
              f("source0_s", "Not Flashman."), // not extracted
              f("source1_s", "Could have had a Flashman.", "Bad Flashman."),
              f("source2_s", "Indubitably Flashman.")));
      assertNotNull(chain, d);
      assertEquals(chain, Arrays.asList("Flashman", "Flashman", "Flashman"), d.getFieldValues("dest_s"));

      // append to existing values
      d = processAdd(chain,
          doc(f("id", "1111"),
              field("dest_s", "orig1", "orig2"),
              f("source0_s", "Never Flashman."), // not extracted
              f("source1_s", "Seeking Flashman.", "Evil incarnate Flashman."),
              f("source2_s", "Perfunctorily Flashman.")));
      assertNotNull(chain, d);
      assertEquals(chain, Arrays.asList("orig1", "orig2", "Flashman", "Flashman", "Flashman"), d.getFieldValues("dest_s"));
    }

    // any chain that copies source1_s to dest_s should be equivalent for these assertions
    for (String chain : Arrays.asList("extract-single", "extract-single-regex",
        "extract-multi", "extract-multi-regex",
        "extract-array", "extract-array-regex",
        "extract-selector", "extract-selector-regex")) {

      // simple extract
      d = processAdd(chain,
          doc(f("id", "1111"),
              f("source1_s", "Always Flashman.", "Flashman. Noone else.")));
      assertNotNull(chain, d);
      assertEquals(chain, Arrays.asList("Flashman", "Flashman"), d.getFieldValues("dest_s"));

      // append to existing values
      d = processAdd(chain,
          doc(f("id", "1111"),
              field("dest_s", "orig1", "orig2"),
              f("source1_s", "Flashman.  And, scene.", "Contemporary Flashman. Yeesh.")));
      assertNotNull(chain, d);
      assertEquals(chain, Arrays.asList("orig1", "orig2", "Flashman", "Flashman"), d.getFieldValues("dest_s"));
    }
  }

  public void testExtractFieldRegexReplaceAll() throws Exception {
    SolrInputDocument d = processAdd("extract-regex-replaceall",
        doc(f("id", "1111"),
            f("foo_x2_s", "Infrequently Flashman.", "In the words of Flashman."),
            f("foo_x3_x7_s", "Flashman. Whoa.")));

    assertNotNull(d);
    assertEquals(Arrays.asList("Flashman", "Flashman"), d.getFieldValues("foo_y2_s"));
    assertEquals("Flashman", d.getFieldValue("foo_y3_y7_s"));
  }

  public void testExtractFieldRegexReplaceAllWithEntityType() throws Exception {
    SolrInputDocument d = processAdd("extract-regex-replaceall-with-entity-type",
        doc(f("id", "1111"),
            f("foo_x2_s", "Infrequently Flashman in London.", "In the words of Flashman in London."),
            f("foo_x3_x7_s", "Flashman in London. Whoa.")));

    assertNotNull(d);
    assertEquals(d.getFieldNames().toString(), Arrays.asList("Flashman", "Flashman"), d.getFieldValues("foo_person_y2_s"));
    assertEquals(d.getFieldNames().toString(), Arrays.asList("London", "London"), d.getFieldValues("foo_location_y2_s"));
    assertEquals(d.getFieldNames().toString(),"Flashman", d.getFieldValue("foo_person_y3_person_y7_s"));
    assertEquals(d.getFieldNames().toString(),"London", d.getFieldValue("foo_location_y3_location_y7_s"));
  }
}
