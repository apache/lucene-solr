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

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the {@link TaggerRequestHandler} with
 * a Analyzer chain that does use the {@link TaggingAttribute}. See the test
 * configuration under 'taggingattribute'.
 */
public class TaggingAttributeTest extends TaggerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tagger.xml", "schema-tagger.xml");
  }

  /**
   * Whole matching, no sub-tags. Links only words with &gt; 3 letters.
   * Because of that "San" is not used to start tags
   *
   */
  @Test
  public void testTaggingAttribute() throws Exception {
    baseParams.set("field", "name_tagAttribute"); // has WordLengthTaggingFilter using the TaggingAttribute
    // this test is based on the longest dominant right test, so we use the
    // the same TagClusterReducer setting
    baseParams.set("overlaps", "LONGEST_DOMINANT_RIGHT");

    buildNames("in", "San", "in San", "Francisco", "San Francisco",
        "San Francisco State College", "College of California",
        "Clayton", "Clayton North", "North Carolina");

    assertTags("He lived in San Francisco.",
        //"in", "San Francisco"); //whis would be expected without taggable
        "Francisco");// this are the expected results with taggable

    assertTags("He enrolled in San Francisco State College of California",
        //"in", "San Francisco State College"); //without taggable enabled
        "Francisco", "College of California");// With taggable
    //NOTE this also tests that started tags are advanced for non-taggable
    //     tokens, as otherwise 'College of California' would not be
    //     suggested.

    assertTags("He lived in Clayton North Carolina",
        //"in", "Clayton", "North Carolina");
        "Clayton", "North Carolina");

  }

}
