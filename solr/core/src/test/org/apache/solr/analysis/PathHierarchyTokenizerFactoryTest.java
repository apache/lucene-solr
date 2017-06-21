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
package org.apache.solr.analysis;

import org.apache.solr.SolrTestCaseJ4;

import org.junit.BeforeClass;

public class PathHierarchyTokenizerFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");

    assertU(adoc("id", "11", 
                 "cat_path", "Movies/Fic/War"));

    assertU(adoc("id", "31", 
                 "cat_path", "Books/Fic"));
    assertU(adoc("id", "31", 
                 "cat_path", "Books/Fic/Law"));
    assertU(adoc("id", "32", 
                 "cat_path", "Books/Fic/Science"));

    assertU(adoc("id", "40", 
                 "cat_path", "Books/NonFic"));
    assertU(adoc("id", "41", 
                 "cat_path", "Books/NonFic/Law"));
    assertU(adoc("id", "42", 
                 "cat_path", "Books/NonFic/Law", 
                 "cat_path", "Books/NonFic/Science"));
    assertU(adoc("id", "43", 
                 "cat_path", "Books/NonFic/Science/Physics", 
                 "cat_path", "Books/NonFic/History"));

    assertU(commit());
  }

  public void testDescendents() throws Exception {

    assertQ(req("{!field f=cat_path}Books/NonFic")
            ,"//*[@numFound='4']"
            ,"//str[@name='id' and .='40']"
            ,"//str[@name='id' and .='41']"
            ,"//str[@name='id' and .='42']"
            ,"//str[@name='id' and .='43']"
            );
    assertQ(req("{!field f=cat_path}Books/NonFic/Law")
            ,"//*[@numFound='2']"
            ,"//str[@name='id' and .='41']"
            ,"//str[@name='id' and .='42']"
            );

    assertQ(req("{!field f=cat_path}Books/NonFic/Science")
            ,"//*[@numFound='2']"
            ,"//str[@name='id' and .='42']"
            ,"//str[@name='id' and .='43']"
            );
  }

  public void testAncestors() throws Exception {

    assertQ(req("{!field f=cat_ancestor}Books/NonFic/Science")
            ,"//*[@numFound='2']"
            ,"//str[@name='id' and .='40']"
            ,"//str[@name='id' and .='42']"
            );
    assertQ(req("{!field f=cat_ancestor}Books/NonFic/Law")
            ,"//*[@numFound='3']"
            ,"//str[@name='id' and .='40']"
            ,"//str[@name='id' and .='41']"
            ,"//str[@name='id' and .='42']"
            );

    assertQ(req("{!field f=cat_ancestor}Books/NonFic/Science/Physics")
            ,"//*[@numFound='3']"
            ,"//str[@name='id' and .='40']"
            ,"//str[@name='id' and .='42']"
            ,"//str[@name='id' and .='43']"
            );
  }
}
