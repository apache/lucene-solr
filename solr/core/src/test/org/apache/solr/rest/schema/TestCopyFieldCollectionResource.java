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
package org.apache.solr.rest.schema;
import org.apache.solr.rest.SolrRestletTestBase;
import org.junit.Test;

public class TestCopyFieldCollectionResource extends SolrRestletTestBase {
  @Test
  public void testXMLGetAllCopyFields() throws Exception {
    assertQ("/schema/copyfields?indent=on&wt=xml",
        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
            +"                                      and str[@name='dest'][.='title']]",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title']"
           +"                                      and str[@name='dest'][.='title_stemmed']"
           +"                                      and int[@name='maxChars'][.='200']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title']"
           +"                                      and str[@name='dest'][.='dest_sub_no_ast_s']]",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                      and str[@name='dest'][.='title']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                      and str[@name='dest'][.='*_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                      and str[@name='dest'][.='*_dest_sub_s']]",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                      and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                      and str[@name='dest'][.='title']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                      and str[@name='dest'][.='*_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                      and str[@name='dest'][.='*_dest_sub_s']]",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                      and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                      and str[@name='dest'][.='*_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                      and str[@name='dest'][.='*_dest_sub_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                      and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                      and str[@name='dest'][.='text']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                      and str[@name='dest'][.='*_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                      and str[@name='dest'][.='*_dest_sub_s']]",

            "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                      and str[@name='dest'][.='dest_sub_no_ast_s']]");
  }

  @Test
  public void testGetAllCopyFields() throws Exception {
    assertJQ("/schema/copyfields",
             "/copyFields/[1]=={'source':'src_sub_no_ast_i','dest':'title'}",
             "/copyFields/[7]=={'source':'title','dest':'dest_sub_no_ast_s'}",

             "/copyFields/[8]=={'source':'*_i','dest':'title'}",
             "/copyFields/[9]=={'source':'*_i','dest':'*_s'}",
             "/copyFields/[10]=={'source':'*_i','dest':'*_dest_sub_s'}",
             "/copyFields/[11]=={'source':'*_i','dest':'dest_sub_no_ast_s'}",

             "/copyFields/[12]=={'source':'*_src_sub_i','dest':'title'}",
             "/copyFields/[13]=={'source':'*_src_sub_i','dest':'*_s'}",
             "/copyFields/[14]=={'source':'*_src_sub_i','dest':'*_dest_sub_s'}",
             "/copyFields/[15]=={'source':'*_src_sub_i','dest':'dest_sub_no_ast_s'}",

             "/copyFields/[16]=={'source':'src_sub_no_ast_i','dest':'*_s'}",
             "/copyFields/[17]=={'source':'src_sub_no_ast_i','dest':'*_dest_sub_s'}",
             "/copyFields/[18]=={'source':'src_sub_no_ast_i','dest':'dest_sub_no_ast_s'}");

  }

  @Test
  public void testRestrictSource() throws Exception {
    assertQ("/schema/copyfields/?wt=xml&source.fl=title,*_i,*_src_sub_i,src_sub_no_ast_i",
        "count(/response/arr[@name='copyFields']/lst) = 16", // 4 + 4 + 4 + 4
        "count(/response/arr[@name='copyFields']/lst/str[@name='source'][.='title']) = 4",
        "count(/response/arr[@name='copyFields']/lst/str[@name='source'][.='*_i']) = 4",
        "count(/response/arr[@name='copyFields']/lst/str[@name='source'][.='*_src_sub_i']) = 4",
        "count(/response/arr[@name='copyFields']/lst/str[@name='source'][.='src_sub_no_ast_i']) = 4");
  }

  @Test
  public void testRestrictDest() throws Exception {
    assertQ("/schema/copyfields/?wt=xml&dest.fl=title,*_s,*_dest_sub_s,dest_sub_no_ast_s",
        "count(/response/arr[@name='copyFields']/lst) = 16", // 3 + 4 + 4 + 5
        "count(/response/arr[@name='copyFields']/lst/str[@name='dest'][.='title']) = 3",
        "count(/response/arr[@name='copyFields']/lst/str[@name='dest'][.='*_s']) = 4",
        "count(/response/arr[@name='copyFields']/lst/str[@name='dest'][.='*_dest_sub_s']) = 4",
        "count(/response/arr[@name='copyFields']/lst/str[@name='dest'][.='dest_sub_no_ast_s']) = 5");
  }

  @Test
  public void testRestrictSourceAndDest() throws Exception {
    assertQ("/schema/copyfields/?wt=xml&source.fl=title,*_i&dest.fl=title,dest_sub_no_ast_s",
        "count(/response/arr[@name='copyFields']/lst) = 3",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='title']"
            + "                                      and str[@name='dest'][.='dest_sub_no_ast_s']]",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
            + "                                      and str[@name='dest'][.='title']]",

        "/response/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
            + "                                      and str[@name='dest'][.='dest_sub_no_ast_s']]");
  }
}
