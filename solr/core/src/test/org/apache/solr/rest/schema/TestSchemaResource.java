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

public class TestSchemaResource extends SolrRestletTestBase {
  @Test
  public void testXMLResponse() throws Exception {
    assertQ("/schema/?indent=on&wt=xml",  // should work with or without trailing slash on '/schema/' path

            "count(/response/lst[@name='schema']/str[@name='name']) = 1",
            "/response/lst[@name='schema']/str[@name='name'][.='test-rest']",

            "count(/response/lst[@name='schema']/float[@name='version']) = 1",
            "/response/lst[@name='schema']/float[@name='version'][.='1.6']",

            "count(/response/lst[@name='schema']/str[@name='uniqueKey']) = 1",
            "/response/lst[@name='schema']/str[@name='uniqueKey'][.='id']",
        
            "(/response/lst[@name='schema']/arr[@name='fieldTypes']/lst/str[@name='name'])[1] = 'HTMLstandardtok'",
            "(/response/lst[@name='schema']/arr[@name='fieldTypes']/lst/str[@name='name'])[2] = 'HTMLwhitetok'",
            "(/response/lst[@name='schema']/arr[@name='fieldTypes']/lst/str[@name='name'])[3] = 'boolean'",

            "(/response/lst[@name='schema']/arr[@name='fields']/lst/str[@name='name'])[1] = 'HTMLstandardtok'",
            "(/response/lst[@name='schema']/arr[@name='fields']/lst/str[@name='name'])[2] = 'HTMLwhitetok'",
            "(/response/lst[@name='schema']/arr[@name='fields']/lst/str[@name='name'])[3] = '_version_'",
        
            "(/response/lst[@name='schema']/arr[@name='dynamicFields']/lst/str[@name='name'])[1] = '*_coordinate'",
            "(/response/lst[@name='schema']/arr[@name='dynamicFields']/lst/str[@name='name'])[2] = 'ignored_*'",
            "(/response/lst[@name='schema']/arr[@name='dynamicFields']/lst/str[@name='name'])[3] = '*_mfacet'",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='title']"
           +"                                                          and str[@name='dest'][.='title_stemmed']"
           +"                                                          and int[@name='maxChars'][.='200']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='title']"
           +"                                                          and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                                          and str[@name='dest'][.='title']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                                          and str[@name='dest'][.='*_s']]",
       
            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                                          and str[@name='dest'][.='*_dest_sub_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_i']"
           +"                                                          and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                                          and str[@name='dest'][.='title']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                                          and str[@name='dest'][.='*_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                                          and str[@name='dest'][.='*_dest_sub_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='*_src_sub_i']"
           +"                                                          and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                                          and str[@name='dest'][.='title']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                                          and str[@name='dest'][.='*_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                                          and str[@name='dest'][.='*_dest_sub_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='src_sub_no_ast_i']"
           +"                                                          and str[@name='dest'][.='dest_sub_no_ast_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                                          and str[@name='dest'][.='text']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                                          and str[@name='dest'][.='*_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                                          and str[@name='dest'][.='*_dest_sub_s']]",

            "/response/lst[@name='schema']/arr[@name='copyFields']/lst[    str[@name='source'][.='title_*']"
           +"                                                          and str[@name='dest'][.='dest_sub_no_ast_s']]");
  }


  @Test
  public void testJSONResponse() throws Exception {
    assertJQ("/schema", // Should work with or without a trailing slash

             "/schema/name=='test-rest'",
             "/schema/version==1.6",
             "/schema/uniqueKey=='id'",

             "/schema/fieldTypes/[0]/name=='HTMLstandardtok'",
             "/schema/fieldTypes/[1]/name=='HTMLwhitetok'",
             "/schema/fieldTypes/[2]/name=='boolean'",
        
             "/schema/fields/[0]/name=='HTMLstandardtok'",
             "/schema/fields/[1]/name=='HTMLwhitetok'",
             "/schema/fields/[2]/name=='_version_'",
        
             "/schema/dynamicFields/[0]/name=='*_coordinate'",
             "/schema/dynamicFields/[1]/name=='ignored_*'",
             "/schema/dynamicFields/[2]/name=='*_mfacet'",
                 
             "/schema/copyFields/[1]=={'source':'src_sub_no_ast_i','dest':'title'}",

             "/schema/copyFields/[7]=={'source':'title','dest':'dest_sub_no_ast_s'}",
             "/schema/copyFields/[8]=={'source':'*_i','dest':'title'}",
             "/schema/copyFields/[9]=={'source':'*_i','dest':'*_s'}",
             "/schema/copyFields/[10]=={'source':'*_i','dest':'*_dest_sub_s'}",
             "/schema/copyFields/[11]=={'source':'*_i','dest':'dest_sub_no_ast_s'}",

             "/schema/copyFields/[12]=={'source':'*_src_sub_i','dest':'title'}",
             "/schema/copyFields/[13]=={'source':'*_src_sub_i','dest':'*_s'}",
             "/schema/copyFields/[14]=={'source':'*_src_sub_i','dest':'*_dest_sub_s'}",
             "/schema/copyFields/[15]=={'source':'*_src_sub_i','dest':'dest_sub_no_ast_s'}",

             "/schema/copyFields/[16]=={'source':'src_sub_no_ast_i','dest':'*_s'}",
             "/schema/copyFields/[17]=={'source':'src_sub_no_ast_i','dest':'*_dest_sub_s'}",
             "/schema/copyFields/[18]=={'source':'src_sub_no_ast_i','dest':'dest_sub_no_ast_s'}");

  }

  @Test
  public void testSchemaXmlResponse() {
    assertQ("/schema?wt=schema.xml",  // should work with or without trailing slash on '/schema/' path

            "/schema/@name = 'test-rest'",
            "/schema/@version = '1.6'",
            "/schema/uniqueKey = 'id'",

            "(/schema/fieldType)[1]/@name = 'HTMLstandardtok'",
            "(/schema/fieldType)[2]/@name = 'HTMLwhitetok'",
            "(/schema/fieldType)[3]/@name = 'boolean'",

            "(/schema/field)[1]/@name = 'HTMLstandardtok'",
            "(/schema/field)[2]/@name = 'HTMLwhitetok'",
            "(/schema/field)[3]/@name = '_version_'",

            "(/schema/dynamicField)[1]/@name = '*_coordinate'",
            "(/schema/dynamicField)[2]/@name = 'ignored_*'",
            "(/schema/dynamicField)[3]/@name = '*_mfacet'",

            "/schema/copyField[@source='title'][@dest='title_stemmed'][@maxChars='200']",
            "/schema/copyField[@source='title'][@dest='dest_sub_no_ast_s']",
            "/schema/copyField[@source='*_i'][@dest='title']",
            "/schema/copyField[@source='*_i'][@dest='*_s']",
            "/schema/copyField[@source='*_i'][@dest='*_dest_sub_s']",
            "/schema/copyField[@source='*_i'][@dest='dest_sub_no_ast_s']",
            "/schema/copyField[@source='*_src_sub_i'][@dest='title']",
            "/schema/copyField[@source='*_src_sub_i'][@dest='*_s']",
            "/schema/copyField[@source='*_src_sub_i'][@dest='*_dest_sub_s']",
            "/schema/copyField[@source='*_src_sub_i'][@dest='dest_sub_no_ast_s']",
            "/schema/copyField[@source='src_sub_no_ast_i'][@dest='title']",
            "/schema/copyField[@source='src_sub_no_ast_i'][@dest='*_s']",
            "/schema/copyField[@source='src_sub_no_ast_i'][@dest='*_dest_sub_s']",
            "/schema/copyField[@source='src_sub_no_ast_i'][@dest='dest_sub_no_ast_s']",
            "/schema/copyField[@source='title_*'][@dest='text']",
            "/schema/copyField[@source='title_*'][@dest='*_s']",
            "/schema/copyField[@source='title_*'][@dest='*_dest_sub_s']",
            "/schema/copyField[@source='title_*'][@dest='dest_sub_no_ast_s']");
  }
}
