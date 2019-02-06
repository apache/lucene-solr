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
import java.io.IOException;

import org.apache.solr.rest.SolrRestletTestBase;
import org.junit.Test;


public class TestFieldCollectionResource extends SolrRestletTestBase {
  @Test
  public void testXMLGetAllFields() throws Exception {
    assertQ("/schema/fields?wt=xml",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[1] = 'HTMLstandardtok'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[2] = 'HTMLwhitetok'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[3] = '_version_'");
  }


  @Test
  public void testGetAllFields() throws Exception {
    assertJQ("/schema/fields",
             "/fields/[0]/name=='HTMLstandardtok'",
             "/fields/[1]/name=='HTMLwhitetok'",
             "/fields/[2]/name=='_version_'");
  }

  @Test
  public void testXMLGetThreeFieldsDontIncludeDynamic() throws IOException {
    //
    assertQ("/schema/fields?wt=xml&fl=id,_version_,price_i",
        "count(/response/arr[@name='fields']/lst/str[@name='name']) = 2",
        "(/response/arr[@name='fields']/lst/str[@name='name'])[1] = 'id'",
        "(/response/arr[@name='fields']/lst/str[@name='name'])[2] = '_version_'");
  }

  @Test
  public void testXMLGetThreeFieldsIncludeDynamic() throws IOException {
    assertQ("/schema/fields?wt=xml&fl=id,_version_,price_i&includeDynamic=on",

        "count(/response/arr[@name='fields']/lst/str[@name='name']) = 3",

        "(/response/arr[@name='fields']/lst/str[@name='name'])[1] = 'id'",

        "(/response/arr[@name='fields']/lst/str[@name='name'])[2] = '_version_'",

        "(/response/arr[@name='fields']/lst/str[@name='name'])[3] = 'price_i'",

        "/response/arr[@name='fields']/lst[    str[@name='name']='price_i'    "
            +"                                  and str[@name='dynamicBase']='*_i']");
  }
  @Test
  public void testXMLNotFoundFields() throws IOException {
    assertQ("/schema/fields?&wt=xml&fl=not_in_there,this_one_either",
        "count(/response/arr[@name='fields']) = 1",
        "count(/response/arr[@name='fields']/lst/str[@name='name']) = 0");
  }


  @Test
  public void testGetAllFieldsIncludeDynamic() throws Exception {
    assertJQ("/schema/fields?includeDynamic=true",
             "/fields/[0]/name=='HTMLstandardtok'",
             "/fields/[1]/name=='HTMLwhitetok'",
             "/fields/[2]/name=='_version_'",
             "/fields/[107]/name=='*_d'",
             "/fields/[106]/name=='*_f'",
             "/fields/[105]/name=='*_b'",
             "/fields/[104]/name=='*_t'",
             "/fields/[103]/name=='*_l'"

    );
  }

}
