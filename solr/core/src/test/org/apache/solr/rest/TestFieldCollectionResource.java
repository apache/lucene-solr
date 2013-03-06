package org.apache.solr.rest;
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

import org.junit.Test;

import java.io.IOException;

public class TestFieldCollectionResource extends SchemaRestletTestBase {
  @Test
  public void testGetAllFields() throws Exception {
    assertQ("/schema/fields?indent=on&wt=xml",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[1] = 'custstopfilt'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[2] = 'lowerfilt'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[3] = 'test_basictv'",
            "count(//copySources/str) = count(//copyDests/str)");
  }

  @Test
  public void testGetTwoFields() throws IOException {
    assertQ("/schema/fields?indent=on&wt=xml&fl=id,_version_",
            "count(/response/arr[@name='fields']/lst/str[@name='name']) = 2",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[1] = 'id'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[2] = '_version_'");
  }

  @Test
  public void testNotFoundFields() throws IOException {
    assertQ("/schema/fields?indent=on&wt=xml&fl=not_in_there,this_one_either",
            "count(/response/arr[@name='fields']) = 1",
            "count(/response/arr[@name='fields']/lst/str[@name='name']) = 0");
  }

  @Test
  public void testJsonGetAllFields() throws Exception {
    assertJQ("/schema/fields?indent=on",
             "/fields/[0]/name=='custstopfilt'",
             "/fields/[1]/name=='lowerfilt'",
             "/fields/[2]/name=='test_basictv'");
  }

  @Test
  public void testJsonGetTwoFields() throws Exception {
    assertJQ("/schema/fields?indent=on&fl=id,_version_&wt=xml", // assertJQ should fix the wt param to be json
             "/fields/[0]/name=='id'",
             "/fields/[1]/name=='_version_'");
  }
}
