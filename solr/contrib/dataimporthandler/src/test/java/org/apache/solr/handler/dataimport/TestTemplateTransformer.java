/**
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
package org.apache.solr.handler.dataimport;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

/**
 * <p>
 * Test for TemplateTransformer
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestTemplateTransformer extends SolrTestCaseJ4 {

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow() {
    List fields = new ArrayList();
    fields.add(AbstractDataImportHandlerTestCase.createMap("column", "firstName"));
    fields.add(AbstractDataImportHandlerTestCase.createMap("column", "lastName"));
    fields.add(AbstractDataImportHandlerTestCase.createMap("column", "middleName"));
    fields.add(AbstractDataImportHandlerTestCase.createMap("column", "name",
            TemplateTransformer.TEMPLATE,
            "${e.lastName}, ${e.firstName} ${e.middleName}"));
    fields.add(AbstractDataImportHandlerTestCase.createMap("column", "emails",
            TemplateTransformer.TEMPLATE,
            "${e.mail}"));

    // test reuse of template output in another template 
    fields.add(AbstractDataImportHandlerTestCase.createMap("column", "mrname",
            TemplateTransformer.TEMPLATE,"Mr ${e.name}"));

    List<String> mails = Arrays.asList(new String[]{"a@b.com", "c@d.com"});
    Map row = AbstractDataImportHandlerTestCase.createMap(
            "firstName", "Shalin",
            "middleName", "Shekhar", 
            "lastName", "Mangar",
            "mail", mails);

    VariableResolverImpl resolver = new VariableResolverImpl();
    resolver.addNamespace("e", row);
    Map<String, String> entityAttrs = AbstractDataImportHandlerTestCase.createMap(
            "name", "e");

    Context context = AbstractDataImportHandlerTestCase.getContext(null, resolver,
            null, Context.FULL_DUMP, fields, entityAttrs);
    new TemplateTransformer().transformRow(row, context);
    Assert.assertEquals("Mangar, Shalin Shekhar", row.get("name"));
    Assert.assertEquals("Mr Mangar, Shalin Shekhar", row.get("mrname"));
    Assert.assertEquals(mails,row.get("emails"));
  }

}
