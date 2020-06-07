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
package org.apache.solr.handler.dataimport;

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
 *
 * @since solr 1.3
 */
public class TestTemplateTransformer extends AbstractDataImportHandlerTestCase {

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow() {
    @SuppressWarnings({"rawtypes"})
    List fields = new ArrayList();
    fields.add(createMap("column", "firstName"));
    fields.add(createMap("column", "lastName"));
    fields.add(createMap("column", "middleName"));
    fields.add(createMap("column", "name",
            TemplateTransformer.TEMPLATE,
            "${e.lastName}, ${e.firstName} ${e.middleName}"));
    fields.add(createMap("column", "emails",
            TemplateTransformer.TEMPLATE,
            "${e.mail}"));

    // test reuse of template output in another template 
    fields.add(createMap("column", "mrname",
            TemplateTransformer.TEMPLATE,"Mr ${e.name}"));

    List<String> mails = Arrays.asList("a@b.com", "c@d.com");
    @SuppressWarnings({"rawtypes"})
    Map row = createMap(
            "firstName", "Shalin",
            "middleName", "Shekhar", 
            "lastName", "Mangar",
            "mail", mails);

    VariableResolver resolver = new VariableResolver();
    resolver.addNamespace("e", row);
    Map<String, String> entityAttrs = createMap("name", "e");

    Context context = getContext(null, resolver,
            null, Context.FULL_DUMP, fields, entityAttrs);
    new TemplateTransformer().transformRow(row, context);
    assertEquals("Mangar, Shalin Shekhar", row.get("name"));
    assertEquals("Mr Mangar, Shalin Shekhar", row.get("mrname"));
    assertEquals(mails,row.get("emails"));
  }
    
  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRowMultiValue() {
    @SuppressWarnings({"rawtypes"})
    List fields = new ArrayList();
    fields.add(createMap("column", "year"));
    fields.add(createMap("column", "month"));
    fields.add(createMap("column", "day"));
      
    // create three variations of date format
    fields.add(createMap( "column", "date",
                          TemplateTransformer.TEMPLATE,
                          "${e.day} ${e.month}, ${e.year}" ));
    fields.add(createMap( "column", "date",
                          TemplateTransformer.TEMPLATE,
                          "${e.month} ${e.day}, ${e.year}" ));
    fields.add(createMap("column", "date",
                          TemplateTransformer.TEMPLATE,
                          "${e.year}-${e.month}-${e.day}" ));
      
    @SuppressWarnings({"rawtypes"})
    Map row = createMap( "year", "2016",
                         "month", "Apr",
                         "day", "30" );
    VariableResolver resolver = new VariableResolver();
    resolver.addNamespace("e", row);
    Map<String, String> entityAttrs = createMap("date", "e");
      
    Context context = getContext(null, resolver,
                                 null, Context.FULL_DUMP, fields, entityAttrs);
    new TemplateTransformer().transformRow(row, context);
    assertTrue( row.get( "date" ) instanceof List );
    
    List<Object> dates = (List<Object>)row.get( "date" );
    assertEquals( dates.size(), 3 );
    assertEquals( dates.get(0).toString(), "30 Apr, 2016" );
    assertEquals( dates.get(1).toString(), "Apr 30, 2016" );
    assertEquals( dates.get(2).toString(), "2016-Apr-30" );
  }

}
