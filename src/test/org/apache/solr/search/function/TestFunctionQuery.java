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

package org.apache.solr.search.function;

import org.apache.solr.util.AbstractSolrTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class TestFunctionQuery extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema11.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  public String getCoreName() { return "basic"; }

  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
  }

  void createIndex(String field, float... values) {
    // lrf.args.put("version","2.0");
    for (float val : values) {
      String s = Float.toString(val);
      assertU(adoc("id", s, field, s));
      System.out.println("added doc for " + val);
    }
    assertU(optimize()); // squeeze out any possible deleted docs
  }

  // replace \0 with the field name and create a parseable string 
  public String func(String field, String template) {
    StringBuilder sb = new StringBuilder("_val_:\"");
    for (char ch : template.toCharArray()) {
      if (ch=='\0') {
        sb.append(field);
        continue;
      }
      if (ch=='"') sb.append('\\');
      sb.append(ch);
    }
    sb.append('"');
    return sb.toString();
  }

  void singleTest(String field, String funcTemplate, float... results) {
    // lrf.args.put("version","2.0");
    String parseableQuery = func(field, funcTemplate);
    List<String> tests = new ArrayList<String>();

    // Construct xpaths like the following:
    // "//doc[./float[@name='foo_pf']='10.0' and ./float[@name='score']='10.0']"

    for (int i=0; i<results.length; i+=2) {
      String xpath = "//doc[./float[@name='" + field + "']='"
              + results[i] + "' and ./float[@name='score']='"
              + results[i+1] + "']";
      tests.add(xpath);
    }

    assertQ(req("q", parseableQuery
                ,"fl", "*,score"
                )
            , tests.toArray(new String[tests.size()])
            );
  }

  void doTest(String field) {
    // lrf.args.put("version","2.0");
    float[] vals = new float[] {
      100,-4,0,10,25,5
    };
    createIndex(field,vals);

    // test identity (straight field value)
    singleTest(field, "\0", 10,10);

    // test constant score
    singleTest(field,"1.414213", 10, 1.414213f);
    singleTest(field,"-1.414213", 10, -1.414213f);

    singleTest(field,"sum(\0,1)", 10, 11);
    singleTest(field,"sum(\0,\0)", 10, 20);
    singleTest(field,"sum(\0,\0,5)", 10, 25);

    singleTest(field,"product(\0,1)", 10, 10);
    singleTest(field,"product(\0,-2,-4)", 10, 80);

    singleTest(field,"log(\0)",10,1, 100,2);
    singleTest(field,"sqrt(\0)",100,10, 25,5, 0,0);
    singleTest(field,"abs(\0)",10,10, -4,4);
    singleTest(field,"pow(\0,\0)",0,1, 5,3125);
    singleTest(field,"pow(\0,0.5)",100,10, 25,5, 0,0);
    singleTest(field,"div(1,\0)",-4,-.25f, 10,.1f, 100,.01f);
    singleTest(field,"div(1,1)",-4,1, 10,1);

    singleTest(field,"sqrt(abs(\0))",-4,2);
    singleTest(field,"sqrt(sum(29,\0))",-4,5);

    singleTest(field,"map(\0,0,0,500)",10,10, -4,-4, 0,500);
    singleTest(field,"map(\0,-4,5,500)",100,100, -4,500, 0,500, 5,500, 10,10, 25,25);

    singleTest(field,"scale(\0,-1,1)",-4,-1, 100,1, 0,-0.9230769f);
    singleTest(field,"scale(\0,-10,1000)",-4,-10, 100,1000, 0,28.846153f);

    // test that infinity doesn't mess up scale function
    singleTest(field,"scale(log(\0),-1000,1000)",100,1000);

  }

  public void testFunctions() {
    doTest("foo_pf");  // a plain float field
    doTest("foo_f");  // a sortable float field
  }
}
