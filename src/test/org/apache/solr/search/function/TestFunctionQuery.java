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
import org.apache.solr.core.SolrCore;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.io.File;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;

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

  String base = "external_foo_extf";
  void makeExternalFile(String field, String contents, String charset) {
    String dir = h.getCore().getIndexDir();
    String filename = dir + "/external_" + field + "." + System.currentTimeMillis();
    try {
      Writer out = new OutputStreamWriter(new FileOutputStream(filename), charset);
      out.write(contents);
      out.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  void createIndex(String field, float... values) {
    // lrf.args.put("version","2.0");
    for (float val : values) {
      String s = Float.toString(val);
      if (field!=null) assertU(adoc("id", s, field, s));
      else assertU(adoc("id", s));
      // System.out.println("added doc for " + val);
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
      String xpath = "//doc[./float[@name='" + "id" + "']='"
              + results[i] + "' and ./float[@name='score']='"
              + results[i+1] + "']";
      tests.add(xpath);
    }

    assertQ(req("q", parseableQuery
                ,"fl", "*,score","indent","on","rows","100"
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

  public void testExternalField() {
    String field = "foo_extf";

    float[] ids = {100,-4,0,10,25,5,77,23,55,-78,-45,-24,63,78,94,22,34,54321,261,-627};

    createIndex(null,ids);

    // Unsorted field, largest first
    makeExternalFile(field, "54321=543210\n0=-999\n25=250","UTF-8");
    // test identity (straight field value)
    singleTest(field, "\0", 54321, 543210, 0,-999, 25,250, 100, 1);
    Object orig = FileFloatSource.onlyForTesting;
    singleTest(field, "log(\0)");
    // make sure the values were cached
    assertTrue(orig == FileFloatSource.onlyForTesting);
    singleTest(field, "sqrt(\0)");
    assertTrue(orig == FileFloatSource.onlyForTesting);

    makeExternalFile(field, "0=1","UTF-8");
    assertU(commit());
    assertTrue(orig != FileFloatSource.onlyForTesting);


    Random r = new Random();
    for (int i=0; i<10; i++) {   // do more iterations for a thorough test
      int len = r.nextInt(ids.length+1);
      boolean sorted = r.nextBoolean();
      // shuffle ids
      for (int j=0; j<ids.length; j++) {
        int other=r.nextInt(ids.length);
        float v=ids[0];
        ids[0] = ids[other];
        ids[other] = v;
      }

      if (sorted) {
        // sort only the first elements
        Arrays.sort(ids,0,len);
      }

      // make random values
      float[] vals = new float[len];
      for (int j=0; j<len; j++) {
        vals[j] = r.nextInt(200)-100;
      }

      // make and write the external file
      StringBuilder sb = new StringBuilder();
      for (int j=0; j<len; j++) {
        sb.append("" + ids[j] + "=" + vals[j]+"\n");        
      }
      makeExternalFile(field, sb.toString(),"UTF-8");

      // make it visible
      assertU(commit());

      // test it
      float[] answers = new float[ids.length*2];
      for (int j=0; j<len; j++) {
        answers[j*2] = ids[j];
        answers[j*2+1] = vals[j];
      }
      for (int j=len; j<ids.length; j++) {
        answers[j*2] = ids[j];
        answers[j*2+1] = 1;  // the default values
      }

      singleTest(field, "\0", answers);
      System.out.println("Done test "+i);
    }

  }

}