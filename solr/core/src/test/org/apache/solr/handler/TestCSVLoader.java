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
package org.apache.solr.handler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCSVLoader extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
  }

  String filename;
  File file;

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    File tempDir = createTempDir("TestCSVLoader").toFile();
    file = new File(tempDir, "solr_tmp.csv");
    filename = file.getPath();
    cleanup();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
    if (null != file) {
      Files.delete(file.toPath());
      file = null;
    }
  }

  void makeFile(String contents) {
    try (Writer out = new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8)) {
      out.write(contents);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void cleanup() {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  void loadLocal(String... args) throws Exception {
    LocalSolrQueryRequest req =  (LocalSolrQueryRequest)req(args);

    // TODO: stop using locally defined streams once stream.file and
    // stream.body work everywhere
    List<ContentStream> cs = new ArrayList<>(1);
    ContentStreamBase f = new ContentStreamBase.FileStream(new File(filename));
    f.setContentType("text/csv");
    cs.add(f);
    req.setContentStreams(cs);
    h.query("/update",req);
  }

  @Test
  public void testCSVLoad() throws Exception {
    makeFile("id\n100\n101\n102");
    loadLocal();
    // check default commit of false
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='3']");
  }

  @Test
  public void testCSVRowId() throws Exception {
    makeFile("id\n100\n101\n102");
    loadLocal("rowid", "rowid_i");//add a special field
    // check default commit of false
    assertU(commit());
    assertQ(req("rowid_i:1"),"//*[@numFound='1']");
    assertQ(req("rowid_i:2"),"//*[@numFound='1']");
    assertQ(req("rowid_i:100"),"//*[@numFound='0']");

    makeFile("id\n200\n201\n202");
    loadLocal("rowid", "rowid_i", "rowidOffset", "100");//add a special field
    // check default commit of false
    assertU(commit());
    assertQ(req("rowid_i:101"),"//*[@numFound='1']");
    assertQ(req("rowid_i:102"),"//*[@numFound='1']");
    assertQ(req("rowid_i:10000"),"//*[@numFound='0']");
  }

  @Test
  public void testCommitFalse() throws Exception {
    makeFile("id\n100\n101\n102");
    loadLocal("commit","false");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='0']");
    assertU(commit());
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='3']");
  }

  @Test
  public void testCommitTrue() throws Exception {
    makeFile("id\n100\n101\n102");
    loadLocal("commit","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='3']");
  }

  @Test
  public void testLiteral() throws Exception {
    makeFile("id\n100");
    loadLocal("commit","true", "literal.name","LITERAL_VALUE");
    assertQ(req("*:*"),"//doc/str[@name='name'][.='LITERAL_VALUE']");
  }


  @Test
  public void testCSV() throws Exception {
    lrf.args.put(CommonParams.VERSION,"2.2");
    
    makeFile("id,str_s\n100,\"quoted\"\n101,\n102,\"\"\n103,");
    loadLocal("commit","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='quoted']");
    assertQ(req("id:101"),"count(//str[@name='str_s'])=0");
    // 102 is a quoted zero length field ,"", as opposed to ,,
    // but we can't distinguish this case (and it's debateable
    // if we should).  Does CSV have a way to specify missing
    // from zero-length?
    assertQ(req("id:102"),"count(//str[@name='str_s'])=0");
    assertQ(req("id:103"),"count(//str[@name='str_s'])=0");

    // test overwrite by default
    loadLocal("commit","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");

    // test explicitly adding header=true (the default)
    loadLocal("commit","true","header","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");

    // test no overwrites
    loadLocal("commit","true", "overwrite","false");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='8']");

    // test overwrite
    loadLocal("commit","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");

    // test global value mapping
    loadLocal("commit","true", "map","quoted:QUOTED");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='QUOTED']");
    assertQ(req("id:101"),"count(//str[@name='str_s'])=0");
    assertQ(req("id:102"),"count(//str[@name='str_s'])=0");
    assertQ(req("id:103"),"count(//str[@name='str_s'])=0");

    // test value mapping to empty (remove)
    loadLocal("commit","true", "map","quoted:");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"count(//str[@name='str_s'])=0");

    // test value mapping from empty
    loadLocal("commit","true", "map",":EMPTY");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='quoted']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[.='EMPTY']");
    assertQ(req("id:102"),"//arr[@name='str_s']/str[.='EMPTY']");
    assertQ(req("id:103"),"//arr[@name='str_s']/str[.='EMPTY']");

    // test multiple map rules
    loadLocal("commit","true", "map",":EMPTY", "map","quoted:QUOTED");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='QUOTED']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[.='EMPTY']");
    assertQ(req("id:102"),"//arr[@name='str_s']/str[.='EMPTY']");
    assertQ(req("id:103"),"//arr[@name='str_s']/str[.='EMPTY']");

    // test indexing empty fields
    loadLocal("commit","true", "f.str_s.keepEmpty","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='quoted']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[.='']");
    assertQ(req("id:102"),"//arr[@name='str_s']/str[.='']");
    assertQ(req("id:103"),"//arr[@name='str_s']/str[.='']");

    // test overriding the name of fields
    loadLocal("commit","true",
             "fieldnames","id,my_s", "header","true",
             "f.my_s.map",":EMPTY");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='my_s']/str[.='quoted']");
    assertQ(req("id:101"),"count(//arr[@name='str_s']/str)=0");
    assertQ(req("id:102"),"count(//arr[@name='str_s']/str)=0");
    assertQ(req("id:103"),"count(//arr[@name='str_s']/str)=0");
    assertQ(req("id:101"),"//arr[@name='my_s']/str[.='EMPTY']");
    assertQ(req("id:102"),"//arr[@name='my_s']/str[.='EMPTY']");
    assertQ(req("id:103"),"//arr[@name='my_s']/str[.='EMPTY']");

    // test that header in file was skipped
    assertQ(req("id:id"),"//*[@numFound='0']");

    // test skipping a field via the "skip" parameter
    loadLocal("commit","true","keepEmpty","true","skip","str_s");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:[100 TO 110]"),"count(//str[@name='str_s']/str)=0");

    // test skipping a field by specifying an empty name
    loadLocal("commit","true","keepEmpty","true","fieldnames","id,");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:[100 TO 110]"),"count(//str[@name='str_s']/str)=0");

    // test loading file as if it didn't have a header
    loadLocal("commit","true",
             "fieldnames","id,my_s", "header","false");
    assertQ(req("id:id"),"//*[@numFound='1']");
    assertQ(req("id:100"),"//arr[@name='my_s']/str[.='quoted']");

    // test skipLines
    loadLocal("commit","true",
             "fieldnames","id,my_s", "header","false", "skipLines","1");
    assertQ(req("id:id"),"//*[@numFound='1']");
    assertQ(req("id:100"),"//arr[@name='my_s']/str[.='quoted']");


    // test multi-valued fields via field splitting w/ mapping of subvalues
    makeFile("id,str_s\n"
            +"100,\"quoted\"\n"
            +"101,\"a,b,c\"\n"
            +"102,\"a,,b\"\n"
            +"103,\n");
    loadLocal("commit","true",
              "f.str_s.map",":EMPTY",
              "f.str_s.split","true");
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='4']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='quoted']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[1][.='a']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[2][.='b']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[3][.='c']");
    assertQ(req("id:102"),"//arr[@name='str_s']/str[2][.='EMPTY']");
    assertQ(req("id:103"),"//arr[@name='str_s']/str[.='EMPTY']");


    // test alternate values for delimiters
    makeFile("id|str_s\n"
            +"100|^quoted^\n"
            +"101|a;'b';c\n"
            +"102|a;;b\n"
            +"103|\n"
            +"104|a\\\\b\n"  // no backslash escaping should be done by default
    );

    loadLocal("commit","true",
              "separator","|",
              "encapsulator","^",
              "f.str_s.map",":EMPTY",
              "f.str_s.split","true",
              "f.str_s.separator",";",
              "f.str_s.encapsulator","'"
    );
    assertQ(req("id:[100 TO 110]"),"//*[@numFound='5']");
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='quoted']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[1][.='a']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[2][.='b']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[3][.='c']");
    assertQ(req("id:102"),"//arr[@name='str_s']/str[2][.='EMPTY']");
    assertQ(req("id:103"),"//arr[@name='str_s']/str[.='EMPTY']");
    assertQ(req("id:104"),"//arr[@name='str_s']/str[.='a\\\\b']");

    // test no escaping + double encapsulator escaping by default
    makeFile("id,str_s\n"
            +"100,\"quoted \"\" \\ string\"\n"
            +"101,unquoted \"\" \\ string\n"     // double encap shouldn't be an escape outside encap
            +"102,end quote \\\n"
    );
    loadLocal("commit","true"
    );
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='quoted \" \\ string']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[.='unquoted \"\" \\ string']");
    assertQ(req("id:102"),"//arr[@name='str_s']/str[.='end quote \\']");


    // setting an escape should disable encapsulator
    makeFile("id,str_s\n"
            +"100,\"quoted \"\" \\\" \\\\ string\"\n"  // quotes should be part of value
            +"101,unquoted \"\" \\\" \\, \\\\ string\n"
    );
    loadLocal("commit","true"
            ,"escape","\\"
    );
    assertQ(req("id:100"),"//arr[@name='str_s']/str[.='\"quoted \"\" \" \\ string\"']");
    assertQ(req("id:101"),"//arr[@name='str_s']/str[.='unquoted \"\" \" , \\ string']");

  }

  

}
