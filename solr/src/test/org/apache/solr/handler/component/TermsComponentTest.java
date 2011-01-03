package org.apache.solr.handler.component;
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.TermsParams;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 *
 *
 **/
public class TermsComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTest() throws Exception {
    initCore("solrconfig.xml","schema12.xml");

    assertNull(h.validateUpdate(adoc("id", "0", "lowerfilt", "a", "standardfilt", "a", "foo_i","1")));
    assertNull(h.validateUpdate(adoc("id", "1", "lowerfilt", "a", "standardfilt", "aa", "foo_i","1")));
    assertNull(h.validateUpdate(adoc("id", "2", "lowerfilt", "aa", "standardfilt", "aaa", "foo_i","2")));
    assertNull(h.validateUpdate(adoc("id", "3", "lowerfilt", "aaa", "standardfilt", "abbb")));
    assertNull(h.validateUpdate(adoc("id", "4", "lowerfilt", "ab", "standardfilt", "b")));
    assertNull(h.validateUpdate(adoc("id", "5", "lowerfilt", "abb", "standardfilt", "bb")));
    assertNull(h.validateUpdate(adoc("id", "6", "lowerfilt", "abc", "standardfilt", "bbbb")));
    assertNull(h.validateUpdate(adoc("id", "7", "lowerfilt", "b", "standardfilt", "c")));
    assertNull(h.validateUpdate(adoc("id", "8", "lowerfilt", "baa", "standardfilt", "cccc")));
    assertNull(h.validateUpdate(adoc("id", "9", "lowerfilt", "bbb", "standardfilt", "ccccc")));

    assertNull(h.validateUpdate(adoc("id", "10", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "11", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "12", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "13", "standardfilt", "ddddd")));
    assertNull(h.validateUpdate(adoc("id", "14", "standardfilt", "d")));
    assertNull(h.validateUpdate(adoc("id", "15", "standardfilt", "d")));
    assertNull(h.validateUpdate(adoc("id", "16", "standardfilt", "d")));

    assertNull(h.validateUpdate(adoc("id", "17", "standardfilt", "snake")));
    assertNull(h.validateUpdate(adoc("id", "18", "standardfilt", "spider")));
    assertNull(h.validateUpdate(adoc("id", "19", "standardfilt", "shark")));
    assertNull(h.validateUpdate(adoc("id", "20", "standardfilt", "snake")));
    assertNull(h.validateUpdate(adoc("id", "21", "standardfilt", "snake")));
    assertNull(h.validateUpdate(adoc("id", "22", "standardfilt", "shark")));
    
    assertNull(h.validateUpdate(commit()));
  }

  @Test
  public void testEmptyLower() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true", "terms.fl","lowerfilt", "terms.upper","b")
        ,"count(//lst[@name='lowerfilt']/*)=6"
        ,"//int[@name='a'] "
        ,"//int[@name='aa'] "
        ,"//int[@name='aaa'] "
        ,"//int[@name='ab'] "
        ,"//int[@name='abb'] "
        ,"//int[@name='abc'] "
    );
  }


  @Test
  public void testMultipleFields() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt", "terms.upper","b",
        "terms.fl","standardfilt")
        ,"count(//lst[@name='lowerfilt']/*)=6"
        ,"count(//lst[@name='standardfilt']/*)=4"
    );

  }

  @Test
  public void testUnlimitedRows() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        "terms.fl","standardfilt",
        "terms.rows","-1")
        ,"count(//lst[@name='lowerfilt']/*)=9"
        ,"count(//lst[@name='standardfilt']/*)=10"
    );


  }

  @Test
  public void testPrefix() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt", "terms.upper","b",
        "terms.fl","standardfilt",
        "terms.lower","aa", "terms.lower.incl","false", "terms.prefix","aa", "terms.upper","b", "terms.limit","50")
        ,"count(//lst[@name='lowerfilt']/*)=1"
        ,"count(//lst[@name='standardfilt']/*)=1"
    );
  }

  @Test
  public void testRegexp() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","a", "terms.lower.incl","false",
        "terms.upper","c", "terms.upper.incl","true",
        "terms.regex","b.*")
        ,"count(//lst[@name='standardfilt']/*)=3"        
    );
  }

  @Test
  public void testRegexpFlagParsing() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(TermsParams.TERMS_REGEXP_FLAG, "case_insensitive", "literal", "comments", "multiline", "unix_lines",
              "unicode_case", "dotall", "canon_eq");
      int flags = new TermsComponent().resolveRegexpFlags(params);
      int expected = Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.COMMENTS | Pattern.MULTILINE | Pattern.UNIX_LINES
              | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ;
      assertEquals(expected, flags);
  }

  @Test
  public void testRegexpWithFlags() throws Exception {
    // TODO: there are no uppercase or mixed-case terms in the index!
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","a", "terms.lower.incl","false",
        "terms.upper","c", "terms.upper.incl","true",
        "terms.regex","B.*",
        "terms.regex.flag","case_insensitive")
        ,"count(//lst[@name='standardfilt']/*)=3"               
    );
  }

  @Test
  public void testSortCount() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","s", "terms.lower.incl","false",
        "terms.prefix","s",
        "terms.sort","count")
        ,"count(//lst[@name='standardfilt']/*)=3"
        ,"//lst[@name='standardfilt']/int[1][@name='snake'][.='3']"
        ,"//lst[@name='standardfilt']/int[2][@name='shark'][.='2']"
        ,"//lst[@name='standardfilt']/int[3][@name='spider'][.='1']"
    );
  
  }

  @Test
  public void testSortIndex() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","s", "terms.lower.incl","false",
        "terms.prefix","s",
        "terms.sort","index")
        ,"count(//lst[@name='standardfilt']/*)=3"
        ,"//lst[@name='standardfilt']/int[1][@name='shark'][.='2']"
        ,"//lst[@name='standardfilt']/int[2][@name='snake'][.='3']"
        ,"//lst[@name='standardfilt']/int[3][@name='spider'][.='1']"
    );
  }
  
  @Test
  public void testPastUpper() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        //no upper bound, lower bound doesn't exist
        "terms.lower","d")
        ,"count(//lst[@name='standardfilt']/*)=0"
    );
  }

  @Test
  public void testLowerExclusive() throws Exception {
     assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        "terms.lower","a", "terms.lower.incl","false",
        "terms.upper","b")
        ,"count(//lst[@name='lowerfilt']/*)=5"
        ,"//int[@name='aa'] "
        ,"//int[@name='aaa'] "
        ,"//int[@name='ab'] "
        ,"//int[@name='abb'] "
        ,"//int[@name='abc'] "
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","cc", "terms.lower.incl","false",
        "terms.upper","d")
        ,"count(//lst[@name='standardfilt']/*)=2"
    );
  }

  @Test
  public void test() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","lowerfilt",
       "terms.lower","a",
       "terms.upper","b")
       ,"count(//lst[@name='lowerfilt']/*)=6"
       ,"//int[@name='a'] "
       ,"//int[@name='aa'] "
       ,"//int[@name='aaa'] "
       ,"//int[@name='ab'] "
       ,"//int[@name='abb'] "
       ,"//int[@name='abc'] "
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","lowerfilt",
       "terms.lower","a",
       "terms.upper","b",
       "terms.raw","true",    // this should have no effect on a text field
       "terms.limit","2")
       ,"count(//lst[@name='lowerfilt']/*)=2"
       ,"//int[@name='a']"
       ,"//int[@name='aa']"
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","foo_i")
       ,"//int[@name='1'][.='2']"
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","foo_i", "terms.raw","true")
       ,"not(//int[@name='1'][.='2'])"
    );

    // check something at the end of the index
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","zzz_i")
        ,"count(//lst[@name='zzz_i']/*)=0"
    );
  }

  @Test
  public void testMinMaxFreq() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","lowerfilt",
       "terms.lower","a",
       "terms.mincount","2",
       "terms.maxcount","-1",
       "terms.limit","50")
       ,"count(//lst[@name='lowerfilt']/*)=1"
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","standardfilt",
       "terms.lower","d",
       "terms.mincount","2",
       "terms.maxcount","3",
       "terms.limit","50")
       ,"count(//lst[@name='standardfilt']/*)=3"
    );
  }
}
