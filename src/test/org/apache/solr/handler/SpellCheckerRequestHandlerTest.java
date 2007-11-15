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

package org.apache.solr.handler;

import org.apache.solr.util.AbstractSolrTestCase;

/**
 * This is a test case to test the SpellCheckerRequestHandler class.
 * It tests: 
 * <ul>
 *   <li>The generation of the spell checkers list with a 10 words</li>
 *   <li>The identification of the word that was being spell checked</li>
 *   <li>The confirmation if the word exists or not in the index</li>
 *   <li>The suggested list of a correctly and incorrectly spelled words</li>
 *   <li>The suggestions for both correct and incorrect words</li>
 *   <li>The limitation on the number of suggestions with the 
 *       suggestionCount parameter</li>
 *   <li>The usage of the parameter multiWords</li>
 * </ul>
 * 
 * Notes/Concerns about this Test Case:
 * <ul>
 *   <li>This is my first test case for a Solr Handler.  As such I am not
 *       familiar with the AbstractSolrTestCase and as such I am not
 *       100% these test cases will work under the same for each person
 *       who runs the test cases (see next note).</li>
 *   <li>The order of the arrays (arr) may not be consistant on other 
 *       systems or different runs, as such these test cases may fail?</li>
 *   <li>Note: I changed //arr/str[1][.='cart'] to //arr/str[.='cart'] and it 
 *       appears to work.</li>
 *   <li>The two notations appear to successfully test for the same thing: 
 *       "//lst[@name='result']/lst[1][@name='word']/str[@name='words'][.='cat']" 
 *       and "//str[@name='words'][.='cat']" which I would think // would indicate 
 *       a root node.</li>
 * </ul>
 */
public class SpellCheckerRequestHandlerTest 
  extends AbstractSolrTestCase 
{

  @Override
  public String getSchemaFile() { return "solr/conf/schema-spellchecker.xml"; } 
  
  @Override
  public String getSolrConfigFile() { return "solr/conf/solrconfig-spellchecker.xml"; }
  
  @Override 
  public void setUp() throws Exception {
      super.setUp();
      

    }

  private void buildSpellCheckIndex()
  {
    lrf = h.getRequestFactory("spellchecker", 0, 20 );
    lrf.args.put("version","2.0");
    lrf.args.put("sp.query.accuracy",".9");

    assertU("Add some words to the Spell Check Index:",
        adoc("id",  "100",
             "spell", "solr"));
      assertU(adoc("id",  "101",
                   "spell", "cat"));
      assertU(adoc("id",  "102",
                   "spell", "cart"));
      assertU(adoc("id",  "103",
                   "spell", "carp"));
      assertU(adoc("id",  "104",
                   "spell", "cant"));
      assertU(adoc("id",  "105",
                   "spell", "catnip"));
      assertU(adoc("id",  "106",
                   "spell", "cattails"));
      assertU(adoc("id",  "107",
                   "spell", "cod"));
      assertU(adoc("id",  "108",
                   "spell", "corn"));
      assertU(adoc("id",  "109",
                   "spell", "cot"));

      assertU(commit());
      assertU(optimize());
      
      lrf.args.put("cmd","rebuild");
      assertQ("Need to first build the index:",
              req("cat")
              ,"//str[@name='cmdExecuted'][.='rebuild']"
              ,"//str[@name='words'][.='cat']"
              ,"//str[@name='exist'][.='true']"
      //        ,"//arr[@name='suggestions'][.='']"
              );
      lrf.args.clear();

  }
  
  /**
   * Test for correct spelling of a single word at various accuracy levels
   * to see how the suggestions vary.
   */
  public void testSpellCheck_01_correctWords() {
    
    buildSpellCheckIndex();
    
    lrf = h.getRequestFactory("spellchecker", 0, 20 );
    lrf.args.put("version","2.0");
    
    lrf.args.put("sp.query.accuracy",".9");
    assertQ("Failed to spell check",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            );

    lrf.args.put("sp.query.accuracy",".4");
    assertQ("Failed to spell check",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            ,"//arr/str[.='cot']"
            ,"//arr/str[.='cart']"
//            ,"//arr/str[1][.='cot']"
//            ,"//arr/str[2][.='cart']"
            );

    lrf.args.put("sp.query.accuracy",".0");
    assertQ("Failed to spell check",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            ,"//arr/str[.='cart']"
            ,"//arr/str[.='cot']"
            ,"//arr/str[.='carp']"
            ,"//arr/str[.='cod']"
            ,"//arr/str[.='corn']"
            );
  }

  /**
   * Test for correct spelling of a single word at various accuracy levels
   * to see how the suggestions vary.
   */
  public void testSpellCheck_02_incorrectWords() {
    
    buildSpellCheckIndex();

    lrf = h.getRequestFactory("spellchecker", 0, 20 );
    lrf.args.put("version","2.0");
    lrf.args.put("sp.query.accuracy",".9");
    
    assertQ("Confirm the index is still valid",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            );
    
    
    assertQ("Failed to spell check",
            req("coat")
            ,"//str[@name='words'][.='coat']"
            ,"//str[@name='exist'][.='false']"
            ,"//arr[@name='suggestions'][.='']"
            );
    
 
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
            req("coat")
            ,"//str[@name='words'][.='coat']"
            ,"//str[@name='exist'][.='false']"
            ,"//arr/str[.='cot']"
            ,"//arr/str[.='cat']"
            ,"//arr/str[.='corn']"
            ,"//arr/str[.='cart']"
            ,"//arr/str[.='cod']"
            ,"//arr/str[.='solr']"
            ,"//arr/str[.='carp']"
            );

    lrf.args.put("sp.query.suggestionCount", "2");
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
            req("coat")
            ,"//str[@name='words'][.='coat']"
            ,"//str[@name='exist'][.='false']"
            ,"//arr/str[.='cot']"
            ,"//arr/str[.='cat']"
            );
  }

  /**
   * Test for correct spelling of a single word at various accuracy levels
   * to see how the suggestions vary.
   */
  public void testSpellCheck_03_multiWords_correctWords() {
    
    buildSpellCheckIndex();

    lrf = h.getRequestFactory("spellchecker", 0, 20 );
    lrf.args.put("version","2.0");
    lrf.args.put("sp.query.accuracy",".9");
    
    assertQ("Confirm the index is still valid",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            );
    
    
    // Enable multiWords formatting:
    lrf.args.put("sp.query.extendedResults", "true");
    
    
    assertQ("Failed to spell check",
            req("cat")
            ,"//int[@name='numDocs'][.=10]"
            ,"//lst[@name='cat']"
            ,"//lst[@name='cat']/int[@name='frequency'][.>0]"
            ,"//lst[@name='cat']/lst[@name='suggestions' and count(lst)=0]"
            );
    
 
    // Please note that the following produces the following XML structure.
    //  <response>
    //    <responseHeader>
    //      <status>0</status><QTime>0</QTime>
    //    </responseHeader>
    //    <lst name="result">
    //      <lst name="cat">
    //        <int name="frequency">1</int>
    //        <lst name="suggestions">
    //          <lst name="cart"><int name="frequency">1</int></lst>
    //          <lst name="cot"><int name="frequency">1</int></lst>
    //          <lst name="cod"><int name="frequency">1</int></lst>
    //          <lst name="carp"><int name="frequency">1</int></lst>
    //        </lst>
    //      </lst>
    //    </lst>
    //  </response>


    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
            req("cat")
            ,"//int[@name='numDocs'][.=10]"
            ,"//lst[@name='cat']"
            ,"//lst[@name='cat']/int[@name='frequency'][.>0]"
            ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cart']/int[@name='frequency'][.>0]"
            ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cot']/int[@name='frequency'][.>0]"
            ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cod']/int[@name='frequency'][.>0]"
            ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='carp']/int[@name='frequency'][.>0]"
            );

    lrf.args.put("sp.query.suggestionCount", "2");
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
            req("cat")
            ,"//lst[@name='cat']"
            ,"//lst[@name='cat']/int[@name='frequency'][.>0]"
            ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cart']"
            ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cot']"
            );

    /* The following is the generated XML response for the next query with three words:
      <response>
        <responseHeader><status>0</status><QTime>0</QTime></responseHeader>
        <int name="numDocs">10</int>
        <lst name="result">
          <lst name="cat">
            <int name="frequency">1</int>
            <lst name="suggestions">
              <lst name="cart"><int name="frequency">1</int></lst>
              <lst name="cot"><int name="frequency">1</int></lst>
            </lst>
          </lst>
          <lst name="card">
            <int name="frequency">1</int>
            <lst name="suggestions">
              <lst name="carp"><int name="frequency">1</int></lst>
              <lst name="cat"><int name="frequency">1</int></lst>
            </lst>
          </lst>
          <lst name="carp">
            <int name="frequency">1</int>
            <lst name="suggestions">
              <lst name="cart"><int name="frequency">1</int></lst>
              <lst name="corn"><int name="frequency">1</int></lst>
            </lst>
          </lst>
        </lst>
      </response>
    */

    lrf.args.put("sp.query.suggestionCount", "2");
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
        req("cat cart carp")
        ,"//lst[@name='cat']"
        ,"//lst[@name='cat']/int[@name='frequency'][.>0]"
        ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cart']"
        ,"//lst[@name='cat']/lst[@name='suggestions']/lst[@name='cot']"

        ,"//lst[@name='cart']"
        ,"//lst[@name='cart']/int[@name='frequency'][.>0]"
        ,"//lst[@name='cart']/lst/lst[1]"
        ,"//lst[@name='cart']/lst/lst[2]"

        ,"//lst[@name='carp']"
        ,"//lst[@name='carp']/int[@name='frequency'][.>0]"
        ,"//lst[@name='carp']/lst[@name='suggestions']/lst[@name='cart']"
        ,"//lst[@name='carp']/lst[@name='suggestions']/lst[@name='corn']"

    );

  }
  
  /**
   * Test for correct spelling of a single word at various accuracy levels
   * to see how the suggestions vary.
   */
  public void testSpellCheck_04_multiWords_incorrectWords() {
    
    buildSpellCheckIndex();

    lrf = h.getRequestFactory("spellchecker", 0, 20 );
    lrf.args.put("version","2.0");
    lrf.args.put("sp.query.accuracy",".9");
    
    assertQ("Confirm the index is still valid",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            );
    
    
    // Enable multiWords formatting:
    lrf.args.put("sp.query.extendedResults", "true");
    
    
    assertQ("Failed to spell check",
            req("coat")
            ,"//int[@name='numDocs'][.=10]"
            ,"//lst[@name='coat']"
            ,"//lst[@name='coat']/int[@name='frequency'][.=0]"
            ,"//lst[@name='coat']/lst[@name='suggestions' and count(lst)=0]"
            );
 
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
            req("coat")
            ,"//lst[@name='coat']"
            ,"//lst[@name='coat']/int[@name='frequency'][.=0]"
            ,"//lst[@name='coat']/lst[@name='suggestions']/lst[@name='cot']"
            ,"//lst[@name='coat']/lst[@name='suggestions']/lst[@name='cat']"
            ,"//lst[@name='coat']/lst[@name='suggestions']/lst[@name='corn']"
            ,"//lst[@name='coat']/lst[@name='suggestions']/lst[@name='cart']"
            );

    lrf.args.put("sp.query.suggestionCount", "2");
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
            req("coat")
            ,"//lst[@name='coat']"
            ,"//lst[@name='coat']/int[@name='frequency'][.=0]"
            ,"//lst[@name='coat']/lst[@name='suggestions']/lst[@name='cot']"
            ,"//lst[@name='coat']/lst[@name='suggestions']/lst[@name='cat']"
            );

  
  
    lrf.args.put("sp.query.suggestionCount", "2");
    lrf.args.put("sp.query.accuracy",".2");
    assertQ("Failed to spell check",
        req("cet cert corp")
        ,"//int[@name='numDocs'][.=10]"
        ,"//lst[@name='cet']"
        ,"//lst[@name='cet']/int[@name='frequency'][.=0]"
        ,"//lst[@name='cet']/lst[@name='suggestions']/lst[1]"
        ,"//lst[@name='cet']/lst[@name='suggestions']/lst[2]"
  
        ,"//lst[@name='cert']"
        ,"//lst[@name='cert']/int[@name='frequency'][.=0]"
        ,"//lst[@name='cert']/lst[@name='suggestions']/lst[1]"
        ,"//lst[@name='cert']/lst[@name='suggestions']/lst[2]"
  
        ,"//lst[@name='corp']"
        ,"//lst[@name='corp']/int[@name='frequency'][.=0]"
        ,"//lst[@name='corp']/lst[@name='suggestions']/lst[1]"
        ,"//lst[@name='corp']/lst[@name='suggestions']/lst[2]"
  
      );
  
  }

  public void testSpellCheck_05_buildDictionary() {
    lrf = h.getRequestFactory("spellchecker", 0, 20 );
    lrf.args.put("version","2.0");
    lrf.args.put("sp.query.accuracy",".9");

    assertU("Add some words to the Spell Check Index:",
      adoc("id",  "100",
             "spell", "solr cat cart"));
    assertU(adoc("id",  "101",
                   "spell", "cat cart"));
    assertU(adoc("id",  "102",
                   "spell", "cat cart"));
    assertU(adoc("id",  "103",
                   "spell", "cat cart carp"));
    assertU(adoc("id",  "104",
                   "spell", "cat car cant"));
    assertU(adoc("id",  "105",
                   "spell", "cat catnip"));
    assertU(adoc("id",  "106",
                   "spell", "cat cattails"));
    assertU(adoc("id",  "107",
                   "spell", "cat cod"));
    assertU(adoc("id",  "108",
                   "spell", "cat corn"));
    assertU(adoc("id",  "109",
                   "spell", "cat cot"));
    assertU(commit());
    assertU(optimize());

    lrf.args.put("sp.dictionary.threshold", "0.20");
    lrf.args.put("cmd","rebuild");
    assertQ("Need to first build the index:",
            req("cat")
            ,"//str[@name='cmdExecuted'][.='rebuild']"
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            );

    lrf.args.clear();
    lrf.args.put("version","2.0");
    lrf.args.put("sp.query.accuracy",".9");

    assertQ("Confirm index contains only words above threshold",
            req("cat")
            ,"//str[@name='words'][.='cat']"
            ,"//str[@name='exist'][.='true']"
            );

    assertQ("Confirm index contains only words above threshold",
            req("cart")
            ,"//str[@name='words'][.='cart']"
            ,"//str[@name='exist'][.='true']"
            );

    assertQ("Confirm index contains only words above threshold",
            req("cod")
            ,"//str[@name='words'][.='cod']"
            ,"//str[@name='exist'][.='false']"
            );

    assertQ("Confirm index contains only words above threshold",
            req("corn")
            ,"//str[@name='words'][.='corn']"
            ,"//str[@name='exist'][.='false']"
            );

    lrf.args.clear();
  }
}
