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
package org.apache.solr.spelling;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.util.LuceneTestCase.SuppressTempFileChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressTempFileChecks(bugUrl = "https://issues.apache.org/jira/browse/SOLR-1877 Spellcheck IndexReader leak bug?")
public class WordBreakSolrSpellCheckerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-spellcheckcomponent.xml","schema.xml");
    assertNull(h.validateUpdate(adoc("id", "0", "lowerfilt", "pain table paintablepine pi ne in able")));
    assertNull(h.validateUpdate(adoc("id", "1", "lowerfilt", "paint able pineapple goodness in")));
    assertNull(h.validateUpdate(adoc("id", "2", "lowerfilt", "pa in table pineapplegoodness")));
    assertNull(h.validateUpdate(adoc("id", "3", "lowerfilt", "printable line in ample food mess")));
    assertNull(h.validateUpdate(adoc("id", "4", "lowerfilt", "printable in pointable paint able")));
    assertNull(h.validateUpdate(adoc("id", "5", "lowerfilt", "printable in puntable paint able ")));
    assertNull(h.validateUpdate(adoc("id", "6", "lowerfilt", "paint able in pintable plantable")));
    assertNull(h.validateUpdate(adoc("id", "7", "lowerfilt", "zxcvqwtp fg hj")));
    assertNull(h.validateUpdate(commit()));    
    //docfreq=7:  in
    //docfreq=5:  able
    //docfreq=4:  paint
    //docfreq=3:  printable
    //docfreq=2:  table
    //docfreq=1:  {all others}
  }
  
  @Test
  public void testStandAlone() throws Exception {
    SolrCore core = h.getCore();
    WordBreakSolrSpellChecker checker = new WordBreakSolrSpellChecker();
    NamedList<String> params = new NamedList<>();
    params.add("field", "lowerfilt");
    params.add(WordBreakSolrSpellChecker.PARAM_BREAK_WORDS, "true");
    params.add(WordBreakSolrSpellChecker.PARAM_COMBINE_WORDS, "true");
    params.add(WordBreakSolrSpellChecker.PARAM_MAX_CHANGES, "10");
    checker.init(params, core);

    //TODO can we use core.withSearcher ? refcounting here is confusing; not sure if intentional
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    QueryConverter qc = new SpellingQueryConverter();
    qc.setAnalyzer(new MockAnalyzer(random()));
    
    {
      //Prior to SOLR-8175, the required term would cause an AIOOBE.
      Collection<Token> tokens = qc.convert("+pine apple good ness");
      SpellingOptions spellOpts = new SpellingOptions(tokens, searcher.get().getIndexReader(), 10);
      SpellingResult result = checker.getSuggestions(spellOpts);
      searcher.decref();      
      assertTrue(result != null && result.getSuggestions() != null);
      assertTrue(result.getSuggestions().size()==5);
    }
    
    Collection<Token> tokens = qc.convert("paintable pine apple good ness");
    SpellingOptions spellOpts = new SpellingOptions(tokens, searcher.get().getIndexReader(), 10);
    SpellingResult result = checker.getSuggestions(spellOpts);
    searcher.decref();
    
    assertTrue(result != null && result.getSuggestions() != null);
    assertTrue(result.getSuggestions().size()==9);
    
    for(Map.Entry<Token, LinkedHashMap<String, Integer>> s : result.getSuggestions().entrySet()) {
      Token orig = s.getKey();
      String[] corr = s.getValue().keySet().toArray(new String[0]);
      if(orig.toString().equals("paintable")) {        
        assertTrue(orig.startOffset()==0);
        assertTrue(orig.endOffset()==9);
        assertTrue(orig.length()==9);
        assertTrue(corr.length==3);
        assertTrue(corr[0].equals("paint able"));  //1 op ; max doc freq=5
        assertTrue(corr[1].equals("pain table"));  //1 op ; max doc freq=2      
        assertTrue(corr[2].equals("pa in table")); //2 ops
      } else if(orig.toString().equals("pine apple")) {
        assertTrue(orig.startOffset()==10);
        assertTrue(orig.endOffset()==20);
        assertTrue(orig.length()==10);
        assertTrue(corr.length==1);
        assertTrue(corr[0].equals("pineapple"));
      } else if(orig.toString().equals("paintable pine")) {
        assertTrue(orig.startOffset()==0);
        assertTrue(orig.endOffset()==14);
        assertTrue(orig.length()==14);
        assertTrue(corr.length==1);
        assertTrue(corr[0].equals("paintablepine"));
      } else if(orig.toString().equals("good ness")) {
        assertTrue(orig.startOffset()==21);
        assertTrue(orig.endOffset()==30);
        assertTrue(orig.length()==9);
        assertTrue(corr.length==1);
        assertTrue(corr[0].equals("goodness"));
      } else if(orig.toString().equals("pine apple good ness")) {
        assertTrue(orig.startOffset()==10);
        assertTrue(orig.endOffset()==30);
        assertTrue(orig.length()==20);
        assertTrue(corr.length==1);
        assertTrue(corr[0].equals("pineapplegoodness"));
      } else if(orig.toString().equals("pine")) {
        assertTrue(orig.startOffset()==10);
        assertTrue(orig.endOffset()==14);
        assertTrue(orig.length()==4);
        assertTrue(corr.length==1);
        assertTrue(corr[0].equals("pi ne"));
      } else if(orig.toString().equals("pine")) {
        assertTrue(orig.startOffset()==10);
        assertTrue(orig.endOffset()==14);
        assertTrue(orig.length()==4);
        assertTrue(corr.length==1);
        assertTrue(corr[0].equals("pi ne"));
      } else if(orig.toString().equals("apple")) {
        assertTrue(orig.startOffset()==15);
        assertTrue(orig.endOffset()==20);
        assertTrue(orig.length()==5);
        assertTrue(corr.length==0);
      } else if(orig.toString().equals("good")) {
        assertTrue(orig.startOffset()==21);
        assertTrue(orig.endOffset()==25);
        assertTrue(orig.length()==4);
        assertTrue(corr.length==0);
      } else if(orig.toString().equals("ness")) {
        assertTrue(orig.startOffset()==26);
        assertTrue(orig.endOffset()==30);
        assertTrue(orig.length()==4);
        assertTrue(corr.length==0);
      }else {
        fail("Unexpected original result: " + orig);
      }        
    }  
  }
  @Test
  public void testInConjunction() throws Exception {
    assertQ(req(
        "q", "lowerfilt:(paintable pine apple good ness)", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.SPELLCHECK_BUILD, "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true"),
        "//lst[@name='suggestions']/lst[1]/@name='paintable'",
        "//lst[@name='suggestions']/lst[2]/@name='pine'",
        "//lst[@name='suggestions']/lst[3]/@name='apple'",
        "//lst[@name='suggestions']/lst[4]/@name='good'",
        "//lst[@name='suggestions']/lst[5]/@name='ness'",
        "//lst[@name='paintable']/int[@name='numFound']=8",
        "//lst[@name='paintable']/int[@name='startOffset']=11",
        "//lst[@name='paintable']/int[@name='endOffset']=20",
        "//lst[@name='paintable']/int[@name='origFreq']=0",
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[1]/str[@name='word']='printable'",  //SolrSpellChecker result interleaved
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[1]/int[@name='freq']=3",        
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[2]/str[@name='word']='paint able'", //1 op
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[2]/int[@name='freq']=5", 
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[3]/str[@name='word']='pintable'",   //SolrSpellChecker result interleaved
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[3]/int[@name='freq']=1",   
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[4]/str[@name='word']='pain table'", //1 op
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[4]/int[@name='freq']=2", 
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[5]/str[@name='word']='pointable'",  //SolrSpellChecker result interleaved
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[5]/int[@name='freq']=1",  
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[6]/str[@name='word']='pa in table'", //2 ops
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[6]/int[@name='freq']=7",
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[7]/str[@name='word']='plantable'",  //SolrSpellChecker result interleaved
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[7]/int[@name='freq']=1",  
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[8]/str[@name='word']='puntable'",   //SolrSpellChecker result interleaved
        "//lst[@name='paintable']/arr[@name='suggestion']/lst[8]/int[@name='freq']=1",  
        "//lst[@name='pine']/int[@name='numFound']=2",
        "//lst[@name='pine']/int[@name='startOffset']=21",
        "//lst[@name='pine']/int[@name='endOffset']=25",
        "//lst[@name='pine']/arr[@name='suggestion']/lst[1]/str[@name='word']='line'",
        "//lst[@name='pine']/arr[@name='suggestion']/lst[2]/str[@name='word']='pi ne'",
        "//lst[@name='apple']/int[@name='numFound']=1",
        "//lst[@name='apple']/arr[@name='suggestion']/lst[1]/str[@name='word']='ample'",
        "//lst[@name='good']/int[@name='numFound']=1",
        "//lst[@name='good']/arr[@name='suggestion']/lst[1]/str[@name='word']='food'",
        "//lst[@name='ness']/int[@name='numFound']=1",
        "//lst[@name='ness']/arr[@name='suggestion']/lst[1]/str[@name='word']='mess'",
        "//lst[@name='pine apple']/int[@name='numFound']=1",
        "//lst[@name='pine apple']/int[@name='startOffset']=21",
        "//lst[@name='pine apple']/int[@name='endOffset']=31",
        "//lst[@name='pine apple']/arr[@name='suggestion']/lst[1]/str[@name='word']='pineapple'",
        "//lst[@name='paintable pine']/int[@name='numFound']=1",
        "//lst[@name='paintable pine']/int[@name='startOffset']=11",
        "//lst[@name='paintable pine']/int[@name='endOffset']=25",
        "//lst[@name='paintable pine']/arr[@name='suggestion']/lst[1]/str[@name='word']='paintablepine'",
        "//lst[@name='good ness']/int[@name='numFound']=1",
        "//lst[@name='good ness']/int[@name='startOffset']=32",
        "//lst[@name='good ness']/int[@name='endOffset']=41",
        "//lst[@name='good ness']/arr[@name='suggestion']/lst[1]/str[@name='word']='goodness'",
        "//lst[@name='pine apple good ness']/int[@name='numFound']=1",
        "//lst[@name='pine apple good ness']/int[@name='startOffset']=21",
        "//lst[@name='pine apple good ness']/int[@name='endOffset']=41",
        "//lst[@name='pine apple good ness']/arr[@name='suggestion']/lst[1]/str[@name='word']='pineapplegoodness'"
    );
  }
  @Test
  public void testCollate() throws Exception {
   assertQ(req(
        "q", "lowerfilt:(paintable pine apple godness)", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.SPELLCHECK_BUILD, "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='lowerfilt:(printable line ample goodness)'",
        "//lst[@name='collation'][2 ]/str[@name='collationQuery']='lowerfilt:(paintablepine ample goodness)'",
        "//lst[@name='collation'][3 ]/str[@name='collationQuery']='lowerfilt:(printable pineapple goodness)'",
        "//lst[@name='collation'][4 ]/str[@name='collationQuery']='lowerfilt:(paint able line ample goodness)'",
        "//lst[@name='collation'][5 ]/str[@name='collationQuery']='lowerfilt:(printable pi ne ample goodness)'",
        "//lst[@name='collation'][6 ]/str[@name='collationQuery']='lowerfilt:(paint able pineapple goodness)'",
        "//lst[@name='collation'][7 ]/str[@name='collationQuery']='lowerfilt:(paint able pi ne ample goodness)'",
        "//lst[@name='collation'][8 ]/str[@name='collationQuery']='lowerfilt:(pintable line ample goodness)'",
        "//lst[@name='collation'][9 ]/str[@name='collationQuery']='lowerfilt:(pintable pineapple goodness)'",
        "//lst[@name='collation'][10]/str[@name='collationQuery']='lowerfilt:(pintable pi ne ample goodness)'",
        "//lst[@name='collation'][10]/lst[@name='misspellingsAndCorrections']/str[@name='paintable']='pintable'",
        "//lst[@name='collation'][10]/lst[@name='misspellingsAndCorrections']/str[@name='pine']='pi ne'",
        "//lst[@name='collation'][10]/lst[@name='misspellingsAndCorrections']/str[@name='apple']='ample'",
        "//lst[@name='collation'][10]/lst[@name='misspellingsAndCorrections']/str[@name='godness']='goodness'"
    );
    assertQ(req(
        "q", "lowerfilt:(pine AND apple)", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='lowerfilt:(line AND ample)'",
        "//lst[@name='collation'][2 ]/str[@name='collationQuery']='lowerfilt:(pineapple)'",
        "//lst[@name='collation'][3 ]/str[@name='collationQuery']='lowerfilt:((pi AND ne) AND ample)'"
    );
    assertQ(req(
        "q", "lowerfilt:pine AND NOT lowerfilt:apple", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='lowerfilt:line AND NOT lowerfilt:ample'",
        "//lst[@name='collation'][2 ]/str[@name='collationQuery']='lowerfilt:(pi AND ne) AND NOT lowerfilt:ample'"
    );
    assertQ(req(
        "q", "lowerfilt:pine NOT lowerfilt:apple", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='lowerfilt:line NOT lowerfilt:ample'",
        "//lst[@name='collation'][2 ]/str[@name='collationQuery']='lowerfilt:(pi AND ne) NOT lowerfilt:ample'"
    );
    assertQ(req(
        "q", "lowerfilt:(+pine -apple)", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='lowerfilt:(+line -ample)'",
        "//lst[@name='collation'][2 ]/str[@name='collationQuery']='lowerfilt:(+pi +ne -ample)'"
    );
    assertQ(req(
        "q", "lowerfilt:(+printableinpuntableplantable)", 
        "qt", "/spellCheckWithWordbreak",
        "indent", "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, "1", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "1"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='lowerfilt:(+printable +in +puntable +plantable)'"
    );
    assertQ(req(
        "q", "zxcv AND qwtp AND fghj", 
        "qt", "/spellCheckWithWordbreak",
        "defType", "edismax",
        "qf", "lowerfilt",
        "indent", "true",
        SpellCheckComponent.SPELLCHECK_BUILD, "true",
        SpellCheckComponent.COMPONENT_NAME, "true", 
        SpellCheckComponent.SPELLCHECK_ACCURACY, ".75", 
        SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE, "true",
        SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true",
        SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10"),
        "//lst[@name='collation'][1 ]/str[@name='collationQuery']='zxcvqwtp AND (fg AND hj)'"
    ); 
  }
}
