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

package org.apache.solr.spelling;

import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.analysis.Token;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.Collection;

/**
 *
 * @since solr 1.3
 **/
public class FileBasedSpellCheckerTest extends AbstractSolrTestCase{

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  private SpellingQueryConverter queryConverter;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    //Index something with a title
    assertU(adoc("id", "0", "teststop", "This is a title"));
    assertU(adoc("id", "1", "teststop", "The quick reb fox jumped over the lazy brown dogs."));
    assertU(adoc("id", "2", "teststop", "This is a Solr"));
    assertU(adoc("id", "3", "teststop", "solr foo"));
    assertU("commit",
            commit());
    String allq = "id:[0 TO 3]";
    assertQ("docs not added", req(allq));
    queryConverter = new SimpleQueryConverter();
    queryConverter.init(new NamedList());
  }

  public void test() throws Exception {
    FileBasedSpellChecker checker = new FileBasedSpellChecker();
    NamedList spellchecker = new NamedList();
    spellchecker.add("classname", FileBasedSpellChecker.class.getName());

    spellchecker.add(SolrSpellChecker.DICTIONARY_NAME, "external");
    File spelling = new File("spellings.txt");
    spellchecker.add(AbstractLuceneSpellChecker.LOCATION, spelling.getAbsolutePath());
    spellchecker.add(IndexBasedSpellChecker.FIELD, "teststop");
    spellchecker.add(FileBasedSpellChecker.SOURCE_FILE_CHAR_ENCODING, "UTF-8");
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File indexDir = new File(tmpDir, "spellingIdx" + new Date().getTime());
    indexDir.mkdirs();
    spellchecker.add(FileBasedSpellChecker.INDEX_DIR, indexDir.getAbsolutePath());
    SolrCore core = h.getCore();
    String dictName = checker.init(spellchecker, core.getResourceLoader());
    assertTrue(dictName + " is not equal to " + "external", dictName.equals("external") == true);
    checker.build(core, null);

    IndexReader reader = core.getSearcher().get().getReader();
    Collection<Token> tokens = queryConverter.convert("fob");
    SpellingResult result = checker.getSuggestions(tokens, reader);
    assertTrue("result is null and it shouldn't be", result != null);
    Map<String, Integer> suggestions = result.get(tokens.iterator().next());
    Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
    assertTrue(entry.getKey() + " is not equal to " + "foo", entry.getKey().equals("foo") == true);
    assertTrue(entry.getValue() + " does not equal: " + SpellingResult.NO_FREQUENCY_INFO, entry.getValue() == SpellingResult.NO_FREQUENCY_INFO);

    tokens = queryConverter.convert("super");
    result = checker.getSuggestions(tokens, reader);
    assertTrue("result is null and it shouldn't be", result != null);
    suggestions = result.get(tokens.iterator().next());
    assertTrue("suggestions is not null and it should be", suggestions == null);


  }

  public void testFieldType() throws Exception {
    FileBasedSpellChecker checker = new FileBasedSpellChecker();
    NamedList spellchecker = new NamedList();
    spellchecker.add("classname", FileBasedSpellChecker.class.getName());
    spellchecker.add(SolrSpellChecker.DICTIONARY_NAME, "external");
    File spelling = new File("spellings.txt");
    spellchecker.add(AbstractLuceneSpellChecker.LOCATION, spelling.getAbsolutePath());
    spellchecker.add(IndexBasedSpellChecker.FIELD, "teststop");
    spellchecker.add(FileBasedSpellChecker.SOURCE_FILE_CHAR_ENCODING, "UTF-8");
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File indexDir = new File(tmpDir, "spellingIdx" + new Date().getTime());
    indexDir.mkdirs();
    spellchecker.add(FileBasedSpellChecker.INDEX_DIR, indexDir.getAbsolutePath());
    spellchecker.add(FileBasedSpellChecker.FIELD_TYPE, "teststop");
    spellchecker.add(AbstractLuceneSpellChecker.SPELLCHECKER_ARG_NAME, spellchecker);
    SolrCore core = h.getCore();
    String dictName = checker.init(spellchecker, core.getResourceLoader());
    assertTrue(dictName + " is not equal to " + "external", dictName.equals("external") == true);
    checker.build(core, null);

    IndexReader reader = core.getSearcher().get().getReader();
    Collection<Token> tokens = queryConverter.convert("Solar");
    SpellingResult result = checker.getSuggestions(tokens, reader);
    assertTrue("result is null and it shouldn't be", result != null);
    //should be lowercased, b/c we are using a lowercasing analyzer
    Map<String, Integer> suggestions = result.get(tokens.iterator().next());
    assertTrue("suggestions Size: " + suggestions.size() + " is not: " + 1, suggestions.size() == 1);
    Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
    assertTrue(entry.getKey() + " is not equal to " + "solr", entry.getKey().equals("solr") == true);
    assertTrue(entry.getValue() + " does not equal: " + SpellingResult.NO_FREQUENCY_INFO, entry.getValue() == SpellingResult.NO_FREQUENCY_INFO);

    //test something not in the spell checker
    tokens = queryConverter.convert("super");
    result = checker.getSuggestions(tokens, reader);
    assertTrue("result is null and it shouldn't be", result != null);
    suggestions = result.get(tokens.iterator().next());
    assertTrue("suggestions is not null and it should be", suggestions == null);
  }

  /**
   * No indexDir location set
   * @throws Exception
   */
  public void testRAMDirectory() throws Exception {
    FileBasedSpellChecker checker = new FileBasedSpellChecker();
    NamedList spellchecker = new NamedList();
    spellchecker.add("classname", FileBasedSpellChecker.class.getName());

    spellchecker.add(SolrSpellChecker.DICTIONARY_NAME, "external");
    File spelling = new File("spellings.txt");
    spellchecker.add(AbstractLuceneSpellChecker.LOCATION, spelling.getAbsolutePath());
    spellchecker.add(FileBasedSpellChecker.SOURCE_FILE_CHAR_ENCODING, "UTF-8");
    spellchecker.add(IndexBasedSpellChecker.FIELD, "teststop");
    spellchecker.add(FileBasedSpellChecker.FIELD_TYPE, "teststop");
    spellchecker.add(AbstractLuceneSpellChecker.SPELLCHECKER_ARG_NAME, spellchecker);

    SolrCore core = h.getCore();
    String dictName = checker.init(spellchecker, core.getResourceLoader());
    assertTrue(dictName + " is not equal to " + "external", dictName.equals("external") == true);
    checker.build(core, null);

    IndexReader reader = core.getSearcher().get().getReader();
    Collection<Token> tokens = queryConverter.convert("solar");
    SpellingResult result = checker.getSuggestions(tokens, reader);
    assertTrue("result is null and it shouldn't be", result != null);
    //should be lowercased, b/c we are using a lowercasing analyzer
    Map<String, Integer> suggestions = result.get(tokens.iterator().next());
    assertTrue("suggestions Size: " + suggestions.size() + " is not: " + 1, suggestions.size() == 1);
    Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
    assertTrue(entry.getKey() + " is not equal to " + "solr", entry.getKey().equals("solr") == true);
    assertTrue(entry.getValue() + " does not equal: " + SpellingResult.NO_FREQUENCY_INFO, entry.getValue() == SpellingResult.NO_FREQUENCY_INFO);


    tokens = queryConverter.convert("super");
    result = checker.getSuggestions(tokens, reader);
    assertTrue("result is null and it shouldn't be", result != null);
    suggestions = result.get(tokens.iterator().next());
    assertTrue("suggestions is not null and it should be", suggestions == null);
  }
}
