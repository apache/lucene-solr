package org.apache.solr.analysis;

import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @since solr 1.4
 */
public class TestMultiWordSynonyms extends BaseTokenTestCase {

  @Test
  public void testMultiWordSynonyms() throws IOException {
    List<String> rules = new ArrayList<String>();
    rules.add("a b c,d");
    SynonymMap synMap = new SynonymMap(true);
    SynonymFilterFactory.parseRules(rules, synMap, "=>", ",", true, null);

    SynonymFilter ts = new SynonymFilter(new WhitespaceTokenizer(new StringReader("a e")), synMap);
    // This fails because ["e","e"] is the value of the token stream
    assertTokenStreamContents(ts, new String[] { "a", "e" });
  }
}
