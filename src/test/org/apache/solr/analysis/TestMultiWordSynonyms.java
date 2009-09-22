package org.apache.solr.analysis;

import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @version $Id$
 * @since solr 1.4
 */
public class TestMultiWordSynonyms {

  @Test
  public void testMultiWordSynonmys() throws IOException {
    List<String> rules = new ArrayList<String>();
    rules.add("a b c,d");
    SynonymMap synMap = new SynonymMap(true);
    SynonymFilterFactory.parseRules(rules, synMap, "=>", ",", true, null);

    SynonymFilter ts = new SynonymFilter(new WhitespaceTokenizer(new StringReader("a e")), synMap);
    TermAttribute termAtt = (TermAttribute) ts.getAttribute(TermAttribute.class);

    ts.reset();
    List<String> tokens = new ArrayList<String>();
    while (ts.incrementToken()) tokens.add(termAtt.term());

    // This fails because ["e","e"] is the value of the token stream
    Assert.assertEquals(Arrays.asList("a", "e"), tokens);
  }
}
