package org.apache.solr.analysis;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.util.Version;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * Factory for {@link SynonymFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_synonym" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.SynonymFilterFactory" synonyms="synonyms.txt" 
 *             format="solr" ignoreCase="false" expand="true" 
 *             tokenizerFactory="solr.WhitespaceTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class SynonymFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  private BaseTokenFilterFactory delegator;

  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    assureMatchVersion();
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_34)) {
      delegator = new FSTSynonymFilterFactory();
    } else {
      // check if you use the new optional arg "format". this makes no sense for the old one, 
      // as its wired to solr's synonyms format only.
      if (args.containsKey("format") && !args.get("format").equals("solr")) {
        throw new IllegalArgumentException("You must specify luceneMatchVersion >= 3.4 to use alternate synonyms formats");
      }
      delegator = new SlowSynonymFilterFactory();
    }
    delegator.init(args);
  }

  @Override
  public TokenStream create(TokenStream input) {
    assert delegator != null : "init() was not called!";
    return delegator.create(input);
  }

  @Override
  public void inform(ResourceLoader loader) {
    assert delegator != null : "init() was not called!";
    ((ResourceLoaderAware) delegator).inform(loader);
  }
}
