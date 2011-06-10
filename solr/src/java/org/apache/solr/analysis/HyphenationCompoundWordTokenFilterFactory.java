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

package org.apache.solr.analysis;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.plugin.ResourceLoaderAware;

import java.util.Map;
import java.io.InputStream;
import org.xml.sax.InputSource;

/**
 * Factory for {@link HyphenationCompoundWordTokenFilter}.
 * <p>
 * This factory accepts the following parameters:
 * <ul>
 *  <li><code>hyphenator</code> (mandatory): path to the FOP xml hyphenation pattern. 
 *  See <a href="http://offo.sourceforge.net/hyphenation/">http://offo.sourceforge.net/hyphenation/</a>.
 *  <li><code>encoding</code> (optional): encoding of the xml hyphenation file. defaults to UTF-8.
 *  <li><code>dictionary</code> (optional): dictionary of words. defaults to no dictionary.
 *  <li><code>minWordSize</code> (optional): minimal word length that gets decomposed. defaults to 5.
 *  <li><code>minSubwordSize</code> (optional): minimum length of subwords. defaults to 2.
 *  <li><code>maxSubwordSize</code> (optional): maximum length of subwords. defaults to 15.
 *  <li><code>onlyLongestMatch</code> (optional): if true, adds only the longest matching subword 
 *    to the stream. defaults to false.
 * </ul>
 * <p>
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_hyphncomp" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.HyphenationCompoundWordTokenFilterFactory" hyphenator="hyphenator.xml" encoding="UTF-8"
 *     	     dictionary="dictionary.txt" minWordSize="5" minSubwordSize="2" maxSubwordSize="15" onlyLongestMatch="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see HyphenationCompoundWordTokenFilter
 */
public class HyphenationCompoundWordTokenFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  private CharArraySet dictionary;
  private HyphenationTree hyphenator;
  private String dictFile;
  private String hypFile;
  private String encoding;
  private int minWordSize;
  private int minSubwordSize;
  private int maxSubwordSize;
  private boolean onlyLongestMatch;
  
  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    assureMatchVersion();
    dictFile = args.get("dictionary");
    if (args.containsKey("encoding"))
      encoding = args.get("encoding");
    hypFile = args.get("hyphenator");
    if (null == hypFile) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Missing required parameter: hyphenator");
    }

    minWordSize = getInt("minWordSize", CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE);
    minSubwordSize = getInt("minSubwordSize", CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE);
    maxSubwordSize = getInt("maxSubwordSize", CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE);
    onlyLongestMatch = getBoolean("onlyLongestMatch", false);
  }
  
  public void inform(ResourceLoader loader) {
    InputStream stream = null;
    try {
      if (dictFile != null) // the dictionary can be empty.
        dictionary = getWordSet(loader, dictFile, false);
      // TODO: Broken, because we cannot resolve real system id
      // ResourceLoader should also supply method like ClassLoader to get resource URL
      stream = loader.openResource(hypFile);
      final InputSource is = new InputSource(stream);
      is.setEncoding(encoding); // if it's null let xml parser decide
      is.setSystemId(hypFile);
      hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(is);
    } catch (Exception e) { // TODO: getHyphenationTree really shouldn't throw "Exception"
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(stream);
    }
  }
  
  public HyphenationCompoundWordTokenFilter create(TokenStream input) {
    return new HyphenationCompoundWordTokenFilter(luceneMatchVersion, input, hyphenator, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }
}
