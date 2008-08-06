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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.HighFrequencyDictionary;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Takes a string (e.g. a query string) as the value of the "q" parameter
 * and looks up alternative spelling suggestions in the spellchecker.
 * The spellchecker used by this handler is the Lucene contrib SpellChecker.
 * 
<style>
pre.code
{
  border: 1pt solid #AEBDCC;
  background-color: #F3F5F7;
  padding: 5pt;
  font-family: courier, monospace;
  white-space: pre;
  // begin css 3 or browser specific rules - do not remove!
  //see: http://forums.techguy.org/archive/index.php/t-249849.html 
    white-space: pre-wrap;
    word-wrap: break-word;
    white-space: -moz-pre-wrap;
    white-space: -pre-wrap;
    white-space: -o-pre-wrap;
   // end css 3 or browser specific rules
}

</style>
 * 
 * <p>The results identifies the original words echoing it as an entry with the 
 * name of "words" and original word value.  It 
 * also identifies if the requested "words" is contained in the index through 
 * the use of the exist true/false name value. Examples of these output 
 * parameters in the standard output format is as follows:</p>
 * <pre class="code">
&lt;str name="words"&gt;facial&lt;/str&gt;
&lt;str name="exist"&gt;true&lt;/str&gt; </pre>
 * 
 * <p>If a query string parameter of "multiWords" is used, then each word within the
 * "q" parameter (seperated by a space or +) will 
 * be iterated through the spell checker and will be wrapped in an 
 * NamedList.  Each word will then get its own set of results: words, exists, and
 * suggestions.</p>
 * 
 * <p>Examples of the use of the standard ouput (XML) without and with the 
 * use of the "multiWords" parameter are as follows.</p>
 * 
 * <p> The following URL
 * examples were configured with the solr.SpellCheckerRequestHandler 
 * named as "/spellchecker".</p>
 * 
 * <p>Without the use of "extendedResults" and one word 
 * spelled correctly: facial </p>
 * <pre class="code">http://.../spellchecker?indent=on&onlyMorePopular=true&accuracy=.6&suggestionCount=20&q=facial</pre>
 * <pre class="code">
&lt;?xml version="1.0" encoding="UTF-8"?&gt;
&lt;response&gt;

&lt;lst name="responseHeader"&gt;
   &lt;int name="status"&gt;0&lt;/int&gt;
   &lt;int name="QTime"&gt;6&lt;/int&gt;
&lt;/lst&gt;
&lt;str name="words"&gt;facial&lt;/str&gt;
&lt;str name="exist"&gt;true&lt;/str&gt;
&lt;arr name="suggestions"&gt;
   &lt;str&gt;faciale&lt;/str&gt;
   &lt;str&gt;faucial&lt;/str&gt;
   &lt;str&gt;fascial&lt;/str&gt;
   &lt;str&gt;facing&lt;/str&gt;
   &lt;str&gt;faciei&lt;/str&gt;
   &lt;str&gt;facialis&lt;/str&gt;
   &lt;str&gt;social&lt;/str&gt;
   &lt;str&gt;facile&lt;/str&gt;
   &lt;str&gt;spacial&lt;/str&gt;
   &lt;str&gt;glacial&lt;/str&gt;
   &lt;str&gt;marcial&lt;/str&gt;
   &lt;str&gt;facies&lt;/str&gt;
   &lt;str&gt;facio&lt;/str&gt;
&lt;/arr&gt;
&lt;/response&gt;   </pre>
 * 
 * <p>Without the use of "extendedResults" and two words,  
 * one spelled correctly and one misspelled: facial salophosphoprotein </p>
 * <pre class="code">http://.../spellchecker?indent=on&onlyMorePopular=true&accuracy=.6&suggestionCount=20&q=facial+salophosphoprotein</pre>
 * <pre class="code">
&lt;?xml version="1.0" encoding="UTF-8"?&gt;
&lt;response&gt;

&lt;lst name="responseHeader"&gt;
   &lt;int name="status"&gt;0&lt;/int&gt;
   &lt;int name="QTime"&gt;18&lt;/int&gt;
&lt;/lst&gt;
&lt;str name="words"&gt;facial salophosphoprotein&lt;/str&gt;
&lt;str name="exist"&gt;false&lt;/str&gt;
&lt;arr name="suggestions"&gt;
   &lt;str&gt;sialophosphoprotein&lt;/str&gt;
&lt;/arr&gt;
&lt;/response&gt;  </pre>
 * 
 * 
 * <p>With the use of "extendedResults" and two words,  
 * one spelled correctly and one misspelled: facial salophosphoprotein </p>
 * <pre class="code">http://.../spellchecker?indent=on&onlyMorePopular=true&accuracy=.6&suggestionCount=20&extendedResults=true&q=facial+salophosphoprotein</pre>
 * <pre class="code">
&lt;?xml version="1.0" encoding="UTF-8"?&gt;
&lt;response&gt;

&lt;lst name="responseHeader"&gt;
   &lt;int name="status"&gt;0&lt;/int&gt;
   &lt;int name="QTime"&gt;23&lt;/int&gt;
&lt;/lst&gt;
&lt;lst name="result"&gt;
  &lt;lst name="facial"&gt;
    &lt;int name="frequency"&gt;1&lt;/int&gt;
    &lt;lst name="suggestions"&gt;
      &lt;lst name="faciale"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="faucial"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="fascial"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="facing"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="faciei"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="facialis"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="social"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="facile"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="spacial"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="glacial"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="marcial"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="facies"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="facio"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
    &lt;/lst&gt;
  &lt;/lst&gt;
  &lt;lst name="salophosphoprotein"&gt;
    &lt;int name="frequency"&gt;0&lt;/int&gt;
    &lt;lst name="suggestions"&gt; 
      &lt;lst name="sialophosphoprotein"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="phosphoprotein"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="phosphoproteins"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
      &lt;lst name="alphalipoprotein"&gt;&lt;int name="frequency"&gt;1&lt;/int&gt;&lt;/lst&gt;
    &lt;/lst&gt;
  &lt;/lst&gt;
&lt;/lst&gt;
&lt;/response&gt;  </pre>

 * 
 * @see <a href="http://wiki.apache.org/jakarta-lucene/SpellChecker">The Lucene Spellchecker documentation</a>
 *
 */
public class SpellCheckerRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  private static Logger log = Logger.getLogger(SpellCheckerRequestHandler.class.getName());
  
  private SpellChecker spellChecker;
  
  /*
   * From http://wiki.apache.org/jakarta-lucene/SpellChecker
   * If reader and restrictToField are both not null:
   * 1. The returned words are restricted only to the words presents in the field
   * "restrictToField "of the Lucene Index "reader".
   *
   * 2. The list is also sorted with a second criterium: the popularity (the
   * frequence) of the word in the user field.
   *
   * 3. If "onlyMorePopular" is true and the mispelled word exist in the user field,
   * return only the words more frequent than this.
   * 
   */

  protected Directory spellcheckerIndexDir = new RAMDirectory();
  protected String dirDescription = "(ramdir)";
  protected String termSourceField;

  protected static final String PREFIX = "sp.";
  protected static final String QUERY_PREFIX = PREFIX + "query.";
  protected static final String DICTIONARY_PREFIX = PREFIX + "dictionary.";

  protected static final String SOURCE_FIELD = DICTIONARY_PREFIX + "termSourceField";
  protected static final String INDEX_DIR = DICTIONARY_PREFIX + "indexDir";
  protected static final String THRESHOLD = DICTIONARY_PREFIX + "threshold";

  protected static final String ACCURACY = QUERY_PREFIX + "accuracy";
  protected static final String SUGGESTIONS = QUERY_PREFIX + "suggestionCount";
  protected static final String POPULAR = QUERY_PREFIX + "onlyMorePopular";
  protected static final String EXTENDED = QUERY_PREFIX + "extendedResults";

  protected static final float DEFAULT_ACCURACY = 0.5f;
  protected static final int DEFAULT_SUGGESTION_COUNT = 1;
  protected static final boolean DEFAULT_MORE_POPULAR = false;
  protected static final boolean DEFAULT_EXTENDED_RESULTS = false;
  protected static final float DEFAULT_DICTIONARY_THRESHOLD = 0.0f;

  protected SolrParams args = null;
  
  @Override
  public void init(NamedList args) {
    super.init(args);
    this.args = SolrParams.toSolrParams(args);
  }

  public void inform(SolrCore core) 
  {
    termSourceField = args.get(SOURCE_FIELD, args.get("termSourceField"));
    try {
      String dir = args.get(INDEX_DIR, args.get("spellcheckerIndexDir"));
      if (null != dir) {
        File f = new File(dir);
        if ( ! f.isAbsolute() ) {
          f = new File(core.getDataDir(), dir);
        }
        dirDescription = f.getAbsolutePath();
        log.info("using spell directory: " + dirDescription);
        spellcheckerIndexDir = FSDirectory.getDirectory(f);
      } else {
        log.info("using RAM based spell directory");
      }
      spellChecker = new SpellChecker(spellcheckerIndexDir);
    } catch (IOException e) {
      throw new RuntimeException("Cannot open SpellChecker index", e);
    }
  }

  /**
   * Processes the following query string parameters: q, multiWords, cmd rebuild,
   * cmd reopen, accuracy, suggestionCount, restrictToField, and onlyMorePopular.
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
    throws Exception {
    SolrParams p = req.getParams();
    String words = p.get("q");
    String cmd = p.get("cmd");
    if (cmd != null) {
      cmd = cmd.trim();
      if (cmd.equals("rebuild")) {
        rebuild(req);
        rsp.add("cmdExecuted","rebuild");
      } else if (cmd.equals("reopen")) {
        reopen();
        rsp.add("cmdExecuted","reopen");
      } else {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unrecognized Command: " + cmd);
      }
    }

    // empty query string
    if (null == words || "".equals(words.trim())) {
      return;
    }

    IndexReader indexReader = null;
    String suggestionField = null;
    Float accuracy;
    int numSug;
    boolean onlyMorePopular;
    boolean extendedResults;
    try {
      accuracy = p.getFloat(ACCURACY, p.getFloat("accuracy", DEFAULT_ACCURACY));
      spellChecker.setAccuracy(accuracy);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Accuracy must be a valid positive float", e);
    }
    try {
      numSug = p.getInt(SUGGESTIONS, p.getInt("suggestionCount", DEFAULT_SUGGESTION_COUNT));
    } catch (NumberFormatException e) {
      throw new RuntimeException("Spelling suggestion count must be a valid positive integer", e);
    }
    try {
      onlyMorePopular = p.getBool(POPULAR, DEFAULT_MORE_POPULAR);
    } catch (SolrException e) {
      throw new RuntimeException("'Only more popular' must be a valid boolean", e);
    }
    try {
      extendedResults = p.getBool(EXTENDED, DEFAULT_EXTENDED_RESULTS);
    } catch (SolrException e) {
      throw new RuntimeException("'Extended results' must be a valid boolean", e);
    }

    // when searching for more popular, a non null index-reader and
    // restricted-field are required
    if (onlyMorePopular || extendedResults) {
      indexReader = req.getSearcher().getReader();
      suggestionField = termSourceField;
    }

    if (extendedResults) {

      rsp.add("numDocs", indexReader.numDocs());

      SimpleOrderedMap<Object> results = new SimpleOrderedMap<Object>();
      String[] wordz = words.split(" ");
      for (String word : wordz)
      {
        SimpleOrderedMap<Object> nl = new SimpleOrderedMap<Object>();
        nl.add("frequency", indexReader.docFreq(new Term(suggestionField, word)));
        String[] suggestions =
          spellChecker.suggestSimilar(word, numSug,
          indexReader, suggestionField, onlyMorePopular);

        // suggestion array
        NamedList<Object> sa = new NamedList<Object>();
        for (int i=0; i<suggestions.length; i++) {
          // suggestion item
          SimpleOrderedMap<Object> si = new SimpleOrderedMap<Object>();
          si.add("frequency", indexReader.docFreq(new Term(termSourceField, suggestions[i])));
          sa.add(suggestions[i], si);
        }
        nl.add("suggestions", sa);
        results.add(word, nl);
      }
      rsp.add( "result", results );

    } else {
      rsp.add("words", words);
      if (spellChecker.exist(words)) {
        rsp.add("exist","true");
      } else {
        rsp.add("exist","false");
      }
      String[] suggestions =
        spellChecker.suggestSimilar(words, numSug,
                                    indexReader, suggestionField,
                                    onlyMorePopular);

      rsp.add("suggestions", Arrays.asList(suggestions));
    }
  }

  /** Returns a dictionary to be used when building the spell-checker index.
   * Override the method for custom dictionary
   */
  protected Dictionary getDictionary(SolrQueryRequest req) {
    float threshold;
    try {
      threshold = req.getParams().getFloat(THRESHOLD, DEFAULT_DICTIONARY_THRESHOLD);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Threshold must be a valid positive float", e);
    }
    IndexReader indexReader = req.getSearcher().getReader();
    return new HighFrequencyDictionary(indexReader, termSourceField, threshold);
  }

  /** Rebuilds the SpellChecker index using values from the <code>termSourceField</code> from the
   * index pointed to by the current {@link IndexSearcher}.
   * Any word appearing in less that thresh documents will not be added to the spellcheck index.
   */
  private void rebuild(SolrQueryRequest req) throws IOException, SolrException {
    if (null == termSourceField) {
      throw new SolrException
        (SolrException.ErrorCode.SERVER_ERROR, "can't rebuild spellchecker index without termSourceField configured");
    }

    Dictionary dictionary = getDictionary(req);
    spellChecker.clearIndex();
    spellChecker.indexDictionary(dictionary);
    reopen();
  }
  
  /**
   * Reopens the SpellChecker index directory.
   * Useful if an external process is responsible for building
   * the spell checker index.
   */
  private void reopen() throws IOException {
    spellChecker.setSpellIndex(spellcheckerIndexDir);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  public String getVersion() {
    return "$Revision$";
  }

  public String getDescription() {
    return "The SpellChecker Solr request handler for SpellChecker index: " + dirDescription;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }
}
