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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Takes a string (e.g. a query string) as the value of the "q" parameter
 * and looks up alternative spelling suggestions in the spellchecker.
 * The spellchecker used by this handler is the Lucene contrib SpellChecker.
 * @see <a href="http://wiki.apache.org/jakarta-lucene/SpellChecker">The Lucene Spellchecker documentation</a>
 *
 */
public class SpellCheckerRequestHandler extends RequestHandlerBase {

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
  private static IndexReader nullReader = null;
  private String restrictToField = null;
  private boolean onlyMorePopular = false;

  private Directory spellcheckerIndexDir = new RAMDirectory();
  private String dirDescription = "(ramdir)";
  private String termSourceField;
  private static final float DEFAULT_ACCURACY = 0.5f;
  private static final int DEFAULT_NUM_SUGGESTIONS = 1;
    
  public void init(NamedList args) {
    super.init(args);
    SolrParams p = SolrParams.toSolrParams(args);
    termSourceField = p.get("termSourceField");

    try {
      String dir = p.get("spellcheckerIndexDir");
      if (null != dir) {
        File f = new File(dir);
        if ( ! f.isAbsolute() ) {
          f = new File(SolrCore.getSolrCore().getDataDir(), dir);
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

    Float accuracy;
    int numSug;
    try {
      accuracy = p.getFloat("accuracy", DEFAULT_ACCURACY);
      spellChecker.setAccuracy(accuracy);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Accuracy must be a valid positive float", e);
    }
    try {
      numSug = p.getInt("suggestionCount", DEFAULT_NUM_SUGGESTIONS);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Spelling suggestion count must be a valid positive integer", e);
    }

    if (null != words && !"".equals(words.trim())) {
      String[] suggestions =
        spellChecker.suggestSimilar(words, numSug,
                                    nullReader, restrictToField,
                                    onlyMorePopular);
          
      rsp.add("suggestions", Arrays.asList(suggestions));
    }
  }

  /** Rebuilds the SpellChecker index using values from the <code>termSourceField</code> from the
   * index pointed to by the current {@link IndexSearcher}.
   */
  private void rebuild(SolrQueryRequest req) throws IOException, SolrException {
    if (null == termSourceField) {
      throw new SolrException
        (SolrException.ErrorCode.SERVER_ERROR, "can't rebuild spellchecker index without termSourceField configured");
    }
      
    IndexReader indexReader = req.getSearcher().getReader();
    Dictionary dictionary = new LuceneDictionary(indexReader, termSourceField);
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
