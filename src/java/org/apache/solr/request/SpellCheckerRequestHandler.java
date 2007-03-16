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

package org.apache.solr.request;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.util.NamedList;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

/**
 * Takes a string (e.g. a query string) as the value of the "q" parameter
 * and looks up alternative spelling suggestions in the spellchecker.
 * The spellchecker used by this handler is the Lucene contrib SpellChecker.
 * @see http://wiki.apache.org/jakarta-lucene/SpellChecker
 *
 * @author Otis Gospodnetic
 */
public class SpellCheckerRequestHandler extends RequestHandlerBase {

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
    private static IndexReader reader = null;
    private String restrictToField = null;
    private boolean onlyMorePopular = false;

    private String spellcheckerIndexDir;
    private String termSourceField;
    private static final float DEFAULT_ACCURACY = 0.5f;
    private static final int DEFAULT_NUM_SUGGESTIONS = 1;
    
    public void init(NamedList args) {
        super.init(args);
        SolrParams p = SolrParams.toSolrParams(args);
        restrictToField = p.get("termSourceField");
        spellcheckerIndexDir = p.get("spellcheckerIndexDir");
        try {
            spellChecker = new SpellChecker(FSDirectory.getDirectory(spellcheckerIndexDir));
        } catch (IOException e) {
            throw new RuntimeException("Cannot open SpellChecker index", e);
        }
    }

    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
            throws Exception {
        SolrParams p = req.getParams();
        String words = p.get("q");
        String cmd = p.get("cmd");
        if (cmd != null && cmd.equals("rebuild"))
            rebuild(req);

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

        String[] suggestions = spellChecker.suggestSimilar(words, numSug,
                reader, restrictToField, onlyMorePopular);

        rsp.add("suggestions", Arrays.asList(suggestions));
    }

    /** Rebuilds the SpellChecker index using values from the <code>termSourceField</code> from the
     * index pointed to by the current {@link IndexSearcher}.
     */
    private void rebuild(SolrQueryRequest req) throws IOException {
        IndexReader indexReader = req.getSearcher().getReader();
        Dictionary dictionary = new LuceneDictionary(indexReader, termSourceField);
        spellChecker.indexDictionary(dictionary);
        spellChecker.setSpellIndex(FSDirectory.getDirectory(spellcheckerIndexDir));
    }

    //////////////////////// SolrInfoMBeans methods //////////////////////

    public String getVersion() {
        return SolrCore.version;
    }

    public String getDescription() {
        return "The SpellChecker Solr request handler for SpellChecker index: " + spellcheckerIndexDir;
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
