package org.apache.lucene.search.spell;


/**
 * Copyright 2002-2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import java.util.*;


/**
 *  <p>
 *	Spell Checker class  (Main class) <br/>
 * (initially inspired by the David Spencer code)
 *  </p>
 *  
 *  <p>
 *  Spell Checker spellchecker= new SpellChecker (spellDirectory);<br/>
 *  <br/>
 *  //To index a field of a user index <br/>
 *  spellchecker.indexDictionary(new LuceneDictionary(my_lucene_reader, a_field));<br/>
 *<br/>
 *   //To index a file containing words  <br/>
 *  spellchecker.indexDictionary(new PlainTextDictionary(new File("myfile.txt")));<br/>
 *</p>
 * 
 * @author Nicolas Maisonneuve
 * @version 1.0
 */
public class SpellChecker {

    /**
     * Field name for each word in the ngram index.
     */
    public static final String F_WORD="word";


    /**
     * the spell index
     */
    Directory spellindex;

    /**
     * Boost value for start and end grams
     */private float bStart=2.0f;
      private float bEnd=1.0f;
  

    private IndexReader reader;
    float min=0.5f;

    public void setSpellIndex (Directory spellindex) {
        this.spellindex=spellindex;
    }


    /**
     *  Set the accuraty 0<min<1 default 0.5
     * @param min float
     */
    public void setAccuraty (float min) {
        this.min=min;
    }


    public SpellChecker (Directory gramIndex) {
        this.setSpellIndex(gramIndex);
    }


    /**
     * Suggest similar words
     * @param word String the word you want a spell check done on
     * @param num_sug int the number of suggest words
     * @throws IOException
     * @return String[]
     */
    public String[] suggestSimilar (String word, int num_sug) throws IOException {
        return this.suggestSimilar(word, num_sug, null, null, false);
    }


    /**
     * Suggest similar words (restricted or not of a field of a user index)
     * @param word String the word you want a spell check done on
     * @param num_sug int the number of suggest words
     * @param IndexReader the indexReader of the user index (can be null see field param)
     * @param field String the field of the user index: if field is not null ,the suggest
     * words are restricted to the words present in this field.
     * @param morePopular boolean return only the suggest words that are more frequent than the searched word
     * (only if restricted mode = (indexReader!=null and field!=null)
     * @throws IOException
     * @return String[] the sorted list of the suggest words with this 2 criteri
     * first criteria : the edit distance, second criteria (only if restricted mode): the popularity
     * of the suggest words in the field of the user index
     */
    public String[] suggestSimilar (String word, int num_sug, IndexReader ir, String field
    , boolean morePopular) throws IOException {

        final TRStringDistance sd=new TRStringDistance(word);
        final int lengthWord=word.length();

        final int goalFreq=(morePopular&&ir!=null)?ir.docFreq(new Term(field, word)):0;
        if (!morePopular&&goalFreq>0) {
            return new String[] {
            word}; // return the word if it exist in the index and i don't want a more popular word
        }

        BooleanQuery query=new BooleanQuery();
        String[] grams;
        String key;

        for (int ng=getMin(lengthWord); ng<=getMax(lengthWord); ng++) {

            key="gram"+ng; // form key

            grams=formGrams(word, ng); // form word into ngrams (allow dups too)

            if (grams.length==0) {
                continue; // hmm
            }

            if (bStart>0) { // should we boost prefixes?
                add(query, "start"+ng, grams[0], bStart); // matches start of word

            }
            if (bEnd>0) { // should we boost suffixes
                add(query, "end"+ng, grams[grams.length-1], bEnd); // matches end of word

            }
            for (int i=0; i<grams.length; i++) {
                add(query, key, grams[i]);
            }

        }

        IndexSearcher searcher=new IndexSearcher(this.spellindex);
        Hits hits=searcher.search(query);
        SuggestWordQueue sugqueue=new SuggestWordQueue(num_sug);

        int stop=Math.min(hits.length(), 10*num_sug); // go thru more than 'maxr' matches in case the distance filter triggers
        SuggestWord sugword=new SuggestWord();
        for (int i=0; i<stop; i++) {

            sugword.string=hits.doc(i).get(F_WORD); // get orig word)

            if (sugword.string==word) {
                continue; // don't suggest a word for itself, that would be silly
            }

            //edit distance/normalize with the min word length
            sugword.score=1.0f-((float) sd.getDistance(sugword.string)/Math.min(sugword.string.length(), lengthWord));
            if (sugword.score<min) {
                continue;
            }

            if (ir!=null) { // use the user index
                sugword.freq=ir.docFreq(new Term(field, sugword.string)); // freq in the index
                if ((morePopular&&goalFreq>sugword.freq)||sugword.freq<1) { // don't suggest a word that is not present in the field
                    continue;
                }
            }
            sugqueue.insert(sugword);
            if (sugqueue.size()==num_sug) {
                //if queue full , maintain the min score
                min=((SuggestWord) sugqueue.top()).score;
            }
            sugword=new SuggestWord();
        }

        // convert to array string
        String[] list=new String[sugqueue.size()];
        for (int i=sugqueue.size()-1; i>=0; i--) {
            list[i]=((SuggestWord) sugqueue.pop()).string;
        }

        searcher.close();
        return list;
    }


    /**
     * Add a clause to a boolean query.
     */
    private static void add (BooleanQuery q, String k, String v, float boost) {
        Query tq=new TermQuery(new Term(k, v));
        tq.setBoost(boost);
        q.add(new BooleanClause(tq, false, false));
    }


    /**
     * Add a clause to a boolean query.
     */
    private static void add (BooleanQuery q, String k, String v) {
        q.add(new BooleanClause(new TermQuery(new Term(k, v)), false, false));
    }


    /**
     * Form all ngrams for a given word.
     * @param text the word to parse
     * @param ng the ngram length e.g. 3
     * @return an array of all ngrams in the word and note that duplicates are not removed
     */
    private static String[] formGrams (String text, int ng) {
        int len=text.length();
        String[] res=new String[len-ng+1];
        for (int i=0; i<len-ng+1; i++) {
            res[i]=text.substring(i, i+ng);
        }
        return res;
    }


    public void clearIndex () throws IOException {
        IndexReader.unlock(spellindex);
        IndexWriter writer=new IndexWriter(spellindex, null, true);
        writer.close();
    }


    /**
     * if the word exist in the index
     * @param word String
     * @throws IOException
     * @return boolean
     */
    public boolean exist (String word) throws IOException {
        if (reader==null) {
            reader=IndexReader.open(spellindex);
        }
        return reader.docFreq(new Term(F_WORD, word))>0;
    }


    /**
     * Index a Dictionnary
     * @param dict the dictionnary to index
     * @throws IOException
     */
    public void indexDictionnary (Dictionary dict) throws IOException {

        int ng1, ng2;
        IndexReader.unlock(spellindex);
        IndexWriter writer=new IndexWriter(spellindex, new WhitespaceAnalyzer(), !IndexReader.indexExists(spellindex));
        writer.mergeFactor=300;
        writer.minMergeDocs=150;

        Iterator iter=dict.getWordsIterator();
        while (iter.hasNext()) {
            String word=(String) iter.next();

            int len=word.length();
            if (len<3) {
                continue; // too short we bail but "too long" is fine...
            }

            if (this.exist(word)) { // if the word already exist in the gramindex
                continue;
            }

            // ok index the word
            Document doc=createDocument(word, getMin(len), getMax(len));
            writer.addDocument(doc);
        }
        // close writer
        writer.optimize();
        writer.close();

        // close reader
        reader.close();
        reader=null;
    }


    private int getMin (int l) {
        if (l>5) {
            return 3;
        }
        if (l==5) {
            return 2;
        }
        return 1;
    }


    private int getMax (int l) {
        if (l>5) {
            return 4;
        }
        if (l==5) {
            return 3;
        }
        return 2;

    }


    private static Document createDocument (String text, int ng1, int ng2) {
        Document doc=new Document();
        doc.add(Field.Keyword(F_WORD, text)); // orig term
        addGram(text, doc, ng1, ng2);
        return doc;
    }


    private static void addGram (String text, Document doc, int ng1, int ng2) {
        int len=text.length();
        for (int ng=ng1; ng<=ng2; ng++) {
            String key="gram"+ng;
            String end=null;
            for (int i=0; i<len-ng+1; i++) {
                String gram=text.substring(i, i+ng);
                doc.add(Field.Keyword(key, gram));
                if (i==0) {
                    doc.add(Field.Keyword("start"+ng, gram));
                }
                end=gram;
            }
            if (end!=null) { // may not be present if len==ng1
                doc.add(Field.Keyword("end"+ng, end));
            }
        }
    }


    protected void finalize () throws Throwable {
        if (reader!=null) {
            reader.close();
        }
    }

}
