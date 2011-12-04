package org.apache.lucene.index;

/**
 * Copyright 2006 The Apache Software Foundation
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
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ReaderUtil;

/**
 * Given a directory and a list of fields, updates the fieldNorms in place for every document.
 * 
 * If Similarity class is specified, uses its computeNorm method to set norms.
 * If -n command line argument is used, removed field norms, as if 
 * {@link org.apache.lucene.document.FieldType#setOmitNorms(boolean)} was used.
 *
 * <p>
 * NOTE: This will overwrite any length normalization or field/document boosts.
 * </p>
 *
 */
public class FieldNormModifier {

  /**
   * Command Line Execution method.
   *
   * <pre>
   * Usage: FieldNormModifier /path/index &lt;package.SimilarityClassName | -n&gt; field1 field2 ...
   * </pre>
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: FieldNormModifier <index> <package.SimilarityClassName | -d> <field1> [field2] ...");
      System.exit(1);
    }

    SimilarityProvider s = null;

    if (args[1].equals("-d"))
      args[1] = DefaultSimilarity.class.getName();

    try {
      s = Class.forName(args[1]).asSubclass(SimilarityProvider.class).newInstance();
    } catch (Exception e) {
      System.err.println("Couldn't instantiate similarity with empty constructor: " + args[1]);
      e.printStackTrace(System.err);
      System.exit(1);
    }

    Directory d = FSDirectory.open(new File(args[0]));
    FieldNormModifier fnm = new FieldNormModifier(d, s);

    for (int i = 2; i < args.length; i++) {
      System.out.print("Updating field: " + args[i] + " " + (new Date()).toString() + " ... ");
      fnm.reSetNorms(args[i]);
      System.out.println(new Date().toString());
    }
    
    d.close();
  }
  
  
  private Directory dir;
  private SimilarityProvider sim;
  
  /**
   * Constructor for code that wishes to use this class programmatically
   * If Similarity is null, kill the field norms.
   *
   * @param d the Directory to modify
   * @param s the Similarity to use (can be null)
   */
  public FieldNormModifier(Directory d, SimilarityProvider s) {
    dir = d;
    sim = s;
  }

  /**
   * Resets the norms for the specified field.
   *
   * <p>
   * Opens a new IndexReader on the Directory given to this instance,
   * modifies the norms (either using the Similarity given to this instance, or by using fake norms,
   * and closes the IndexReader.
   * </p>
   *
   * @param field the field whose norms should be reset
   */
  public void reSetNorms(String field) throws IOException {
    Similarity fieldSim = sim.get(field); 
    IndexReader reader = null;
    try {
      reader = IndexReader.open(dir, false);

      final List<IndexReader> subReaders = new ArrayList<IndexReader>();
      ReaderUtil.gatherSubReaders(subReaders, reader);

      final FieldInvertState invertState = new FieldInvertState();
      for(IndexReader subReader : subReaders) {
        final Bits liveDocs = subReader.getLiveDocs();

        int[] termCounts = new int[subReader.maxDoc()];
        Fields fields = subReader.fields();
        if (fields != null) {
          Terms terms = fields.terms(field);
          if (terms != null) {
            TermsEnum termsEnum = terms.iterator(null);
            DocsEnum docs = null;
            DocsEnum docsAndFreqs = null;
            while(termsEnum.next() != null) {
              docsAndFreqs = termsEnum.docs(liveDocs, docsAndFreqs, true);
              final DocsEnum docs2;
              if (docsAndFreqs != null) {
                docs2 = docsAndFreqs;
              } else {
                docs2 = docs = termsEnum.docs(liveDocs, docs, false);
              }
              while(true) {
                int docID = docs2.nextDoc();
                if (docID != docs.NO_MORE_DOCS) {
                  termCounts[docID] += docsAndFreqs == null ? 1 : docsAndFreqs.freq();
                } else {
                  break;
                }
              }
            }
          }
        }

        invertState.setBoost(1.0f);
        for (int d = 0; d < termCounts.length; d++) {
          if (liveDocs == null || liveDocs.get(d)) {
            invertState.setLength(termCounts[d]);
            subReader.setNorm(d, field, fieldSim.computeNorm(invertState));
          }
        }
      }
      
    } finally {
      if (null != reader) reader.close();
    }
  }
}
