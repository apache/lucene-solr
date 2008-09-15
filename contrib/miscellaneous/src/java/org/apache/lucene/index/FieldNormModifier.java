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
import java.util.Date;

import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Given a directory and a list of fields, updates the fieldNorms in place for every document.
 * 
 * If Similarity class is specified, uses its lengthNorm method to set norms.
 * If -n command line argument is used, removed field norms, as if 
 * {@link org.apache.lucene.document.Field.Index}.NO_NORMS was used.
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
   * Usage: FieldNormModifier /path/index <package.SimilarityClassName | -n> field1 field2 ...
   * </pre>
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: FieldNormModifier <index> <package.SimilarityClassName | -n> <field1> [field2] ...");
      System.exit(1);
    }

    Similarity s = null;
    if (!args[1].equals("-n")) {
      try {
        Class simClass = Class.forName(args[1]);
        s = (Similarity)simClass.newInstance();
      } catch (Exception e) {
        System.err.println("Couldn't instantiate similarity with empty constructor: " + args[1]);
        e.printStackTrace(System.err);
        System.exit(1);
      }
    }

    Directory d = FSDirectory.getDirectory(args[0], false);
    FieldNormModifier fnm = new FieldNormModifier(d, s);

    for (int i = 2; i < args.length; i++) {
      System.out.print("Updating field: " + args[i] + " " + (new Date()).toString() + " ... ");
      fnm.reSetNorms(args[i]);
      System.out.println(new Date().toString());
    }
    
    d.close();
  }
  
  
  private Directory dir;
  private Similarity sim;
  
  /**
   * Constructor for code that wishes to use this class programatically
   * If Similarity is null, kill the field norms.
   *
   * @param d the Directory to modify
   * @param s the Similiary to use (can be null)
   */
  public FieldNormModifier(Directory d, Similarity s) {
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
    String fieldName = field.intern();
    int[] termCounts = new int[0];
    byte[] fakeNorms = new byte[0];
    
    IndexReader reader = null;
    TermEnum termEnum = null;
    TermDocs termDocs = null;
    try {
      reader = IndexReader.open(dir);
      termCounts = new int[reader.maxDoc()];
      // if we are killing norms, get fake ones
      if (sim == null)
        fakeNorms = SegmentReader.createFakeNorms(reader.maxDoc());
      try {
        termEnum = reader.terms(new Term(field));
        try {
          termDocs = reader.termDocs();
          do {
            Term term = termEnum.term();
            if (term != null && term.field().equals(fieldName)) {
              termDocs.seek(termEnum.term());
              while (termDocs.next()) {
                termCounts[termDocs.doc()] += termDocs.freq();
              }
            }
          } while (termEnum.next());
          
        } finally {
          if (null != termDocs) termDocs.close();
        }
      } finally {
        if (null != termEnum) termEnum.close();
      }
    } finally {
      if (null != reader) reader.close();
    }
    
    try {
      reader = IndexReader.open(dir); 
      for (int d = 0; d < termCounts.length; d++) {
        if (! reader.isDeleted(d)) {
          if (sim == null)
            reader.setNorm(d, fieldName, fakeNorms[0]);
          else
            reader.setNorm(d, fieldName, sim.encodeNorm(sim.lengthNorm(fieldName, termCounts[d])));
        }
      }
      
    } finally {
      if (null != reader) reader.close();
    }
  }
  
}
