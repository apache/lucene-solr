package org.apache.lucene.misc;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PriorityQueue;
import java.util.Arrays;
import java.util.Comparator;

import java.io.File;

/**
 * <code>HighFreqTerms</code> class extracts the top n most frequent terms
 * (by document frequency ) from an existing Lucene index and reports their
 * document frequency.  If used with the -t flag it also reports their 
 * total tf (total number of occurences) in order of highest total tf
 */
public class HighFreqTerms {
  
   // The top numTerms will be displayed
  public static final int DEFAULTnumTerms = 100;
  public static int numTerms = DEFAULTnumTerms;
  
  public static void main(String[] args) throws Exception {
    IndexReader reader = null;
    FSDirectory dir = null;
    String field = null;
    boolean IncludeTermFreqs = false; 
   
    if (args.length == 0 || args.length > 4) {
      usage();
      System.exit(1);
    }     

    if (args.length > 0) {
      dir = FSDirectory.open(new File(args[0]));
    }
   
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-t")) {
        IncludeTermFreqs = true;
      }
      else{
        try {
          numTerms = Integer.parseInt(args[i]);
        } catch (NumberFormatException e) {
          field=args[i];
        }
      }
    }
    
    
    reader = IndexReader.open(dir, true);
    TermStats[] terms = getHighFreqTerms(reader, numTerms, field);
    /*
     * Insert logic so it will only lookup totaltf if right arg
     * also change names as in flex
     */
    if (!IncludeTermFreqs) {
      //default HighFreqTerms behavior
      for (int i = 0; i < terms.length; i++) {
        System.out.printf("%s %,d \n",
            terms[i].term, terms[i].docFreq);
      }
    } else {

      TermStats[] termsWithTF = sortByTotalTermFreq(reader, terms);
      for (int i = 0; i < termsWithTF.length; i++) {
        System.out.printf("%s \t total_tf = %,d \t doc freq = %,d \n",
                          termsWithTF[i].term, termsWithTF[i].totalTermFreq, termsWithTF[i].docFreq);
      }
    }

    reader.close();
 }
  
  private static void usage() {
    System.out
        .println("\n\n"
            + "java org.apache.lucene.misc.HighFreqTerms <index dir> [-t] [number_terms] [field]\n\t -t: include totalTermFreq\n\n");
  } 

  /**
   * 
   * @param reader
   * @param numTerms
   * @param field
   * @return TermStats[] ordered by terms with highest docFreq first.
   * @throws Exception
   */
  public static TermStats[] getHighFreqTerms(IndexReader reader,
      int numTerms, String field) throws Exception {

    TermInfoWiTFQueue tiq = new TermInfoWiTFQueue(numTerms);
    if (field != null) {
      TermEnum terms = reader.terms(new Term(field));
      if (terms != null && terms.term() != null) {
        do {
          if (!terms.term().field().equals(field)) {
            break;
          }
          tiq.insertWithOverflow(new TermStats(terms.term(), terms.docFreq()));
        } while (terms.next());
      } else {
        System.out.println("No terms for field \"" + field + "\"");
      }
    } else {
      TermEnum terms = reader.terms();
      while (terms.next()) {
        tiq.insertWithOverflow(new TermStats(terms.term(), terms.docFreq()));
      }
    }

    TermStats[] result = new TermStats[tiq.size()];

    // we want highest first so we read the queue and populate the array
    // starting at the end and work backwards
    int count = tiq.size() - 1;
    while (tiq.size() != 0) {
      result[count] = tiq.pop();
      count--;
    }
    return result;
  }

  /**
   * Takes array of TermStats. For each term looks up the tf for each doc
   * containing the term and stores the total in the output array of TermStats.
   * Output array is sorted by highest total tf.
   * 
   * @param reader
   * @param terms
   *          TermStats[]
   * @return TermStats[]
   * @throws Exception
   */
 
  public static TermStats[] sortByTotalTermFreq(IndexReader reader, TermStats[] terms) throws Exception {
    TermStats[] ts = new TermStats[terms.length]; // array for sorting
    long totalTF;
    for (int i = 0; i < terms.length; i++) {
      totalTF = getTotalTermFreq(reader, terms[i].term);
      ts[i] = new TermStats( terms[i].term, terms[i].docFreq, totalTF);
    }
    
    Comparator<TermStats> c = new TotalTermFreqComparatorSortDescending();
    Arrays.sort(ts, c);
    
    return ts;
  }
  
  public static long getTotalTermFreq(IndexReader reader, Term term) throws Exception {
    long totalTF = 0;
    TermDocs td = reader.termDocs(term);
    while (td.next()) {
      totalTF += td.freq();
    }
    return totalTF;
  }
}


final class TermStats {
  public Term term;
  public int docFreq;
  public long totalTermFreq;
  
  public TermStats(Term t, int df) {
    this.term = t;
    this.docFreq = df;
  }
  
  public TermStats(Term t, int df, long tf) {
    this.term = t;
    this.docFreq = df;
    this.totalTermFreq = tf;
  }
}


/**
 * Priority queue for TermStats objects ordered by TermStats.docFreq
 **/
final class TermInfoWiTFQueue extends PriorityQueue<TermStats> {
  TermInfoWiTFQueue(int size) {
    initialize(size);
  }
  
  @Override
  protected boolean lessThan(TermStats termInfoA,
      TermStats termInfoB) {
    return termInfoA.docFreq < termInfoB.docFreq;
  }
}

/**
 * Comparator
 * 
 * Reverse of normal Comparator. i.e. returns 1 if a.totalTermFreq is less than
 * b.totalTermFreq So we can sort in descending order of totalTermFreq
 */
final class TotalTermFreqComparatorSortDescending implements Comparator<TermStats> {
  
  public int compare(TermStats a, TermStats b) {
    if (a.totalTermFreq < b.totalTermFreq) {
      return 1;
    } else if (a.totalTermFreq > b.totalTermFreq) {
      return -1;
    } else {
      return 0;
    }
  }
}

