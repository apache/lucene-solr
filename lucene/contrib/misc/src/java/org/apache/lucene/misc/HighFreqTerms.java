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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ReaderUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * 
 * <code>HighFreqTerms</code> class extracts the top n most frequent terms
 * (by document frequency ) from an existing Lucene index and reports their document frequencey.
 * If the -t flag is  and reports both their document frequency and their total tf (total number of occurences) 
 * in order of highest total tf
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
    
    reader = IndexReader.open(dir);
    TermStats[] terms = getHighFreqTerms(reader, numTerms, field);
    if (!IncludeTermFreqs) {
      //default HighFreqTerms behavior
      for (int i = 0; i < terms.length; i++) {
        System.out.printf("%s:%s %,d \n",
            terms[i].field, terms[i].termtext.utf8ToString(), terms[i].docFreq);
      }
    }
    else{
      TermStats[] termsWithTF = sortByTotalTermFreq(reader, terms);
      for (int i = 0; i < termsWithTF.length; i++) {
        System.out.printf("%s:%s \t totalTF = %,d \t doc freq = %,d \n",
            termsWithTF[i].field, termsWithTF[i].termtext.utf8ToString(),
            termsWithTF[i].totalTermFreq, termsWithTF[i].docFreq);
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
  public static TermStats[] getHighFreqTerms(IndexReader reader, int numTerms, String field) throws Exception {
    TermStatsQueue tiq = null;
    
    if (field != null) {
      Fields fields = MultiFields.getFields(reader);
      if (fields == null) {
        throw new RuntimeException("field " + field + " not found");
      }
      Terms terms = fields.terms(field);
      if (terms != null) {
        TermsEnum termsEnum = terms.iterator(null);
        tiq = new TermStatsQueue(numTerms);
        fillQueue(termsEnum, tiq, field);
      }
    } else {
      Fields fields = MultiFields.getFields(reader);
      if (fields == null) {
        throw new RuntimeException("no fields found for this index");
      }
      tiq = new TermStatsQueue(numTerms);
      FieldsEnum fieldsEnum = fields.iterator();
      while (true) {
        field = fieldsEnum.next();
        if (field != null) {
          Terms terms = fieldsEnum.terms();
          if (terms != null) {
            fillQueue(terms.iterator(null), tiq, field);
          }
        } else {
          break;
        }
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
      totalTF = getTotalTermFreq(reader, terms[i].field, terms[i].termtext);
      ts[i] = new TermStats(terms[i].field, terms[i].termtext, terms[i].docFreq, totalTF);
    }
    
    Comparator<TermStats> c = new TotalTermFreqComparatorSortDescending();
    Arrays.sort(ts, c);
    
    return ts;
  }
  
  public static long getTotalTermFreq(IndexReader reader, final String field, final BytesRef termText) throws Exception {   
    final long totalTF[] = new long[1];
    
    new ReaderUtil.Gather(reader) {

      @Override
      protected void add(int base, AtomicReader r) throws IOException {
        Bits liveDocs = r.getLiveDocs();
        if (liveDocs == null) {
          // TODO: we could do this up front, during the scan
          // (next()), instead of after-the-fact here w/ seek,
          // if the codec supports it and there are no del
          // docs...
          final long totTF = r.totalTermFreq(field, termText);
          if (totTF != -1) {
            totalTF[0] += totTF;
            return;
          }
        }
        DocsEnum de = r.termDocsEnum(liveDocs, field, termText, true);
        if (de != null) {
          while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS)
            totalTF[0] += de.freq();
        }
      }
    }.run();
    
    return totalTF[0];
  }
  
  public static void fillQueue(TermsEnum termsEnum, TermStatsQueue tiq, String field) throws Exception {
    
  while (true) {
      BytesRef term = termsEnum.next();
      if (term != null) {
        tiq.insertWithOverflow(new TermStats(field, term, termsEnum.docFreq()));
      } else {
        break;
      }
    }
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

/**
 * Priority queue for TermStats objects ordered by docFreq
 **/
final class TermStatsQueue extends PriorityQueue<TermStats> {
  TermStatsQueue(int size) {
    super(size);
  }
  
  @Override
  protected boolean lessThan(TermStats termInfoA, TermStats termInfoB) {
    return termInfoA.docFreq < termInfoB.docFreq;
  }
}
