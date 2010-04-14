package org.apache.lucene.misc;

/**
  * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PriorityQueue;

import java.io.File;

/**
 * <code>HighFreqTerms</code> class extracts terms and their frequencies out
 * of an existing Lucene index.
 */
public class HighFreqTerms {
  
  // The top numTerms will be displayed
  public static final int numTerms = 100;

  public static void main(String[] args) throws Exception {
    IndexReader reader = null;
    FSDirectory dir = null;
    String field = null;
    if (args.length == 1) {
      dir = FSDirectory.open(new File(args[0]));
      reader = IndexReader.open(dir, true);
    } else if (args.length == 2) {
      dir = FSDirectory.open(new File(args[0]));
      reader = IndexReader.open(dir, true);
      field = args[1];
    } else {
      usage();
      System.exit(1);
    }

    TermInfoQueue tiq = new TermInfoQueue(numTerms);

    if (field != null) { 
      Fields fields = MultiFields.getFields(reader);
      if (fields == null) {
        return;
      }
      Terms terms = fields.terms(field);
      if (terms != null) {
        TermsEnum termsEnum = terms.iterator();
        while(true) {
          BytesRef term = termsEnum.next();
          if (term != null) {
            tiq.insertWithOverflow(new TermInfo(new Term(field, term.utf8ToString()), termsEnum.docFreq()));
          } else {
            break;
          }    
        }
      }
    } else {
      Fields fields = MultiFields.getFields(reader);
      if (fields == null) {
        return;
      }
      FieldsEnum fieldsEnum = fields.iterator();
      while(true) {
        field = fieldsEnum.next();
        if (field != null) {
          TermsEnum terms = fieldsEnum.terms();
          while(true) {
            BytesRef term = terms.next();
            if (term != null) {
              tiq.insertWithOverflow(new TermInfo(new Term(field, term.toString()), terms.docFreq()));
            } else {
              break;
            }
          }
        } else {
          break;
        }
      }
    }
    
    while (tiq.size() != 0) {
      TermInfo termInfo = tiq.pop();
      System.out.println(termInfo.term + " " + termInfo.docFreq);
    }

    reader.close();
  }

  private static void usage() {
    System.out.println(
         "\n\n"
         + "java org.apache.lucene.misc.HighFreqTerms <index dir> [field]\n\n");
  }
}

final class TermInfo {
  TermInfo(Term t, int df) {
    term = t;
    docFreq = df;
  }
  int docFreq;
  Term term;
}

final class TermInfoQueue extends PriorityQueue<TermInfo> {
  TermInfoQueue(int size) {
    initialize(size);
  }
  @Override
  protected final boolean lessThan(TermInfo termInfoA, TermInfo termInfoB) {
    return termInfoA.docFreq < termInfoB.docFreq;
  }
}
