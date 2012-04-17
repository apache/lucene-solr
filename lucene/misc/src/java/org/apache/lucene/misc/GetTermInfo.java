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

import java.io.File;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.IndexReader;

/**
 * Utility to get document frequency and total number of occurrences (sum of the tf for each doc)  of a term. 
 */
public class GetTermInfo {
  
  public static void main(String[] args) throws Exception {
    
    FSDirectory dir = null;
    String inputStr = null;
    String field = null;
    
    if (args.length == 3) {
      dir = FSDirectory.open(new File(args[0]));
      field = args[1];
      inputStr = args[2];
    } else {
      usage();
      System.exit(1);
    }
      
    getTermInfo(dir,field, new BytesRef(inputStr));
  }
  
  public static void getTermInfo(Directory dir, String field, BytesRef termtext) throws Exception {
    IndexReader reader = IndexReader.open(dir);
    long totalTF = HighFreqTerms.getTotalTermFreq(reader, field, termtext);
    System.out.printf("%s:%s \t totalTF = %,d \t doc freq = %,d \n",
         field, termtext.utf8ToString(), totalTF, reader.docFreq(field, termtext)); 
  }
   
  private static void usage() {
    System.out
        .println("\n\nusage:\n\t"
            + "java " + GetTermInfo.class.getName() + " <index dir> field term \n\n");
  }
}
