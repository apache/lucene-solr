package org.apache.lucene.demo;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
//import org.apache.lucene.index.Term;


/** Deletes documents from an index that do not contain a term. */
public class DeleteFiles {
  
  private DeleteFiles() {}                         // singleton

  /** Deletes documents from an index that do not contain a term. */
  public static void main(String[] args) {
    String usage = "java org.apache.lucene.demo.DeleteFiles <unique_term>";
    if (args.length == 0) {
      System.err.println("Usage: " + usage);
      System.exit(1);
    }
    try {
      Directory directory = FSDirectory.getDirectory("index", false);
      IndexReader reader = IndexReader.open(directory);

      Term term = new Term("path", args[0]);
      int deleted = reader.deleteDocuments(term);

      System.out.println("deleted " + deleted +
 			 " documents containing " + term);

      // one can also delete documents by their internal id:
      /*
      for (int i = 0; i < reader.maxDoc(); i++) {
        System.out.println("Deleting document with id " + i);
        reader.delete(i);
      }*/

      reader.close();
      directory.close();

    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }
}
