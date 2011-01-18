package org.apache.lucene.demo;

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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.util.Date;
import java.util.Arrays;

/** Indexer for HTML files. */
public class IndexHTML {
  private IndexHTML() {}

  private static boolean deleting = false;	  // true during deletion pass
  private static IndexReader reader;		  // existing index
  private static IndexWriter writer;		  // new index being built
  private static TermsEnum uidIter;		  // document id iterator

  /** Indexer for HTML files.*/
  public static void main(String[] argv) {
    try {
      File index = new File("index");
      boolean create = false;
      File root = null;

      String usage = "IndexHTML [-create] [-index <index>] <root_directory>";

      if (argv.length == 0) {
        System.err.println("Usage: " + usage);
        return;
      }

      for (int i = 0; i < argv.length; i++) {
        if (argv[i].equals("-index")) {		  // parse -index option
          index = new File(argv[++i]);
        } else if (argv[i].equals("-create")) {	  // parse -create option
          create = true;
        } else if (i != argv.length-1) {
          System.err.println("Usage: " + usage);
          return;
        } else
          root = new File(argv[i]);
      }
      
      if(root == null) {
        System.err.println("Specify directory to index");
        System.err.println("Usage: " + usage);
        return;
      }

      Date start = new Date();

      if (!create) {				  // delete stale docs
        deleting = true;
        indexDocs(root, index, create);
      }
      writer = new IndexWriter(FSDirectory.open(index), new IndexWriterConfig(
          Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT))
          .setOpenMode(create ? OpenMode.CREATE : OpenMode.CREATE_OR_APPEND));
      indexDocs(root, index, create);		  // add new docs

      System.out.println("Optimizing index...");
      writer.optimize();
      writer.close();

      Date end = new Date();

      System.out.print(end.getTime() - start.getTime());
      System.out.println(" total milliseconds");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /* Walk directory hierarchy in uid order, while keeping uid iterator from
  /* existing index in sync.  Mismatches indicate one of: (a) old documents to
  /* be deleted; (b) unchanged documents, to be left alone; or (c) new
  /* documents, to be indexed.
   */

  private static void indexDocs(File file, File index, boolean create)
       throws Exception {
    if (!create) {				  // incrementally update

      reader = IndexReader.open(FSDirectory.open(index), false);		  // open existing index
      Terms terms = MultiFields.getTerms(reader, "uid");
      if (terms != null) {
        uidIter = terms.iterator();

        indexDocs(file);

        if (deleting) {				  // delete rest of stale docs
          BytesRef text;
          while ((text=uidIter.next()) != null) {
            String termText = text.utf8ToString();
            System.out.println("deleting " +
                               HTMLDocument.uid2url(termText));
            reader.deleteDocuments(new Term("uid", termText));
          }
          deleting = false;
        }
      }

      reader.close();				  // close existing index

    } else					  // don't have exisiting
      indexDocs(file);
  }

  private static void indexDocs(File file) throws Exception {
    if (file.isDirectory()) {			  // if a directory
      String[] files = file.list();		  // list its files
      Arrays.sort(files);			  // sort the files
      for (int i = 0; i < files.length; i++)	  // recursively index them
        indexDocs(new File(file, files[i]));

    } else if (file.getPath().endsWith(".html") || // index .html files
      file.getPath().endsWith(".htm") || // index .htm files
      file.getPath().endsWith(".txt")) { // index .txt files

      if (uidIter != null) {
        String uid = HTMLDocument.uid(file);	  // construct uid for doc

        BytesRef text;
        while((text = uidIter.next()) != null) {
          String termText = text.utf8ToString();
          if (termText.compareTo(uid) < 0) {
            if (deleting) {			  // delete stale docs
              System.out.println("deleting " +
                                 HTMLDocument.uid2url(termText));
              reader.deleteDocuments(new Term("uid", termText));
            }
          } else {
            break;
          }
        }
        if (text != null &&
            text.utf8ToString().compareTo(uid) == 0) {
          uidIter.next();			  // keep matching docs
        } else if (!deleting) {			  // add new docs
          Document doc = HTMLDocument.Document(file);
          System.out.println("adding " + doc.get("path"));
          writer.addDocument(doc);
        }
      } else {					  // creating a new index
        Document doc = HTMLDocument.Document(file);
        System.out.println("adding " + doc.get("path"));
        writer.addDocument(doc);		  // add docs unconditionally
      }
    }
  }
}
