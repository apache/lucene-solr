package org.apache.lucene;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import com.lucene.analysis.StopAnalyzer;
import com.lucene.index.*;
import com.lucene.document.Document;
import com.lucene.util.Arrays;
import demo.HTMLParser.HTMLParser;

import java.io.File;
import java.util.Date;

class IndexHTML {
  private static boolean deleting = false;	  // true during deletion pass
  private static IndexReader reader;		  // existing index
  private static IndexWriter writer;		  // new index being built
  private static TermEnum uidIter;		  // document id iterator

  public static void main(String[] argv) {
    try {
      String index = "index";
      boolean create = false;
      File root = null;
      
      String usage = "IndexHTML [-create] [-index <index>] <root_directory>";

      if (argv.length == 0) {
	System.err.println("Usage: " + usage);
	return;
      }

      for (int i = 0; i < argv.length; i++) {
	if (argv[i].equals("-index")) {		  // parse -index option
	  index = argv[++i];
	} else if (argv[i].equals("-create")) {	  // parse -create option
	  create = true;
	} else if (i != argv.length-1) {
	  System.err.println("Usage: " + usage);
	  return;
	} else
	  root = new File(argv[i]);
      }

      Date start = new Date();

      if (!create) {				  // delete stale docs
	deleting = true;
	indexDocs(root, index, create);
      }

      writer = new IndexWriter(index, new StopAnalyzer(), create);
      writer.mergeFactor = 20;
      writer.maxFieldLength = 1000000;

      indexDocs(root, index, create);		  // add new docs

      System.out.println("Optimizing index...");
      writer.optimize();
      writer.close();

      Date end = new Date();

      System.out.print(end.getTime() - start.getTime());
      System.out.println(" total milliseconds");

    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }

  /* Walk directory hierarchy in uid order, while keeping uid iterator from
  /* existing index in sync.  Mismatches indicate one of: (a) old documents to
  /* be deleted; (b) unchanged documents, to be left alone; or (c) new
  /* documents, to be indexed.
   */

  private static void indexDocs(File file, String index, boolean create)
       throws Exception {
    if (!create) {				  // incrementally update
      
      reader = IndexReader.open(index);		  // open existing index
      uidIter = reader.terms(new Term("uid", "")); // init uid iterator
    
      indexDocs(file);

      if (deleting) {				  // delete rest of stale docs
	while (uidIter.term() != null && uidIter.term().field() == "uid") {
	  System.out.println("deleting " +
			     HTMLDocument.uid2url(uidIter.term().text()));
	  reader.delete(uidIter.term());
	  uidIter.next();
	}
	deleting = false;
      }

      uidIter.close();				  // close uid iterator
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

	while (uidIter.term() != null && uidIter.term().field() == "uid" &&
	       uidIter.term().text().compareTo(uid) < 0) {
	  if (deleting) {			  // delete stale docs
	    System.out.println("deleting " +
			       HTMLDocument.uid2url(uidIter.term().text()));
	    reader.delete(uidIter.term());
	  }
	  uidIter.next();
	}
	if (uidIter.term() != null && uidIter.term().field() == "uid" &&
	    uidIter.term().text().compareTo(uid) == 0) {
	  uidIter.next();			  // keep matching docs
	} else if (!deleting) {			  // add new docs
	  Document doc = HTMLDocument.Document(file);
	  System.out.println("adding " + doc.get("url"));
	writer.addDocument(doc);
	}
      } else {					  // creating a new index
	Document doc = HTMLDocument.Document(file);
	System.out.println("adding " + doc.get("url"));
	writer.addDocument(doc);		  // add docs unconditionally
      }
    }
  }
}
