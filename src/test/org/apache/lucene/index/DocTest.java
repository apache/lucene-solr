package org.apache.lucene.index;

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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;
import demo.FileDocument;

import java.io.File;
import java.util.Date;


class DocTest {
  public static void main(String[] args) {
    try {
      Directory directory = FSDirectory.getDirectory("test", true);
      directory.close();

      indexDoc("one", "test.txt");
      printSegment("one");
      indexDoc("two", "test2.txt");
      printSegment("two");
      
      merge("one", "two", "merge");
      printSegment("merge");

      merge("one", "two", "merge2");
      printSegment("merge2");

      merge("merge", "merge2", "merge3");
      printSegment("merge3");

    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }

  public static void indexDoc(String segment, String fileName)
       throws Exception {
    Directory directory = FSDirectory.getDirectory("test", false);
    Analyzer analyzer = new SimpleAnalyzer();
    DocumentWriter writer = new DocumentWriter(directory, analyzer, 1000);

    File file = new File(fileName);
    Document doc = FileDocument.Document(file);

    writer.addDocument(segment, doc);

    directory.close();
  }

  static void merge(String seg1, String seg2, String merged)
       throws Exception {
    Directory directory = FSDirectory.getDirectory("test", false);

    SegmentReader r1 = new SegmentReader(new SegmentInfo(seg1, 1, directory));
    SegmentReader r2 = new SegmentReader(new SegmentInfo(seg2, 1, directory));

    SegmentMerger merger = new SegmentMerger(directory, merged);
    merger.add(r1);
    merger.add(r2);
    merger.merge();

    directory.close();
  }

  static void printSegment(String segment)
       throws Exception {
    Directory directory = FSDirectory.getDirectory("test", false);
    SegmentReader reader =
      new SegmentReader(new SegmentInfo(segment, 1, directory));

    for (int i = 0; i < reader.numDocs(); i++)
      System.out.println(reader.document(i));
    
    TermEnum tis = reader.terms();
    while (tis.next()) {
      System.out.print(tis.term());
      System.out.println(" DF=" + tis.docFreq());
      
      TermPositions positions = reader.termPositions(tis.term());
      try {
	while (positions.next()) {
	  System.out.print(" doc=" + positions.doc());
	  System.out.print(" TF=" + positions.freq());
	  System.out.print(" pos=");
	  System.out.print(positions.nextPosition());
	  for (int j = 1; j < positions.freq(); j++)
	    System.out.print("," + positions.nextPosition());
	  System.out.println("");
	}
      } finally {
	positions.close();
      }
    }
    tis.close();
    reader.close();
    directory.close();
  }
}
