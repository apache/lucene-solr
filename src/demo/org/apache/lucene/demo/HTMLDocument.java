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

import java.io.*;
import org.apache.lucene.document.*;
import org.apache.lucene.demo.html.HTMLParser;

/** A utility for making Lucene Documents for HTML documents. */

public class HTMLDocument {
  static char dirSep = System.getProperty("file.separator").charAt(0);

  public static String uid(File f) {
    // Append path and date into a string in such a way that lexicographic
    // sorting gives the same results as a walk of the file hierarchy.  Thus
    // null (\u0000) is used both to separate directory components and to
    // separate the path from the date.
    return f.getPath().replace(dirSep, '\u0000') +
      "\u0000" +
      DateField.timeToString(f.lastModified());
  }

  public static String uid2url(String uid) {
    String url = uid.replace('\u0000', '/');	  // replace nulls with slashes
    return url.substring(0, url.lastIndexOf('/')); // remove date from end
  }

  public static Document Document(File f)
       throws IOException, InterruptedException  {
    // make a new, empty document
    Document doc = new Document();

    // Add the url as a field named "url".  Use an UnIndexed field, so
    // that the url is just stored with the document, but is not searchable.
    doc.add(Field.UnIndexed("url", f.getPath().replace(dirSep, '/')));

    // Add the last modified date of the file a field named "modified".  Use a
    // Keyword field, so that it's searchable, but so that no attempt is made
    // to tokenize the field into words.
    doc.add(Field.Keyword("modified",
			  DateField.timeToString(f.lastModified())));

    // Add the uid as a field, so that index can be incrementally maintained.
    // This field is not stored with document, it is indexed, but it is not
    // tokenized prior to indexing.
    doc.add(new Field("uid", uid(f), false, true, false));

    HTMLParser parser = new HTMLParser(f);

    // Add the tag-stripped contents as a Reader-valued Text field so it will
    // get tokenized and indexed.
    doc.add(Field.Text("contents", parser.getReader()));

    // Add the summary as an UnIndexed field, so that it is stored and returned
    // with hit documents for display.
    doc.add(Field.UnIndexed("summary", parser.getSummary()));

    // Add the title as a separate Text field, so that it can be searched
    // separately.
    doc.add(Field.Text("title", parser.getTitle()));

    // return the document
    return doc;
  }

  private HTMLDocument() {}
}
    
