package org.apache.lucene.wordnet;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import java.io.IOException;

public class SynLookup {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println(
    "java org.apache.lucene.wordnet.SynLookup <index path> <word>");
    }

    FSDirectory directory = FSDirectory.getDirectory(args[0], false);
    IndexSearcher searcher = new IndexSearcher(directory);

    String word = args[1];
    Hits hits = searcher.search(
      new TermQuery(new Term("word", word)));

    if (hits.length() == 0) {
      System.out.println("No synonyms found for " + word);
    } else {
      System.out.println("Synonyms found for \"" + word + "\":");
    }

    for (int i = 0; i < hits.length(); i++) {
      Document doc = hits.doc(i);

      String[] values = doc.getValues("syn");

      for (int j = 0; j < values.length; j++) {
        System.out.println(values[j]);
      }
    }

    searcher.close();
    directory.close();
  }
}
