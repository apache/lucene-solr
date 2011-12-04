package org.apache.lucene.search.suggest.fst;

import java.io.*;

import org.apache.lucene.util.BytesRef;

/**
 * Try to build a suggester from a large data set. The input is a simple text
 * file, newline-delimited.
 */
public class LargeInputFST {
  public static void main(String[] args) throws IOException {
    File input = new File("/home/dweiss/tmp/shuffled.dict");

    int buckets = 20;
    int shareMaxTail = 10;

    ExternalRefSorter sorter = new ExternalRefSorter(new Sort());
    FSTCompletionBuilder builder = new FSTCompletionBuilder(buckets, sorter, shareMaxTail);

    BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            new FileInputStream(input), "UTF-8"));
    
    BytesRef scratch = new BytesRef();
    String line;
    int count = 0;
    while ((line = reader.readLine()) != null) {
      scratch.copyChars(line);
      builder.add(scratch, count % buckets);
      if ((count++ % 100000) == 0) {
        System.err.println("Line: " + count);
      }
    }

    System.out.println("Building FSTCompletion.");
    FSTCompletion completion = builder.build();

    File fstFile = new File("completion.fst");
    System.out.println("Done. Writing automaton: " + fstFile.getAbsolutePath());
    completion.getFST().save(fstFile);
  }
}
