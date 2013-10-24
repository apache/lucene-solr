package org.apache.lucene.facet.util;

/*
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
import java.io.IOException;
import java.io.PrintStream;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader.ChildrenIterator;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/** Prints how many ords are under each dimension. */

// java -cp ../build/core/classes/java:../build/facet/classes/java org.apache.lucene.facet.util.PrintTaxonomyStats -printTree /s2/scratch/indices/wikibig.trunk.noparents.facets.Lucene41.nd1M/facets
public class PrintTaxonomyStats {

  public static void main(String[] args) throws IOException {
    boolean printTree = false;
    String path = null;
    for(int i=0;i<args.length;i++) {
      if (args[i].equals("-printTree")) {
        printTree = true;
      } else {
        path = args[i];
      }
    }
    if (args.length != (printTree ? 2 : 1)) {
      System.out.println("\nUsage: java -classpath ... org.apache.lucene.facet.util.PrintTaxonomyStats [-printTree] /path/to/taxononmy/index\n");
      System.exit(1);
    }
    Directory dir = FSDirectory.open(new File(path));
    TaxonomyReader r = new DirectoryTaxonomyReader(dir);
    printStats(r, System.out, printTree);
    r.close();
    dir.close();
  }

  public static void printStats(TaxonomyReader r, PrintStream out, boolean printTree) throws IOException {
    out.println(r.getSize() + " total categories.");

    ChildrenIterator it = r.getChildren(TaxonomyReader.ROOT_ORDINAL);
    int child;
    while ((child = it.next()) != TaxonomyReader.INVALID_ORDINAL) {
      ChildrenIterator chilrenIt = r.getChildren(child);
      int numImmediateChildren = 0;
      while (chilrenIt.next() != TaxonomyReader.INVALID_ORDINAL) {
        numImmediateChildren++;
      }
      CategoryPath cp = r.getPath(child);
      out.println("/" + cp + ": " + numImmediateChildren + " immediate children; " + (1+countAllChildren(r, child)) + " total categories");
      if (printTree) {
        printAllChildren(out, r, child, "  ", 1);
      }
    }
  }

  private static int countAllChildren(TaxonomyReader r, int ord) throws IOException {
    int count = 0;
    ChildrenIterator it = r.getChildren(ord);
    int child;
    while ((child = it.next()) != TaxonomyReader.INVALID_ORDINAL) {
      count += 1 + countAllChildren(r, child);
    }
    return count;
  }

  private static void printAllChildren(PrintStream out, TaxonomyReader r, int ord, String indent, int depth) throws IOException {
    ChildrenIterator it = r.getChildren(ord);
    int child;
    while ((child = it.next()) != TaxonomyReader.INVALID_ORDINAL) {
      out.println(indent + "/" + r.getPath(child).components[depth]);
      printAllChildren(out, r, child, indent + "  ", depth+1);
    }
  }
}
