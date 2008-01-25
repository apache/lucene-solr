package org.apache.lucene;

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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.demo.FileDocument;

import java.io.File;
import java.util.Date;

class IndexTest {
  public static void main(String[] args) {
    try {
      Date start = new Date();
      IndexWriter writer = new IndexWriter(File.createTempFile("luceneTest", "idx"),
        new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

      writer.setMergeFactor(20);

      indexDocs(writer, new File("/tmp"));

      writer.optimize();
      writer.close();

      Date end = new Date();

      System.out.print(end.getTime() - start.getTime());
      System.out.println(" total milliseconds");

      Runtime runtime = Runtime.getRuntime();

      System.out.print(runtime.freeMemory());
      System.out.println(" free memory before gc");
      System.out.print(runtime.totalMemory());
      System.out.println(" total memory before gc");

      runtime.gc();

      System.out.print(runtime.freeMemory());
      System.out.println(" free memory after gc");
      System.out.print(runtime.totalMemory());
      System.out.println(" total memory after gc");

    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
          "\n with message: " + e.getMessage());
    }
  }

  public static void indexDocs(IndexWriter writer, File file)
    throws Exception {
    if (file.isDirectory()) {
      String[] files = file.list();
      for (int i = 0; i < files.length; i++)
        indexDocs(writer, new File(file, files[i]));
    } else {
      System.out.println("adding " + file);
      writer.addDocument(FileDocument.Document(file));
    }
  }
}
