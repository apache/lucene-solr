package org.apache.lucene.index;

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

/**
 * Prints the filename and size of each file within a given compound file.
 * Add the -extract flag to extract files to the current working directory.
 * In order to make the extracted version of the index work, you have to copy
 * the segments file from the compound index into the directory where the extracted files are stored.
 * @param args Usage: org.apache.lucene.index.IndexReader [-extract] &lt;cfsfile&gt;
 */

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CommandLineUtil;

/**
 * Command-line tool for extracting sub-files out of a compound file.
 */
public class CompoundFileExtractor {

  public static void main(String [] args) {
    String filename = null;
    boolean extract = false;
    String dirImpl = null;

    int j = 0;
    while(j < args.length) {
      String arg = args[j];
      if ("-extract".equals(arg)) {
        extract = true;
      } else if ("-dir-impl".equals(arg)) {
        if (j == args.length - 1) {
          System.out.println("ERROR: missing value for -dir-impl option");
          System.exit(1);
        }
        j++;
        dirImpl = args[j];
      } else if (filename == null) {
        filename = arg;
      }
      j++;
    }

    if (filename == null) {
      System.out.println("Usage: org.apache.lucene.index.CompoundFileExtractor [-extract] [-dir-impl X] <cfsfile>");
      return;
    }

    Directory dir = null;
    CompoundFileDirectory cfr = null;
    IOContext context = IOContext.READ;

    try {
      File file = new File(filename);
      String dirname = file.getAbsoluteFile().getParent();
      filename = file.getName();
      if (dirImpl == null) {
        dir = FSDirectory.open(new File(dirname));
      } else {
        dir = CommandLineUtil.newFSDirectory(dirImpl, new File(dirname));
      }
      
      cfr = new CompoundFileDirectory(dir, filename, IOContext.DEFAULT, false);

      String [] files = cfr.listAll();
      ArrayUtil.mergeSort(files);   // sort the array of filename so that the output is more readable

      for (int i = 0; i < files.length; ++i) {
        long len = cfr.fileLength(files[i]);

        if (extract) {
          System.out.println("extract " + files[i] + " with " + len + " bytes to local directory...");
          IndexInput ii = cfr.openInput(files[i], context);

          FileOutputStream f = new FileOutputStream(files[i]);

          // read and write with a small buffer, which is more effective than reading byte by byte
          byte[] buffer = new byte[1024];
          int chunk = buffer.length;
          while(len > 0) {
            final int bufLen = (int) Math.min(chunk, len);
            ii.readBytes(buffer, 0, bufLen);
            f.write(buffer, 0, bufLen);
            len -= bufLen;
          }

          f.close();
          ii.close();
        }
        else
          System.out.println(files[i] + ": " + len + " bytes");
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    finally {
      try {
        if (dir != null)
          dir.close();
        if (cfr != null)
          cfr.close();
      }
      catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }
}
