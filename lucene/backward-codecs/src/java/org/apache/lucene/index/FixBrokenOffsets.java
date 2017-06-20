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
package org.apache.lucene.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Command-line tool that reads from a source index and
 * writes to a dest index, correcting any broken offsets
 * in the process.
 *
 * @lucene.experimental
 */
public class FixBrokenOffsets {
  public SegmentInfos infos;

  FSDirectory fsDir;

  Path dir;

  @SuppressForbidden(reason = "System.out required: command line tool")
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: FixBrokenOffsetse <srcDir> <destDir>");
      return;
    }
    Path srcPath = Paths.get(args[0]);
    if (!Files.exists(srcPath)) {
      throw new RuntimeException("srcPath " + srcPath.toAbsolutePath() + " doesn't exist");
    }
    Path destPath = Paths.get(args[1]);
    if (Files.exists(destPath)) {
      throw new RuntimeException("destPath " + destPath.toAbsolutePath() + " already exists; please remove it and re-run");
    }
    Directory srcDir = FSDirectory.open(srcPath);
    DirectoryReader reader = DirectoryReader.open(srcDir);

    List<LeafReaderContext> leaves = reader.leaves();
    CodecReader[] filtered = new CodecReader[leaves.size()];
    for(int i=0;i<leaves.size();i++) {
      filtered[i] = SlowCodecReaderWrapper.wrap(new FilterLeafReader(leaves.get(i).reader()) {
          @Override
          public Fields getTermVectors(int docID) throws IOException {
            Fields termVectors = in.getTermVectors(docID);
            if (termVectors == null) {
              return null;
            }
            return new FilterFields(termVectors) {
              @Override
              public Terms terms(String field) throws IOException {
                return new FilterTerms(super.terms(field)) {
                  @Override
                  public TermsEnum iterator() throws IOException {
                    return new FilterTermsEnum(super.iterator()) {
                      @Override
                      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                        return new FilterPostingsEnum(super.postings(reuse, flags)) {
                          int nextLastStartOffset = 0;
                          int lastStartOffset = 0;

                          @Override
                          public int nextPosition() throws IOException {
                            int pos = super.nextPosition();
                            lastStartOffset = nextLastStartOffset;
                            nextLastStartOffset = startOffset();
                            return pos;
                          }
                          
                          @Override
                          public int startOffset() throws IOException {
                            int offset = super.startOffset();
                            if (offset < lastStartOffset) {
                              offset = lastStartOffset;
                            }
                            return offset;
                          }
                          
                          @Override
                          public int endOffset() throws IOException {
                            int offset = super.endOffset();
                            if (offset < lastStartOffset) {
                              offset = lastStartOffset;
                            }
                            return offset;
                          }
                        };
                      }
                    };
                  }
                };
              }
            };
          }

          @Override
          public CacheHelper getCoreCacheHelper() {
            return null;
          }

          @Override
          public CacheHelper getReaderCacheHelper() {
            return null;
          }
        });
    }

    Directory destDir = FSDirectory.open(destPath);
    // We need to maintain the same major version
    int createdMajor = SegmentInfos.readLatestCommit(srcDir).getIndexCreatedVersionMajor();
    new SegmentInfos(createdMajor).commit(destDir);
    IndexWriter writer = new IndexWriter(destDir, new IndexWriterConfig());
    writer.addIndexes(filtered);
    IOUtils.close(writer, reader, srcDir, destDir);
  }
}
