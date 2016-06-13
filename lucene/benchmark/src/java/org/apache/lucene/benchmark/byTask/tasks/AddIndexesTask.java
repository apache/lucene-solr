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
package org.apache.lucene.benchmark.byTask.tasks;


import java.nio.file.Paths;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Adds an input index to an existing index, using
 * {@link IndexWriter#addIndexes(Directory...)} or
 * {@link IndexWriter#addIndexes(CodecReader...)}. The location of the input
 * index is specified by the parameter {@link #ADDINDEXES_INPUT_DIR} and is
 * assumed to be a directory on the file system.
 * <p>
 * Takes optional parameter {@code useAddIndexesDir} which specifies which
 * addIndexes variant to use (defaults to true, to use addIndexes(Directory)).
 */
public class AddIndexesTask extends PerfTask {

  public static final String ADDINDEXES_INPUT_DIR = "addindexes.input.dir";

  public AddIndexesTask(PerfRunData runData) {
    super(runData);
  }

  private boolean useAddIndexesDir = true;
  private FSDirectory inputDir;
  
  @Override
  public void setup() throws Exception {
    super.setup();
    String inputDirProp = getRunData().getConfig().get(ADDINDEXES_INPUT_DIR, null);
    if (inputDirProp == null) {
      throw new IllegalArgumentException("config parameter " + ADDINDEXES_INPUT_DIR + " not specified in configuration");
    }
    inputDir = FSDirectory.open(Paths.get(inputDirProp));
  }
  
  @Override
  public int doLogic() throws Exception {
    IndexWriter writer = getRunData().getIndexWriter();
    if (useAddIndexesDir) {
      writer.addIndexes(inputDir);
    } else {
      try (IndexReader r = DirectoryReader.open(inputDir)) {
        CodecReader leaves[] = new CodecReader[r.leaves().size()];
        int i = 0;
        for (LeafReaderContext leaf : r.leaves()) {
          leaves[i++] = SlowCodecReaderWrapper.wrap(leaf.reader());
        }
        writer.addIndexes(leaves);
      }
    }
    return 1;
  }
  
  /**
   * Set the params (useAddIndexesDir only)
   * 
   * @param params
   *          {@code useAddIndexesDir=true} for using
   *          {@link IndexWriter#addIndexes(Directory...)} or {@code false} for
   *          using {@link IndexWriter#addIndexes(CodecReader...)}. Defaults to
   *          {@code true}.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    useAddIndexesDir = Boolean.parseBoolean(params); 
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
  
  @Override
  public void tearDown() throws Exception {
    inputDir.close();
    super.tearDown();
  }
  
}
