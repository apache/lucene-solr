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

package org.apache.lucene.analysis.opennlp.tools;

import java.io.IOException;
import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;

/** Supply OpenNLP Chunking tool Requires binary models from OpenNLP project on SourceForge. */
public class NLPChunkerOp {
  private ChunkerME chunker = null;

  public NLPChunkerOp(ChunkerModel chunkerModel) throws IOException {
    chunker = new ChunkerME(chunkerModel);
  }

  public synchronized String[] getChunks(String[] words, String[] tags, double[] probs) {
    String[] chunks = chunker.chunk(words, tags);
    if (probs != null) chunker.probs(probs);
    return chunks;
  }
}
