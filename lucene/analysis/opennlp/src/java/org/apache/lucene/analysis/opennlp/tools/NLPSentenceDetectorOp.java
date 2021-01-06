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
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.Span;

/**
 * Supply OpenNLP Sentence Detector tool Requires binary models from OpenNLP project on SourceForge.
 */
public class NLPSentenceDetectorOp {
  private final SentenceDetectorME sentenceSplitter;

  public NLPSentenceDetectorOp(SentenceModel model) throws IOException {
    sentenceSplitter = new SentenceDetectorME(model);
  }

  public NLPSentenceDetectorOp() {
    sentenceSplitter = null;
  }

  public synchronized Span[] splitSentences(String line) {
    if (sentenceSplitter != null) {
      return sentenceSplitter.sentPosDetect(line);
    } else {
      Span[] shorty = new Span[1];
      shorty[0] = new Span(0, line.length());
      return shorty;
    }
  }
}
