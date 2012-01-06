package org.apache.lucene.analysis.kuromoji;

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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.lucene.analysis.kuromoji.Tokenizer.Mode;
import org.apache.lucene.analysis.kuromoji.dict.*;
import org.apache.lucene.analysis.kuromoji.viterbi.GraphvizFormatter;
import org.apache.lucene.analysis.kuromoji.viterbi.Viterbi;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode;

public class DebugTokenizer {
  
  private GraphvizFormatter formatter;
  
  private Viterbi viterbi;
  
  protected DebugTokenizer(UserDictionary userDictionary, Mode mode) {
    
    final ConnectionCosts costs = ConnectionCosts.getInstance();
    this.viterbi = new Viterbi(TokenInfoDictionary.getInstance(),
        UnknownDictionary.getInstance(),
        costs,
        userDictionary,
        mode);
    
    this.formatter = new GraphvizFormatter(costs);
  }
  
  public String debugTokenize(String text) {
    ViterbiNode[][][] lattice = this.viterbi.build(text);
    List<ViterbiNode> bestPath = this.viterbi.search(lattice);
    return this.formatter.format(lattice[0], lattice[1], bestPath);
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  public static class Builder {
    
    private Mode mode = Mode.NORMAL;
    
    private UserDictionary userDictionary = null;
    
    public synchronized Builder mode(Mode mode) {
      this.mode = mode;
      return this;
    }
    
    public synchronized Builder userDictionary(InputStream userDictionaryInputStream)
        throws IOException {
      this.userDictionary = UserDictionary.read(userDictionaryInputStream);
      return this;
    }
    
    public synchronized Builder userDictionary(String userDictionaryPath)
        throws FileNotFoundException, IOException {
      this.userDictionary(new BufferedInputStream(new FileInputStream(userDictionaryPath)));
      return this;
    }
    
    public synchronized DebugTokenizer build() {
      return new DebugTokenizer(userDictionary, mode);
    }
  }
}
