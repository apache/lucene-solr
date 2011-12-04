package org.apache.lucene.index.codecs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfo;

// simple api just for now before switching to docvalues apis
public abstract class NormsWriter implements Closeable {

  // TODO: I think IW should set info.normValueType from Similarity,
  // and then this method just returns DocValuesConsumer
  public abstract void startField(FieldInfo info) throws IOException;
  public abstract void writeNorm(byte norm) throws IOException;
  public abstract void finish(int numDocs) throws IOException;
  
}
