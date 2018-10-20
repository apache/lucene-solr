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
package org.apache.lucene.codecs.blockterms;


import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;

/** 
 * Base class for terms index implementations to plug
 * into {@link BlockTermsWriter}.
 * 
 * @see TermsIndexReaderBase
 * @lucene.experimental 
 */
public abstract class TermsIndexWriterBase implements Closeable {

  /**
   * Terms index API for a single field.
   */
  public abstract class FieldWriter {
    public abstract boolean checkIndexTerm(BytesRef text, TermStats stats) throws IOException;
    public abstract void add(BytesRef text, TermStats stats, long termsFilePointer) throws IOException;
    public abstract void finish(long termsFilePointer) throws IOException;
  }

  public abstract FieldWriter addField(FieldInfo fieldInfo, long termsFilePointer) throws IOException;
}
