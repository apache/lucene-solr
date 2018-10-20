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

import org.apache.lucene.util.LuceneTestCase;

/**
 * Accessor to make some package protected methods in {@link IndexWriter} available for testing.
 * @lucene.internal
 */
public final class IndexWriterMaxDocsChanger  {
  
  private IndexWriterMaxDocsChanger() {}
  
  /**
   * Tells {@link IndexWriter} to enforce the specified limit as the maximum number of documents in one index; call
   * {@link #restoreMaxDocs} once your test is done.
   * @see LuceneTestCase#setIndexWriterMaxDocs(int)
   */
  public static void setMaxDocs(int limit) {
    IndexWriter.setMaxDocs(limit);
  }

  /** 
   * Returns to the default {@link IndexWriter#MAX_DOCS} limit.
   * @see LuceneTestCase#restoreIndexWriterMaxDocs()
   */
  public static void restoreMaxDocs() {
    IndexWriter.setMaxDocs(IndexWriter.MAX_DOCS);
  }

}
