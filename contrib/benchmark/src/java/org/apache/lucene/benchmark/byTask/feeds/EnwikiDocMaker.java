package org.apache.lucene.benchmark.byTask.feeds;

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

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

/**
 * A {@link DocMaker} which reads the English Wikipedia dump. Uses
 * {@link EnwikiContentSource} as its content source, regardless if a different
 * content source was defined in the configuration.
 * @deprecated Please use {@link DocMaker} instead, with content.source=EnwikiContentSource
 */
public class EnwikiDocMaker extends DocMaker {
  public void setConfig(Config config) {
    super.setConfig(config);
    // Override whatever content source was set in the config
    source = new EnwikiContentSource();
    source.setConfig(config);
    System.out.println("NOTE: EnwikiDocMaker is deprecated; please use DocMaker instead (which is the default if you don't specify doc.maker) with content.source=EnwikiContentSource");
  }
}
