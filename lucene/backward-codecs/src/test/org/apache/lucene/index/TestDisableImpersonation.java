package org.apache.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40RWCodec;
import org.apache.lucene.codecs.lucene41.Lucene41RWCodec;
import org.apache.lucene.codecs.lucene42.Lucene42RWCodec;
import org.apache.lucene.codecs.lucene45.Lucene45RWCodec;
import org.apache.lucene.codecs.lucene46.Lucene46RWCodec;
import org.apache.lucene.codecs.lucene49.Lucene49RWCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

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

public class TestDisableImpersonation extends LuceneTestCase {

  public void testDisableImpersonation() throws Exception {
    Codec[] oldCodecs = new Codec[] { new Lucene40RWCodec(), new Lucene41RWCodec(), new Lucene42RWCodec(), new Lucene45RWCodec(), new Lucene46RWCodec(), new Lucene49RWCodec() };
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(oldCodecs[random().nextInt(oldCodecs.length)]);
    IndexWriter writer = new IndexWriter(dir, conf);

    Document doc = new Document();
    doc.add(new StringField("f", "bar", Store.YES));
    doc.add(new NumericDocValuesField("n", 18L));

    OLD_FORMAT_IMPERSONATION_IS_ACTIVE = false;
    try {
      writer.addDocument(doc);
      writer.close();
      fail("should not have succeeded to impersonate an old format!");
    } catch (UnsupportedOperationException e) {
      writer.rollback();
    } finally {
      OLD_FORMAT_IMPERSONATION_IS_ACTIVE = true;
    }

    dir.close();
  }

}
