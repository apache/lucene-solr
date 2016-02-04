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
package org.apache.lucene.codecs.lucene42;

import java.io.InputStream;
import java.nio.file.Path;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BaseNormsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.TestUtil;

/** Tests Lucene42's norms format */
public class TestLucene42NormsFormat extends BaseNormsFormatTestCase {
  final Codec codec = new Lucene42RWCodec();
  
  @Override
  protected Codec getCodec() {
    return codec;
  }

  /* Copy this back to /l/421/lucene/CreateUndeadNorms.java, then:
   *   - ant clean
   *   - pushd analysis/common; ant jar; popd
   *   - pushd core; ant jar; popd
   *   - javac -cp build/analysis/common/lucene-analyzers-common-4.2.1-SNAPSHOT.jar:build/core/lucene-core-4.2.1-SNAPSHOT.jar CreateUndeadNorms.java
   *   - java -cp .:build/analysis/common/lucene-analyzers-common-4.2.1-SNAPSHOT.jar:build/core/lucene-core-4.2.1-SNAPSHOT.jar CreateUndeadNorms
   *   - cd /tmp/undeadnorms  ; zip index.42.undeadnorms.zip *

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class CreateUndeadNorms {
  public static void main(String[] args) throws Exception {
    File file = new File("/tmp/undeadnorms");
    if (file.exists()) {
      throw new RuntimeException("please remove /tmp/undeadnorms first");
    }
    Directory dir = FSDirectory.open(file);
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_42, new WhitespaceAnalyzer(Version.LUCENE_42)));
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    Field content = new TextField("content", "some content", Field.Store.NO);
    content.setTokenStream(new TokenStream() {
        @Override
        public boolean incrementToken() throws IOException {
          throw new IOException("brains brains!");
        }
      });

    doc.add(content);
    try {
      w.addDocument(doc);
      throw new RuntimeException("didn't hit exception");
    } catch (IOException ioe) {
      // perfect
    }
    w.close();
    dir.close();
  }
}
*/
  /*
   * LUCENE-6006: Test undead norms.
   *                                 .....            
   *                             C C  /            
   *                            /<   /             
   *             ___ __________/_#__=o             
   *            /(- /(\_\________   \              
   *            \ ) \ )_      \o     \             
   *            /|\ /|\       |'     |             
   *                          |     _|             
   *                          /o   __\             
   *                         / '     |             
   *                        / /      |             
   *                       /_/\______|             
   *                      (   _(    <              
   *                       \    \    \             
   *                        \    \    |            
   *                         \____\____\           
   *                         ____\_\__\_\          
   *                       /`   /`     o\          
   *                       |___ |_______|
   *
   */
  public void testReadUndeadNorms() throws Exception {
    InputStream resource = TestLucene42NormsFormat.class.getResourceAsStream("index.42.undeadnorms.zip");
    assertNotNull(resource);
    Path path = createTempDir("undeadnorms");
    TestUtil.unzip(resource, path);
    Directory dir = FSDirectory.open(path);
    IndexReader r = DirectoryReader.open(dir);
    NumericDocValues undeadNorms = MultiDocValues.getNormValues(r, "content");
    assertNotNull(undeadNorms);
    assertEquals(2, r.maxDoc());
    assertEquals(0, undeadNorms.get(0));
    assertEquals(0, undeadNorms.get(1));
    dir.close();
    r.close();
  }
}
