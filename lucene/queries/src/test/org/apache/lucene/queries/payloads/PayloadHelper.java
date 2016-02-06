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
package org.apache.lucene.queries.payloads;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.Random;

/**
 *
 *
 **/
public class PayloadHelper {

  private byte[] payloadField = new byte[]{1};
  private byte[] payloadMultiField1 = new byte[]{2};
  private byte[] payloadMultiField2 = new byte[]{4};
  public static final String NO_PAYLOAD_FIELD = "noPayloadField";
  public static final String MULTI_FIELD = "multiField";
  public static final String FIELD = "field";

  public IndexReader reader;

  public final class PayloadAnalyzer extends Analyzer {

    public PayloadAnalyzer() {
      super(PER_FIELD_REUSE_STRATEGY);
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result, fieldName));
    }
  }

  public final class PayloadFilter extends TokenFilter {
    private final String fieldName;
    private int numSeen = 0;
    private final PayloadAttribute payloadAtt;
    
    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
      payloadAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      
      if (input.incrementToken()) {
        if (fieldName.equals(FIELD)) {
          payloadAtt.setPayload(new BytesRef(payloadField));
        } else if (fieldName.equals(MULTI_FIELD)) {
          if (numSeen  % 2 == 0) {
            payloadAtt.setPayload(new BytesRef(payloadMultiField1));
          }
          else {
            payloadAtt.setPayload(new BytesRef(payloadMultiField2));
          }
          numSeen++;
        }
        return true;
      }
      return false;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.numSeen = 0;
    }
  }

  /**
   * Sets up a RAMDirectory, and adds documents (using English.intToEnglish()) with two fields: field and multiField
   * and analyzes them using the PayloadAnalyzer
   * @param similarity The Similarity class to use in the Searcher
   * @param numDocs The num docs to add
   * @return An IndexSearcher
   */
  // TODO: randomize
  public IndexSearcher setUp(Random random, Similarity similarity, int numDocs) throws IOException {
    Directory directory = new MockDirectoryWrapper(random, new RAMDirectory());
    PayloadAnalyzer analyzer = new PayloadAnalyzer();

    // TODO randomize this
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(
        analyzer).setSimilarity(similarity));
    // writer.infoStream = System.out;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new TextField(FIELD, English.intToEnglish(i), Field.Store.YES));
      doc.add(new TextField(MULTI_FIELD, English.intToEnglish(i) + "  " + English.intToEnglish(i), Field.Store.YES));
      doc.add(new TextField(NO_PAYLOAD_FIELD, English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = DirectoryReader.open(writer);
    writer.close();

    IndexSearcher searcher = LuceneTestCase.newSearcher(reader);
    searcher.setSimilarity(similarity);
    return searcher;
  }

  public void tearDown() throws Exception {
    reader.close();
  }
}
