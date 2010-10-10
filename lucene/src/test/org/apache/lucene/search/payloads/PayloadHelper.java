package org.apache.lucene.search.payloads;

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

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.English;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowMultiReaderWrapper;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;

import static org.apache.lucene.util.LuceneTestCase.TEST_VERSION_CURRENT;

import java.io.Reader;
import java.io.IOException;

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



    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
      result = new PayloadFilter(result, fieldName);
      return result;
    }
  }

  public final class PayloadFilter extends TokenFilter {
    String fieldName;
    int numSeen = 0;
    PayloadAttribute payloadAtt;
    
    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
      payloadAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      
      if (input.incrementToken()) {
        if (fieldName.equals(FIELD))
        {
          payloadAtt.setPayload(new Payload(payloadField));
        }
        else if (fieldName.equals(MULTI_FIELD))
        {
          if (numSeen  % 2 == 0)
          {
            payloadAtt.setPayload(new Payload(payloadMultiField1));
          }
          else
          {
            payloadAtt.setPayload(new Payload(payloadMultiField2));
          }
          numSeen++;
        }
        return true;
      }
      return false;
    }
  }

  /**
   * Sets up a RAMDirectory, and adds documents (using English.intToEnglish()) with two fields: field and multiField
   * and analyzes them using the PayloadAnalyzer
   * @param similarity The Similarity class to use in the Searcher
   * @param numDocs The num docs to add
   * @return An IndexSearcher
   * @throws IOException
   */
  // TODO: randomize
  public IndexSearcher setUp(Similarity similarity, int numDocs) throws IOException {
    Directory directory = new MockDirectoryWrapper(new RAMDirectory());
    PayloadAnalyzer analyzer = new PayloadAnalyzer();

    // TODO randomize this
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer).setSimilarity(similarity));
    // writer.infoStream = System.out;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new Field(FIELD, English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field(MULTI_FIELD, English.intToEnglish(i) + "  " + English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field(NO_PAYLOAD_FIELD, English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    reader = IndexReader.open(writer);
    writer.close();

    IndexSearcher searcher = new IndexSearcher(SlowMultiReaderWrapper.wrap(reader));
    searcher.setSimilarity(similarity);
    return searcher;
  }

  public void tearDown() throws Exception {
    reader.close();
  }
}
