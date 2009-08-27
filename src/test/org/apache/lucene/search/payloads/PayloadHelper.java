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
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.English;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;

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

  public class PayloadAnalyzer extends Analyzer {



    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new LowerCaseTokenizer(reader);
      result = new PayloadFilter(result, fieldName);
      return result;
    }
  }

  public class PayloadFilter extends TokenFilter {
    String fieldName;
    int numSeen = 0;
    PayloadAttribute payloadAtt;
    
    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
      payloadAtt = (PayloadAttribute) addAttribute(PayloadAttribute.class);
    }

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
  public IndexSearcher setUp(Similarity similarity, int numDocs) throws IOException {
    RAMDirectory directory = new RAMDirectory();
    PayloadAnalyzer analyzer = new PayloadAnalyzer();
    IndexWriter writer
            = new IndexWriter(directory, analyzer, true);
    writer.setSimilarity(similarity);
    //writer.infoStream = System.out;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new Field(FIELD, English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field(MULTI_FIELD, English.intToEnglish(i) + "  " + English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field(NO_PAYLOAD_FIELD, English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    //writer.optimize();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);
    searcher.setSimilarity(similarity);
    return searcher;
  }
}
