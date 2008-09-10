package org.apache.lucene.search.payloads;


import org.apache.lucene.analysis.*;
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

    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
    }

    public Token next() throws IOException {
      Token result = input.next();
      if (result != null) {
        if (fieldName.equals(FIELD))
        {
          result.setPayload(new Payload(payloadField));
        }
        else if (fieldName.equals(MULTI_FIELD))
        {
          if (numSeen  % 2 == 0)
          {
            result.setPayload(new Payload(payloadMultiField1));
          }
          else
          {
            result.setPayload(new Payload(payloadMultiField2));
          }
          numSeen++;
        }

      }
      return result;
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
