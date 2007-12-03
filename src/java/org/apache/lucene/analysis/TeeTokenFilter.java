package org.apache.lucene.analysis;

import java.io.IOException;


/**
 * Works in conjunction with the SinkTokenizer to provide the ability to set aside tokens
 * that have already been analyzed.  This is useful in situations where multiple fields share
 * many common analysis steps and then go their separate ways.
 * <p/>
 * It is also useful for doing things like entity extraction or proper noun analysis as
 * part of the analysis workflow and saving off those tokens for use in another field.
 *
 * <pre>
SinkTokenizer sink1 = new SinkTokenizer(null);
SinkTokenizer sink2 = new SinkTokenizer(null);

TokenStream source1 = new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(reader1), sink1), sink2);
TokenStream source2 = new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(reader2), sink1), sink2);

TokenStream final1 = new LowerCaseFilter(source1);
TokenStream final2 = source2;
TokenStream final3 = new EntityDetect(sink1);
TokenStream final4 = new URLDetect(sink2);

d.add(new Field("f1", final1));
d.add(new Field("f2", final2));
d.add(new Field("f3", final3));
d.add(new Field("f4", final4));
 * </pre>
 * In this example, sink1 and sink2 will both get tokens from both reader1 and reader2 after whitespace tokenizer
   and now we can further wrap any of these in extra analysis, and more "sources" can be inserted if desired.
 Note, the EntityDetect and URLDetect TokenStreams are for the example and do not currently exist in Lucene
 <p/>
 *
 * See http://issues.apache.org/jira/browse/LUCENE-1058
 * @see SinkTokenizer
 *
 **/
public class TeeTokenFilter extends TokenFilter {
  SinkTokenizer sink;

  public TeeTokenFilter(TokenStream input, SinkTokenizer sink) {
    super(input);
    this.sink = sink;
  }

  public Token next(Token result) throws IOException {
    Token t = input.next(result);
    sink.add(t);
    return t;
  }

}
