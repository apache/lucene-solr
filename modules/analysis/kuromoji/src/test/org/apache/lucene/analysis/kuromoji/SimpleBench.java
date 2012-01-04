package org.apache.lucene.analysis.kuromoji;

import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.kuromoji.Tokenizer.Mode;
import org.apache.lucene.analysis.kuromoji.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

// nocommit: just for basic testing
// java -cp modules/analysis/build/kuromoji/classes/java:modules/analysis/build/kuromoji/classes/test:modules/analysis/build/common/classes/java:lucene/build/classes/java org.apache.lucene.analysis.kuromoji.SimpleBench
public class SimpleBench {
  
  public static void main(String args[]) throws Exception {
    org.apache.lucene.analysis.kuromoji.Tokenizer tokenizer = 
        org.apache.lucene.analysis.kuromoji.Tokenizer.builder().mode(Mode.NORMAL).build();
    Analyzer a = new KuromojiAnalyzer(tokenizer);
    Analyzer b = new CJKAnalyzer(Version.LUCENE_CURRENT);
    
    /* slight warmup */
    consume(a, "fsdfdsfsdfdsf sdfdsfds fsdf dsfds");
    consume(a, "多くの学生が試験に落ちた。");
    
    for (int i = 0; i < 4; i++) {
      long ms = System.currentTimeMillis();
      for (int j = 0; j < 50000; j++) {
        consume(a, "魔女狩大将マシュー・ホプキンス。 魔女狩大将マシュー・ホプキンス。");
      }
      long ms2 = System.currentTimeMillis();
      for (int j = 0; j < 50000; j++) {
        consume(b, "魔女狩大将マシュー・ホプキンス。 魔女狩大将マシュー・ホプキンス。");
      }
      long ms3 = System.currentTimeMillis();
      if (i != 0 /* exclude first round */) {
        System.out.println("KUROMOJI: " + (ms2 - ms));
        System.out.println("CJK: " + (ms3 - ms2));
      }
    }
  }
  
  public static void consume(Analyzer a, String s) throws Exception {
    TokenStream ts = a.tokenStream("foo", new StringReader(s));
    ts.reset();
    ts.addAttribute(CharTermAttribute.class);
    PartOfSpeechAttribute pos = ts.addAttribute(PartOfSpeechAttribute.class);
    while (ts.incrementToken()) {
      // nothing
      pos.getPartOfSpeech(); // force a metadata decode
    }
    ts.end();
    ts.close();
  }
}