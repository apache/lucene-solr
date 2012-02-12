import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.kuromoji.KuromojiTokenizer2;
import org.apache.lucene.analysis.kuromoji.KuromojiTokenizer;
import org.apache.lucene.analysis.kuromoji.Segmenter.Mode;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;

// javac -cp ../build/kuromoji/classes/java:../../../lucene/build/core/classes/java:../../analysis/build/common/lucene-analyzers-common-4.0-SNAPSHOT.jar Perf.java

// java -cp .:../build/kuromoji/classes/java:../../../lucene/build/core/classes/java:../../analysis/build/common/lucene-analyzers-common-4.0-SNAPSHOT.jar Perf
public class Perf {

  private final static Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new KuromojiTokenizer2(reader, null, true, Mode.SEARCH);
      //Tokenizer tokenizer = new KuromojiTokenizer(reader);
      return new TokenStreamComponents(tokenizer, tokenizer);
    }
  };
  
  final static String s0 = "本来は、貧困層の女性や子供に医療保護を提供するために創設された制度である、" +
    "アメリカ低所得者医療援助制度が、今日では、その予算の約３分の１を老人に費やしている。";
  final static String s = s0 + s0 + s0 + s0 + s0 + s0;

  public static void main(String[] args) throws Exception {
    for(int iter=0;iter<10;iter++) {
      final long t0 = System.currentTimeMillis();
      long count = 0;
      final int ITERS = 3000;
      for(int i=0;i<ITERS;i++) {
        final TokenStream ts = analyzer.tokenStream("foo", new StringReader(s));
        while(ts.incrementToken()) {
          count++;
        }
      }
      final long t1 = System.currentTimeMillis();
      System.out.println((t1-t0) + " msec; " + (s0.length()*ITERS/((double) t1-t0)) + " chars/msec (" + count + " tokens)");
    }
  }
}
