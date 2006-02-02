package org.apache.solr.search;

import org.apache.lucene.search.Filter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermDocs;

import java.util.BitSet;
import java.io.IOException;

/**
 * @author yonik
 * @version $Id: PrefixFilter.java,v 1.1 2005/06/10 05:47:32 yonik Exp $
 */
public class PrefixFilter extends Filter {
  protected final Term prefix;

  PrefixFilter(Term prefix) {
    this.prefix = prefix;
  }

  Term getPrefix() { return prefix; }

  public BitSet bits(IndexReader reader) throws IOException {
    final BitSet bitSet = new BitSet(reader.maxDoc());
    new PrefixGenerator(prefix) {
      public void handleDoc(int doc) {
        bitSet.set(doc);
      }
    }.generate(reader);
    return bitSet;
  }
}

// keep this protected until I decide if it's a good way
// to separate id generation from collection (or should
// I just reuse hitcollector???)
interface IdGenerator {
  public void generate(IndexReader reader) throws IOException;
  public void handleDoc(int doc);
}


abstract class PrefixGenerator implements IdGenerator {
  protected final Term prefix;

  PrefixGenerator(Term prefix) {
    this.prefix = prefix;
  }

  public void generate(IndexReader reader) throws IOException {
    TermEnum enumerator = reader.terms(prefix);
    TermDocs termDocs = reader.termDocs();

    try {

      String prefixText = prefix.text();
      String prefixField = prefix.field();
      do {
        Term term = enumerator.term();
        if (term != null &&
            term.text().startsWith(prefixText) &&
            term.field() == prefixField)
        {
          termDocs.seek(term);
          while (termDocs.next()) {
            handleDoc(termDocs.doc());
          }
        } else {
          break;
        }
      } while (enumerator.next());
    } finally {
      termDocs.close();
      enumerator.close();
    }
  }
}
