package org.apache.lucene.analysis.cn;

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

/**
 * Title: ChineseAnalyzer
 * Description:
 *   Subclass of org.apache.lucene.analysis.Analyzer
 *   build from a ChineseTokenizer, filtered with ChineseFilter.
 * Copyright:   Copyright (c) 2001
 * Company:
 * @author Yiyi Sun
 * @version 1.0
 *
 */

public class ChineseAnalyzer extends Analyzer {

    public ChineseAnalyzer() {
    }

    /**
    * Creates a TokenStream which tokenizes all the text in the provided Reader.
    *
    * @return  A TokenStream build from a ChineseTokenizer filtered with ChineseFilter.
    */
    public final TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream result = new ChineseTokenizer(reader);
        result = new ChineseFilter(result);
        return result;
    }
}