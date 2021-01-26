package org.apache.lucene.backward_codecs.lucene60;

import org.apache.lucene.backward_codecs.lucene84.Lucene84RWCodec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BaseFieldInfoFormatTestCase;

public class TestLucene60FieldInfosFormat extends BaseFieldInfoFormatTestCase {
    @Override
    protected Codec getCodec() {
        return new Lucene84RWCodec();
    }
}
