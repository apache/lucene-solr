package org.apache.solr.index;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.uninverting.UninvertingReader;

/**
 * Delegates to an Uninverting for fields with docvalues
 *
 * This is going to blow up FieldCache, look into an alternative implementation that uninverts without
 * fieldcache
 */
public class UninvertingFilterCodecReader extends FilterCodecReader {

  private final LeafReader uninvertingReader;
  private final DocValuesProducer docValuesProducer;

  public UninvertingFilterCodecReader(CodecReader in, Map<String, UninvertingReader.Type> uninversionMap,
                                      boolean skipIntegrityCheck) {
    super(in);

    this.uninvertingReader = UninvertingReader.wrap(in, uninversionMap::get);
    this.docValuesProducer = new DocValuesProducer() {

      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return uninvertingReader.getNumericDocValues(field.name);
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return uninvertingReader.getBinaryDocValues(field.name);
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedDocValues(field.name);
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedNumericDocValues(field.name);
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedSetDocValues(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        if (!skipIntegrityCheck) {
          uninvertingReader.checkIntegrity();
        }
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }

  @Override
  protected void doClose() throws IOException {
    docValuesProducer.close();
    uninvertingReader.close();
    super.doClose();
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    return docValuesProducer;
  }

  @Override
  public FieldInfos getFieldInfos() {
    return uninvertingReader.getFieldInfos();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

}
