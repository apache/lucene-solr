/*
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
package org.apache.solr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.uninverting.UninvertingReader;

/**
 * A merge policy that can detect schema changes and  write docvalues into merging segments when a field has docvalues enabled
 * Using UninvertingReader.
 * 
 * This merge policy will delegate to the wrapped merge policy for selecting merge segments
 * 
 * @deprecated This class will be removed in Solr 9 due to changes in Lucene 9.
 */
@Deprecated
public class UninvertDocValuesMergePolicyFactory extends WrapperMergePolicyFactory {
  
  final private boolean skipIntegrityCheck;

  /**
   * Whether or not the wrapped docValues producer should check consistency 
   */
  public boolean getSkipIntegrityCheck() {
    return skipIntegrityCheck;
  }

  public UninvertDocValuesMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    final Boolean sic = (Boolean)args.remove("skipIntegrityCheck");
    if (sic != null) {
      this.skipIntegrityCheck = sic.booleanValue();
    } else {
      this.skipIntegrityCheck = false;
    }
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were "+args+" but "+getClass().getSimpleName()+" takes no arguments.");
    }
  }

  @Override
  protected MergePolicy getMergePolicyInstance(MergePolicy wrappedMP) {
    return new OneMergeWrappingMergePolicy(wrappedMP, (merge) -> new UninvertDocValuesOneMerge(merge.segments));
  }
  
  private UninvertingReader.Type getUninversionType(FieldInfo fi) {
    SchemaField sf = schema.getFieldOrNull(fi.name);
    
    if (null != sf &&
        sf.hasDocValues() &&
        fi.getDocValuesType() == DocValuesType.NONE &&
        fi.getIndexOptions() != IndexOptions.NONE) {
      return sf.getType().getUninversionType(sf);
    } else {
      return null;
    }
  }
    
  private class UninvertDocValuesOneMerge extends MergePolicy.OneMerge {

    public UninvertDocValuesOneMerge(List<SegmentCommitInfo> segments) {
      super(segments);
    }
    
    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
      // Wrap the reader with an uninverting reader if any of the fields have no docvalues but the 
      // Schema says there should be
      
      
      Map<String,UninvertingReader.Type> uninversionMap = null;
      
      for(FieldInfo fi: reader.getFieldInfos()) {
        final UninvertingReader.Type type = getUninversionType(fi);
        if (type != null) {
          if (uninversionMap == null) {
            uninversionMap = new HashMap<>();
          }
          uninversionMap.put(fi.name, type);
        }
        
      }
      
      if(uninversionMap == null) {
        return reader; // Default to normal reader if nothing to uninvert
      } else {
        return new UninvertingFilterCodecReader(reader, uninversionMap);
      }
      
    }
    
  }
  
  
  /**
   * Delegates to an Uninverting for fields with docvalues
   * 
   * This is going to blow up FieldCache, look into an alternative implementation that uninverts without
   * fieldcache
   */
  private class UninvertingFilterCodecReader extends FilterCodecReader {

    private final LeafReader uninvertingReader;
    private final DocValuesProducer docValuesProducer;

    public UninvertingFilterCodecReader(CodecReader in, Map<String,UninvertingReader.Type> uninversionMap) {
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

}
