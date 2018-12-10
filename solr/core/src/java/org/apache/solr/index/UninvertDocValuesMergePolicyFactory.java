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

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
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
 */
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
        return new UninvertingFilterCodecReader(reader, uninversionMap, skipIntegrityCheck);
      }
      
    }
    
  }


}
