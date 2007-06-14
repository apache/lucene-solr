/**
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

package org.apache.solr.client.solrj.response;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.util.NamedList;


/**
 * This is an incomplete representation of the data returned from Luke
 * 
 * @author ryan
 * @version $Id$
 * @since solr 1.3
 */
public class LukeResponse extends SolrResponseBase
{
  public static class FieldInfo {
    String name;
    String type;
    String schema;
    int docs;
    int distinct;
    boolean cacheableFaceting;
    NamedList<Integer> topTerms;
    
    public FieldInfo( String n )
    {
      name = n;
    }
    
    @SuppressWarnings("unchecked")
    public void read( NamedList<Object> nl )
    {
      for( Map.Entry<String, Object> entry : nl ) {
        if( "type".equals( entry.getKey() ) ) {
          type = (String)entry.getValue();
        }
        else if( "schema".equals( entry.getKey() ) ) {
          schema = (String)entry.getValue();
        }
        else if( "docs".equals( entry.getKey() ) ) {
          docs = (Integer)entry.getValue();
        }
        else if( "distinct".equals( entry.getKey() ) ) {
          distinct = (Integer)entry.getValue();
        }
        else if( "cacheableFaceting".equals( entry.getKey() ) ) {
          cacheableFaceting = (Boolean)entry.getValue();
        }
        else if( "topTerms".equals( entry.getKey() ) ) {
          topTerms = (NamedList<Integer>)entry.getValue();
        }
      }
    }

    public boolean isCacheableFaceting() {
      return cacheableFaceting;
    }

    public int getDistinct() {
      return distinct;
    }

    public int getDocs() {
      return docs;
    }

    public String getName() {
      return name;
    }

    public String getSchema() {
      return schema;
    }

    public NamedList<Integer> getTopTerms() {
      return topTerms;
    }

    public String getType() {
      return type;
    }
  };

  private NamedList<Object> indexInfo;
  private Map<String,FieldInfo> fieldInfo;
  
  @SuppressWarnings("unchecked")
  public LukeResponse(NamedList<Object> res) {
    super(res);
    
    // Parse indexinfo
    indexInfo = (NamedList<Object>)res.get( "index" );
    
    NamedList<Object> flds = (NamedList<Object>)res.get( "fields" );
    if( flds != null ) {
      fieldInfo = new HashMap<String,FieldInfo>( );
      for( Map.Entry<String, Object> field : flds ) {
        FieldInfo f = new FieldInfo( field.getKey() );
        f.read( (NamedList<Object>)field.getValue() );
        fieldInfo.put( field.getKey(), f );
      }
    }
  }

  //----------------------------------------------------------------
  //----------------------------------------------------------------
  
  public String getIndexDirectory()
  {
    if( indexInfo == null ) return null;
    return (String)indexInfo.get( "directory" );
  }

  public Integer getNumDocs()
  {
    if( indexInfo == null ) return null;
    return (Integer)indexInfo.get( "numDocs" );
  }

  public Integer getMaxDoc()
  {
    if( indexInfo == null ) return null;
    return (Integer)indexInfo.get( "maxDoc" );
  }

  public Integer getNumTerms()
  {
    if( indexInfo == null ) return null;
    return (Integer)indexInfo.get( "numTerms" );
  }

  public Map<String,FieldInfo> getFieldInfo() {
    return fieldInfo;
  }

  public FieldInfo getFieldInfo( String f ) {
    return fieldInfo.get( f );
  }

  //----------------------------------------------------------------
  //----------------------------------------------------------------
}
