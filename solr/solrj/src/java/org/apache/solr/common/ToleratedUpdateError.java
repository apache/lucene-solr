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
package org.apache.solr.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.SimpleOrderedMap;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * Models the basic information related to a single "tolerated" error that occurred during updates.  
 * This class is only useful when the <code>TolerantUpdateProcessorFactory</code> is used in an update 
 * processor chain
 */
public final class ToleratedUpdateError {
    
  private final static String META_PRE =  ToleratedUpdateError.class.getName() + "--";
  private final static int META_PRE_LEN = META_PRE.length();

  /**
   * Given a 'maxErrors' value such that<code>-1 &lt;= maxErrors &lt;= {@link Integer#MAX_VALUE}</code> 
   * this method returns the original input unless it is <code>-1</code> in which case the effective value of
   * {@link Integer#MAX_VALUE}  is returned.
   * Input of <code>maxErrors &lt; -1</code> will trip an assertion and otherwise have undefined behavior.
   * @see #getUserFriendlyMaxErrors
   */
  public static int getEffectiveMaxErrors(int maxErrors) {
    assert -1 <= maxErrors;
    return -1 == maxErrors ? Integer.MAX_VALUE : maxErrors;
  }
  
  /**
   * Given a 'maxErrors' value such that<code>-1 &lt;= maxErrors &lt;= {@link Integer#MAX_VALUE}</code> 
   * this method returns the original input unless it is {@link Integer#MAX_VALUE} in which case 
   * <code>-1</code> is returned for user convinience.
   * Input of <code>maxErrors &lt; -1</code> will trip an assertion and otherwise have undefined behavior.
   * @see #getEffectiveMaxErrors
   */
  public static int getUserFriendlyMaxErrors(int maxErrors) {
    assert -1 <= maxErrors;
    return Integer.MAX_VALUE == maxErrors ? -1 : maxErrors;
  }
  
  /** 
   * returns a list of maps of simple objects suitable for putting in a SolrQueryResponse header 
   * @see #getSimpleMap
   * @see #parseMap
   */
  public static List<SimpleOrderedMap<String>> formatForResponseHeader(List<ToleratedUpdateError> errs) {
    List<SimpleOrderedMap<String>> result = new ArrayList<>(errs.size());
    for (ToleratedUpdateError e : errs) {
      result.add(e.getSimpleMap());
    }
    return result;
  }
  
  /** 
   * returns a ToleratedUpdateError instance from the data in this Map 
   * @see #getSimpleMap
   */
  public static ToleratedUpdateError parseMap(SimpleOrderedMap<String> data) {
    final String id = data.get(ID);
    final String message = data.get("message");
    final String t = data.get("type");
    if (null == t || null == id || null == message) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Map does not represent a ToleratedUpdateError, must contain 'type', 'id', and 'message'");
    }
    try {
      return new ToleratedUpdateError(CmdType.valueOf(t), id, message);
    } catch (IllegalArgumentException iae) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid type for ToleratedUpdateError: " + t, iae);
    }
  }
  
  /** 
   * returns a ToleratedUpdateError instance if this metadataKey is one we care about, else null 
   * @see #getMetadataKey
   * @see #getMetadataValue
   */
  public static ToleratedUpdateError parseMetadataIfToleratedUpdateError(String metadataKey,
                                                                         String metadataVal) {
    if (! metadataKey.startsWith(META_PRE)) {
      return null; // not a key we care about
    }
    final int typeEnd = metadataKey.indexOf(':', META_PRE_LEN);
    if (typeEnd < 0) {
      return null; // has our prefix, but not our format -- must not be a key we (actually) care about
    }
    return new ToleratedUpdateError(CmdType.valueOf(metadataKey.substring(META_PRE_LEN, typeEnd)),
                                    metadataKey.substring(typeEnd+1), metadataVal);
  }

  private final CmdType type;
  private final String id; 
  private final String message;
  
  public ToleratedUpdateError(CmdType type, String id, String message) {
    assert null != type;
    this.type = type;
    
    assert null != id;
    this.id = id;
    
    assert null != message;
    this.message = message;
  }

  public CmdType getType() {
    return type;
  }
  public String getId() {
    return id;
  }
  public String getMessage() {
    return message;
  }
  
  /**
   * returns a string suitable for use as a key in {@link SolrException#setMetadata}
   *
   * @see #parseMetadataIfToleratedUpdateError
   */
  public String getMetadataKey() {
    return META_PRE + type + ":" + id;
  }
  
  /**
   * returns a string suitable for use as a value in {@link SolrException#setMetadata}
   *
   * @see #parseMetadataIfToleratedUpdateError
   */
  public String getMetadataValue() {
    return message.toString();
  }
  
  /** 
   * returns a map of simple objects suitable for putting in a SolrQueryResponse header 
   * @see #formatForResponseHeader
   * @see #parseMap
   */
  public SimpleOrderedMap<String> getSimpleMap() {
    SimpleOrderedMap<String> entry = new SimpleOrderedMap<String>();
    entry.add("type", type.toString());
    entry.add(ID, id);
    entry.add("message", message);
    return entry;
  }
  
  public String toString() {
    return getMetadataKey() + "=>" + getMetadataValue();
  }
  
  public int hashCode() {
    int h = this.getClass().hashCode();
    h = h * 31 + type.hashCode();
    h = h * 31 + id.hashCode();
    h = h * 31 + message.hashCode();
    return h;
  }
  
  public boolean equals(Object o) {
    if (o instanceof ToleratedUpdateError) {
      ToleratedUpdateError that = (ToleratedUpdateError)o;
      return that.type.equals(this.type)
        && that.id.equals(this.id)
        && that.message.equals(this.message);
    }
    return false;
  }
  
  /**
   * Helper class for dealing with SolrException metadata (String) keys 
   */
  public static enum CmdType {
    ADD, DELID, DELQ; 

    // if we add support for things like commit, parsing/toString/hashCode logic
    // needs to be smarter to account for 'id' being null ... "usesId" should be a prop of enum instances
  }
}

  
