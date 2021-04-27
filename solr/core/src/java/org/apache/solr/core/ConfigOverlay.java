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
package org.apache.solr.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * This class encapsulates the config overlay json file. It is immutable
 * and any edit operations performed on tbhis gives a new copy of the object
 * with the changed value
 */
public class ConfigOverlay implements MapSerializable {
  private final int znodeVersion;
  private final Map<String, Object> data;
  private Map<String, Object> props;
  private Map<String, Object> userProps;

  @SuppressWarnings({"unchecked"})
  public ConfigOverlay(Map<String, Object> jsonObj, int znodeVersion) {
    if (jsonObj == null) jsonObj = Collections.EMPTY_MAP;
    this.znodeVersion = znodeVersion;
    data = Collections.unmodifiableMap(jsonObj);
    props = (Map<String, Object>) data.get("props");
    if (props == null) props = Collections.EMPTY_MAP;
    userProps = (Map<String, Object>) data.get("userProps");
    if (userProps == null) userProps = Collections.EMPTY_MAP;
  }

  public Object getXPathProperty(String xpath) {
    return getXPathProperty(xpath, true);
  }

  public Object getXPathProperty(String xpath, boolean onlyPrimitive) {
    List<String> hierarchy = checkEditable(xpath, true, false);
    if (hierarchy == null) return null;
    return Utils.getObjectByPath(props, onlyPrimitive, hierarchy);
  }

  public Object getXPathProperty(List<String> path) {
    List<String> hierarchy = new ArrayList<>();
    if(isEditable(true, hierarchy, path) == null) return null;
    return Utils.getObjectByPath(props, true, hierarchy);
  }

  @SuppressWarnings({"unchecked"})
  public ConfigOverlay setUserProperty(String key, Object val) {
    @SuppressWarnings({"rawtypes"})
    Map copy = new LinkedHashMap(userProps);
    copy.put(key, val);
    Map<String, Object> jsonObj = new LinkedHashMap<>(this.data);
    jsonObj.put("userProps", copy);
    return new ConfigOverlay(jsonObj, znodeVersion);
  }

  public ConfigOverlay unsetUserProperty(String key) {
    if (!userProps.containsKey(key)) return this;
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map copy = new LinkedHashMap(userProps);
    copy.remove(key);
    Map<String, Object> jsonObj = new LinkedHashMap<>(this.data);
    jsonObj.put("userProps", copy);
    return new ConfigOverlay(jsonObj, znodeVersion);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public ConfigOverlay setProperty(String name, Object val) {
    List<String> hierarchy = checkEditable(name, false, true);
    Map deepCopy = (Map) Utils.fromJSON(Utils.toJSON(props));
    Map obj = deepCopy;
    for (int i = 0; i < hierarchy.size(); i++) {
      String s = hierarchy.get(i);
      if (i < hierarchy.size() - 1) {
        if (obj.get(s) == null || (!(obj.get(s) instanceof Map))) {
          obj.put(s, new LinkedHashMap<>());
        }
        obj = (Map) obj.get(s);
      } else {
        obj.put(s, val);
      }
    }

    Map<String, Object> jsonObj = new LinkedHashMap<>(this.data);
    jsonObj.put("props", deepCopy);

    return new ConfigOverlay(jsonObj, znodeVersion);
  }


  public static final String NOT_EDITABLE = "''{0}'' is not an editable property";

  private List<String> checkEditable(String propName, boolean isXPath, boolean failOnError) {
    LinkedList<String> hierarchy = new LinkedList<>();
    if (!isEditableProp(propName, isXPath, hierarchy)) {
      if (failOnError)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, StrUtils.formatString(NOT_EDITABLE, propName));
      else return null;
    }
    return hierarchy;

  }

  @SuppressWarnings({"rawtypes"})
  public ConfigOverlay unsetProperty(String name) {
    List<String> hierarchy = checkEditable(name, false, true);
    Map deepCopy = (Map) Utils.fromJSON(Utils.toJSON(props));
    Map obj = deepCopy;
    for (int i = 0; i < hierarchy.size(); i++) {
      String s = hierarchy.get(i);
      if (i < hierarchy.size() - 1) {
        if (obj.get(s) == null || (!(obj.get(s) instanceof Map))) {
          return this;
        }
        obj = (Map) obj.get(s);
      } else {
        obj.remove(s);
      }
    }

    Map<String, Object> jsonObj = new LinkedHashMap<>(this.data);
    jsonObj.put("props", deepCopy);

    return new ConfigOverlay(jsonObj, znodeVersion);
  }

  public byte[] toByteArray() {
    return Utils.toJSON(data);
  }


  public int getZnodeVersion() {
    return znodeVersion;
  }

  @Override
  public String toString() {
    return toJSONString(data);
  }


  public static final String RESOURCE_NAME = "configoverlay.json";

  /*private static final Long STR_ATTR = 0L;
  private static final Long STR_NODE = 1L;
  private static final Long BOOL_ATTR = 10L;
  private static final Long BOOL_NODE = 11L;
  private static final Long INT_ATTR = 20L;
  private static final Long INT_NODE = 21L;
  private static final Long FLOAT_ATTR = 30L;
  private static final Long FLOAT_NODE = 31L;*/
  //The path maps to the xml xpath and value of 1 means it is a tag with a string value and value
  // of 0 means it is an attribute with string value

  @SuppressWarnings({"rawtypes"})
  private static Map editable_prop_map = (Map) Utils.fromJSONResource("EditableSolrConfigAttributes.json");

  public static boolean isEditableProp(String path, boolean isXpath, List<String> hierarchy) {
    return !(checkEditable(path, isXpath, hierarchy) == null);
  }


  @SuppressWarnings({"rawtypes"})
  public static Class checkEditable(String path, boolean isXpath, List<String> hierarchy) {
    return isEditable(isXpath, hierarchy, StrUtils.splitSmart(path, isXpath ? '/' : '.'));
  }

  @SuppressWarnings("rawtypes")
  private static Class isEditable(boolean isXpath, List<String> hierarchy, List<String> parts) {
    Object obj = editable_prop_map;
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);
      boolean isAttr = false;
      try {
        isAttr = isXpath && part.startsWith("@");
      } catch (RuntimeException e) {
        throw e;
      }
      if (isAttr) {
        part = part.substring(1);
      }
      if (hierarchy != null) hierarchy.add(part);
      if (obj == null) return null;
      if (i == parts.size() - 1) {
        if (obj instanceof Map) {
          Map map = (Map) obj;
          Object o = map.get(part);
          return checkType(o, isXpath, isAttr);
        }
        return null;
      }
      obj = ((Map) obj).get(part);
    }
    return null;
  }

  @SuppressWarnings({"rawtypes"})
  static Class[] types = new Class[]{String.class, Boolean.class, Integer.class, Float.class};

  @SuppressWarnings({"rawtypes"})
  private static Class checkType(Object o, boolean isXpath, boolean isAttr) {
    if (o instanceof Long) {
      Long aLong = (Long) o;
      int ten = aLong.intValue() / 10;
      int one = aLong.intValue() % 10;
      if (isXpath && isAttr && one != 0) return null;
      return types[ten];
    } else {
      return null;
    }
  }

  @SuppressWarnings({"unchecked"})
  public Map<String, String> getEditableSubProperties(String xpath) {
    Object o = Utils.getObjectByPath(props, false, StrUtils.splitSmart(xpath, '/'));
    if (o instanceof Map) {
      return (Map<String,String>) o;
    } else {
      return null;
    }
  }

  public Map<String, Object> getUserProps() {
    return userProps;
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    map.put(ZNODEVER, znodeVersion);
    map.putAll(data);
    return map;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Map<String, Map> getNamedPlugins(String typ) {
    Map<String, Map> reqHandlers = (Map<String, Map>) data.get(typ);
    if (reqHandlers == null) return Collections.EMPTY_MAP;
    return Collections.unmodifiableMap(reqHandlers);
  }

  boolean hasKey(String key) {
    return props.containsKey(key);
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public ConfigOverlay addNamedPlugin(Map<String, Object> info, String typ) {
    Map dataCopy = Utils.getDeepCopy(data, 4);
    Map existing = (Map) dataCopy.get(typ);
    if (existing == null) dataCopy.put(typ, existing = new LinkedHashMap());
    existing.put(info.get(CoreAdminParams.NAME), info);
    return new ConfigOverlay(dataCopy, this.znodeVersion);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public ConfigOverlay deleteNamedPlugin(String name, String typ) {
    Map dataCopy = Utils.getDeepCopy(data, 4);
    Map reqHandler = (Map) dataCopy.get(typ);
    if (reqHandler == null) return this;
    reqHandler.remove(name);
    return new ConfigOverlay(dataCopy, this.znodeVersion);

  }

  public static final String ZNODEVER = "znodeVersion";
  public static final String NAME = "overlay";

}
