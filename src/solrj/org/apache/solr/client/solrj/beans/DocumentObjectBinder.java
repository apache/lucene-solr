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
package org.apache.solr.client.solrj.beans;

import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class to map objects to and from solr documents.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class DocumentObjectBinder {
  private final Map<Class, List<DocField>> infocache = new ConcurrentHashMap<Class, List<DocField>>();

  public DocumentObjectBinder() {
  }

  public <T> List<T> getBeans(Class<T> clazz, SolrDocumentList solrDocList) {
    List<DocField> fields = getDocFields( clazz );
    List<T> result = new ArrayList<T>(solrDocList.size());

    for(int j=0;j<solrDocList.size();j++) {
      SolrDocument sdoc = solrDocList.get(j);

      T obj = null;
      try {
        obj = clazz.newInstance();
        result.add(obj);
      } catch (Exception e) {
        throw new RuntimeException("Could not instantiate object of " + clazz,e);
      }
      for (int i = 0; i < fields.size(); i++) {
        DocField docField = fields.get(i);
        docField.inject(obj, sdoc);
      }
    }
    return result;
  }
  
  public SolrInputDocument toSolrInputDocument( Object obj )
  {
    List<DocField> fields = getDocFields( obj.getClass() );
    if( fields.isEmpty() ) {
      throw new RuntimeException( "class: "+obj.getClass()+" does not define any fields." );
    }
    
    SolrInputDocument doc = new SolrInputDocument();
    for( DocField field : fields ) {
      doc.setField( field.name, field.get( obj ), 1.0f );
    }
    return doc;
  }
  
  private List<DocField> getDocFields( Class clazz )
  {
    List<DocField> fields = infocache.get(clazz);
    if (fields == null) {
      synchronized(infocache) {
        infocache.put(clazz, fields = collectInfo(clazz));
      }
    }
    return fields;
  }

  private List<DocField> collectInfo(Class clazz) {
    List<DocField> fields = new ArrayList<DocField>();
    Class superClazz = clazz;
    ArrayList<AccessibleObject> members = new ArrayList<AccessibleObject>();
    while (superClazz != null && superClazz != Object.class) {
      members.addAll(Arrays.asList(superClazz.getDeclaredFields()));
      members.addAll(Arrays.asList(superClazz.getDeclaredMethods()));
      superClazz = superClazz.getSuperclass();
    }
    for (AccessibleObject member : members) {
      if (member.isAnnotationPresent(Field.class)) {
        member.setAccessible(true);
        fields.add(new DocField(member));
      }
    }
    return fields;
  }

  private static class DocField {
    private String name;
    private java.lang.reflect.Field field;
    private Method setter;
    private Method getter;
    private Class type;
    private boolean isArray = false, isList=false;

    public DocField(AccessibleObject member) {
      if (member instanceof java.lang.reflect.Field) {
        field = (java.lang.reflect.Field) member;
      } else {
        setter = (Method) member;
      }
      Field annotation = member.getAnnotation(Field.class);
      storeName(annotation);
      storeType();
      
      // Look for a matching getter
      if( setter != null ) {
        String gname = setter.getName();
        if( gname.startsWith("set") ) {
          gname = "get" + gname.substring(3);
          try {
            getter = setter.getDeclaringClass().getMethod( gname, (Class[])null );
          }
          catch( Exception ex ) {
            // no getter -- don't worry about it...
            if( type == Boolean.class ) {
              gname = "is" + setter.getName().substring( 3 );
              try {
                getter = setter.getDeclaringClass().getMethod( gname, (Class[])null );
              }
              catch( Exception ex2 ) {
                // no getter -- don't worry about it...
              }
            }
          }
        }
      }
    }

    private void storeName(Field annotation) {
      if (annotation.value().equals(Field.DEFAULT)) {
        if (field != null) {
          name = field.getName();
        } else {
          String setterName = setter.getName();
          if (setterName.startsWith("set") && setterName.length() > 3) {
            name = setterName.substring(3, 4).toLowerCase() + setterName.substring(4);
          } else {
            name = setter.getName();
          }
        }
      } else {
        name = annotation.value();
      }
    }

    private void storeType() {
      if (field != null) {
        type = field.getType();
      } else {
        Class[] params = setter.getParameterTypes();
        if (params.length != 1)
          throw new RuntimeException("Invalid setter method. Must have one and only one parameter");
        type = params[0];
      }
      if(type == Collection.class || type == List.class || type == ArrayList.class) {
        type = Object.class;
        isList = true;
        /*ParameterizedType parameterizedType = null;
        if(field !=null){
          if( field.getGenericType() instanceof ParameterizedType){
            parameterizedType = (ParameterizedType) field.getGenericType();
            Type[] types = parameterizedType.getActualTypeArguments();
            if (types != null && types.length > 0) type = (Class) types[0];
          }
        }*/
      } else if (type.isArray()) {
        isArray = true;
        type = type.getComponentType();
      }
    }

    public <T> void inject(T obj, SolrDocument sdoc) {
      Object val = sdoc.getFieldValue(name);
      if(val == null) return;
      if (isArray) {
        if (val instanceof List) {
          List collection = (List) val;
          set(obj, collection.toArray((Object[]) Array.newInstance(type,collection.size())));
        } else {
          Object[] arr = (Object[]) Array.newInstance(type, 1);
          arr[0] = val;
          set(obj, arr);
        }
      } else if (isList) {
        if (val instanceof List) {
          set(obj, val);
        } else {
          ArrayList l = new ArrayList();
          l.add(val);
          set(obj, l);
        }
      } else {
        if (val instanceof List) {
          List l = (List) val;
          if(l.size()>0) 
            set(obj, l.get(0));
        } 
        else {
          set(obj,val) ;
        }
      }
    }
    
    private void set(Object obj, Object v) {
      try {
        if (field != null) {
          field.set(obj, v);
        } else if (setter != null) {
          setter.invoke(obj, v);
        }
      } 
      catch (Exception e) {
        throw new RuntimeException("Exception while setting value : "+v+" on " + (field != null ? field : setter), e);
      }
    }
    
    public Object get( final Object obj )
    {
      if (field != null) {
        try {
          return field.get(obj);
        } 
        catch (Exception e) {        
          throw new RuntimeException("Exception while getting value: " + field, e);
        }
      }
      else if (getter == null) {
        throw new RuntimeException( "Missing getter for field: "+name+" -- You can only call the 'get' for fields that have a field of 'get' method" );
      }
      
      try {
        return getter.invoke( obj, (Object[])null );
      } 
      catch (Exception e) {        
        throw new RuntimeException("Exception while getting value: " + getter, e);
      }
    }
  }
}
