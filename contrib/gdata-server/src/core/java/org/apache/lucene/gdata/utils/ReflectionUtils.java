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

package org.apache.lucene.gdata.utils;

import java.lang.reflect.Constructor;

/**
 * A collection of static helper methods solve common reflection problems
 * 
 * @author Simon Willnauer
 * 
 */
public class ReflectionUtils {

    /**
     * Check if the given type implements a given super type
     * @param typeToCheck - type supposed to implement an interface
     * @param superType - the interface to be implemented by the type to check
     * @return <code>true</code> if and only if the super type is above in the type hierarchy of the given type, otherwise <code>false</code>
     */
    public static boolean implementsType(Class typeToCheck, Class superType) {
        if(superType == null)
            return false;
        if(!superType.isInterface())
            return  false;
        if (typeToCheck == null)
            return false;
        if (typeToCheck.equals(Object.class))
            return false;
        if (typeToCheck.equals(superType))
            return true;
        Class[] interfaces = typeToCheck.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            if (implementsType(interfaces[i], superType))
                return true;
        }
        return implementsType(typeToCheck.getSuperclass(),superType);
        
    }
    /**
     * Check if the given type extends a given super type
     * @param typeToCheck - type supposed to extend an specific type
     * @param superType - the type to be extended by the type to check
     * @return <code>true</code> if and only if the super type is above in the type hierarchy of the given type, otherwise <code>false</code>
     */
    public static boolean extendsType(Class typeToCheck, Class superType) {
        if (typeToCheck == null)
            return false;
        if (typeToCheck.equals(Object.class))
            return false;
        if (typeToCheck.equals(superType))
            return true;
        
        return extendsType(typeToCheck.getSuperclass(),superType);
    }
    /**
     * This method combines the extendsType and implementsType and checks interfaces and classes
     * @param typeToCheck - type supposed to extend / implement an specific type
     * @param superType - the type to be extended / implemented by the type to check
     * @return <code>true</code> if and only if the super type is above in the type hierarchy of the given type, otherwise <code>false</code>
     */
    public static boolean isTypeOf(Class typeToCheck, Class superType){
        return extendsType(typeToCheck,superType)||implementsType(typeToCheck,superType);
    }
    
    /**
     * @param type - the type to check
     * @param parameter - the constructor parameter
     * @return <code>true</code> if and only if the type has a visible constructor with the desired parameters
     */
    public static boolean hasDesiredConstructor(Class type, Class[] parameter){
        try{
        return type.getConstructor(parameter) != null;
        
        }catch (Exception e) {
            return false;
        }
    }
  
 
/**
 * @param <T> the type of the class to instantiate 
 * @param clazz - class object of the type
 * @return a new instance of T

 */
@SuppressWarnings("unchecked")
public  static <T> T getDefaultInstance(Class<T> clazz) {
    if(clazz == null)
        throw new ReflectionException("class must not be null");
    
    try{
    Constructor constructor = clazz.getConstructor(new Class[]{});
    return (T) constructor.newInstance(new Object[]{});
    }catch (Exception e) {
        throw new ReflectionException("can not instantiate type of class "+clazz.getName(),e);
    }
}


/**
 * This method calls {@link Class#newInstance()} to get a new instance. Use with care!
 * @param clazz - the class to instantiate
 * @return <code>true</code> if an instance could be created, otherwise false;
 */
public static boolean canCreateInstance(Class clazz){
    if(clazz == null)
        return false;
    if(clazz.isPrimitive())
        clazz = getPrimitiveWrapper(clazz);
    try{
        Object o = clazz.newInstance();
        return o != null;
    }catch (Throwable e) {
        return false;
    }
}
/**
 * Returns the wrapper type for the given primitive type. Wrappers can be
 * easily instantiated via reflection and will be boxed by the VM
 * @param primitive - the primitive type 
 * @return - the corresponding wrapper type
 */
public static final Class getPrimitiveWrapper(Class primitive) {
    if(primitive == null )
        throw new ReflectionException("primitive must not be null");
    if(!primitive.isPrimitive())
        throw new ReflectionException("given class is not a primitive");
                
    if (primitive == Integer.TYPE)
        return Integer.class;
    if (primitive == Float.TYPE)
        return Float.class;
    if (primitive == Long.TYPE)
        return Long.class;
    if (primitive == Short.TYPE)
        return Short.class;
    if (primitive == Byte.TYPE)
        return Byte.class;
    if (primitive == Double.TYPE)
        return Double.class;
    if (primitive == Boolean.TYPE)
        return Boolean.class;

    return primitive;
}

/**
 * Exception wrapper for all thrown exception in the ReflectionUtils methods
 * @author Simon Willnauer
 *
 */
public static class ReflectionException extends RuntimeException{

    /**
     * 
     */
    private static final long serialVersionUID = -4855060602565614280L;

    /**
     * @param message -  the exception message
     * @param cause - the exception root cause
     */
    public ReflectionException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message - the exception message
     */
    public ReflectionException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }
    
}

}
