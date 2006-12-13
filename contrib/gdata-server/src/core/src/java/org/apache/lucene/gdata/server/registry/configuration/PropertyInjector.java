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

package org.apache.lucene.gdata.server.registry.configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.gdata.utils.ReflectionUtils;

/**
 * PropertyInjector is used to set member variables / properties of classes via
 * <i>setter</i> methods using the
 * {@link org.apache.lucene.gdata.server.registry.configuration.ComponentConfiguration}
 * class.
 * <p>
 * To populate a object with properties from a ComponentConfiguration instance
 * the class or a superclass of the object to populate has to provide at least
 * one setter method with a single parameter. The object to populate is set via
 * the {@link PropertyInjector#setTargetObject} method. The class of the object
 * will be analyzed for setter methods having a "set" prefix in their method
 * name. If one of the found setter methods is annotated with
 * {@link org.apache.lucene.gdata.server.registry.configuration.Requiered} this
 * property is interpreted as a mandatory property. Mandatory properties must be
 * available in the provided ComponentConfiguration, if not the injection will
 * fail.<br>
 * The
 * {@link org.apache.lucene.gdata.server.registry.configuration.ComponentConfiguration}
 * contains key / value pairs where the key must match the signature of the
 * setter method without the 'set' prefix and must begin with a lower case
 * character. <span>Key<code>bufferSize</code> does match a method signature
 * of <code>setBufferSize</code></span> The type of the parameter will be
 * reflected via the Reflection API and instantiated with the given value if
 * possible.
 * </p>
 * <p>
 * Setter methods without a <code>Required</code> annotation will be set if
 * the property is present in the ComponentConfiguration
 * </p>
 * <p>This class does not support overloaded setter methods.</p>
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.server.registry.configuration.Requiered
 * @see org.apache.lucene.gdata.server.registry.configuration.ComponentConfiguration
 */
public class PropertyInjector {
    private static final String SETTER_PREFIX = "set";

    private Class targetClass;

    private Object target;

    private Map<String, Method> requieredProperties = new HashMap<String, Method>();

    private Map<String, Method> optionalProperties = new HashMap<String, Method>();

    /**
     * Sets the object to be populated with the properties provided in the ComponentConfiguration.
     * @param o - the object to populate
     */
    public void setTargetObject(final Object o) {
        if (o == null)
            throw new IllegalArgumentException("TargetObject must not be null");
        this.target = o;
        this.targetClass = o.getClass();
        try {
            registerProperties(this.targetClass);
        } catch (Exception e) {
            throw new InjectionException("can access field -- "
                    + e.getMessage(), e);

        }
        if (this.requieredProperties.isEmpty()
                && this.optionalProperties.isEmpty())
            throw new InjectionException(
                    "Given type has no public setter methods -- "
                            + o.getClass().getName());

    }

    protected int getRequiredSize() {
        return this.requieredProperties.size();
    }

    protected int getOptionalSize() {
        return this.optionalProperties.size();
    }

    private void registerProperties(final Class clazz)
            throws SecurityException, NoSuchFieldException {
        if (clazz == null)
            return;
        Method[] methodes = clazz.getMethods();
        for (int i = 0; i < methodes.length; i++) {
            if (methodes[i].getName()
                    .startsWith(PropertyInjector.SETTER_PREFIX)) {
                String methodName = methodes[i].getName();
                String fieldName = getFieldName(methodName);
                if (methodes[i].getAnnotation(Requiered.class) != null)
                    this.requieredProperties.put(fieldName, methodes[i]);
                else
                    this.optionalProperties.put(fieldName, methodes[i]);

            }

        }
        registerProperties(clazz.getSuperclass());
    }

    private String getFieldName(final String setterMethodName) {
        // remove 'set' prefix --> first char as lowerCase
        String retVal = setterMethodName.substring(3);
        String firstLetter = retVal.substring(0, 1);
        retVal = retVal.replaceFirst(firstLetter, firstLetter.toLowerCase());
        return retVal;
    }

    /**
     * Injects the properties stored in the <code>ComponentConfiguration</code>
     * to the corresponding methods of the target object
     * @param bean - configuration bean containing all properties to set.
     * 
     */
    public void injectProperties(final ComponentConfiguration bean) {
        if (bean == null)
            throw new IllegalArgumentException("bean must not be null");
        if (this.target == null)
            throw new IllegalStateException("target is not set -- null");
        Set<Entry<String, Method>> requiered = this.requieredProperties
                .entrySet();
        // set required properties
        for (Entry<String, Method> entry : requiered) {
            if (!bean.contains(entry.getKey()))
                throw new InjectionException(
                        "Required property can not be set -- value not in configuration bean; Property: "
                                + entry.getKey()
                                + "for class "
                                + this.targetClass.getName());
            populate(bean, entry);

        }
        Set<Entry<String, Method>> optinal = this.optionalProperties.entrySet();
        // set optional properties
        for (Entry<String, Method> entry : optinal) {
            if (bean.contains(entry.getKey()))
                populate(bean, entry);
        }

    }

    private void populate(ComponentConfiguration bean,
            Entry<String, Method> entry) {
        String value = bean.get(entry.getKey());
        Method m = entry.getValue();
        Class<?>[] parameterTypes = m.getParameterTypes();
        if (parameterTypes.length > 1)
            throw new InjectionException("Setter has more than one parameter "
                    + m.getName() + " -- can not invoke method -- ");
        Object parameter = null;
        try {

            parameter = createObject(value, parameterTypes[0]);
        } catch (InjectionException e) {
            throw new InjectionException(
                    "parameter object creation failed for method "
                            + m.getName() + " in class: "
                            + this.targetClass.getName(), e);
        }
        // only setters with one parameter are supported
        Object[] parameters = { parameter };
        try {
            m.invoke(this.target, parameters);
        } catch (Exception e) {
            throw new InjectionException("Can not set value of type "
                    + value.getClass().getName()
                    + " -- can not invoke method -- " + e.getMessage(), e);

        }
    }

    private Object createObject(String s, Class<?> clazz) {

        try {
            // if class is requested use s as fully qualified class name
            if (clazz == Class.class)
                return Class.forName(s);
            // check for primitive type
            if (clazz.isPrimitive())
                clazz = ReflectionUtils.getPrimitiveWrapper(clazz);
            boolean defaultConst = false;
            boolean stringConst = false;
            Constructor[] constructors = clazz.getConstructors();
            if (constructors.length == 0)
                defaultConst = true;
            for (int i = 0; i < constructors.length; i++) {
                if (constructors[i].getParameterTypes().length == 0) {
                    defaultConst = true;
                    continue;
                }
                if (constructors[i].getParameterTypes().length == 1
                        && constructors[i].getParameterTypes()[0]
                                .equals(String.class))
                    stringConst = true;
            }
            /*
             * if there is a string constructor use the string as a parameter
             */
            if (stringConst) {
                Constructor constructor = clazz
                        .getConstructor(new Class[] { String.class });
                return constructor.newInstance(new Object[] { s });
            }
            /*
             * if no string const. but a default const. -- use the string as a
             * class name
             */
            if (defaultConst)
                return Class.forName(s).newInstance();
            throw new InjectionException(
                    "Parameter can not be created -- no default or String constructor found for class "
                            + clazz.getName());

        } catch (Exception e) {

            throw new InjectionException("can not create object for setter", e);
        }

    }

    /**
     * Sets all members to their default values and clears the internal used
     * {@link Map} instances
     * 
     * @see Map#clear()
     */
    public void clear() {
        this.target = null;
        this.targetClass = null;
        this.optionalProperties.clear();
        this.requieredProperties.clear();
    }

    

}
