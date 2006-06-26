/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.gdata.server.registry;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * The GDataServerRegistry represents the registry component of the GData
 * Server. All provided services and server components will be registered here.
 * The Gdata Server serves RSS / ATOM feeds for defined services. Each service
 * provides <i>n</i> feeds of a defined subclass of
 * {@link com.google.gdata.data.BaseFeed}. Each feed contains <i>m</i> entries
 * of a defined subclass of {@link com.google.gdata.data.BaseEntry}. To
 * generate RSS / ATOM formates a class of the type
 * {@link com.google.gdata.data.ExtensionProfile} is also defined for a service.
 * <p>
 * The entry,feed and the ExtensionProfile classes are defined in the
 * gdata-config.xml and will be loaded when the server starts up.
 * </p>
 * <p>
 * The components defined in the gdata-config.xml will also be loaded and
 * instanciated at startup. If a component can not be loaded or an Exception
 * occures the server will not start up. To cause of the exception or error will
 * be logged to the standart server output.
 * </p>
 * <p>The GDataServerRegistry is a Singleton</p>
 * 
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataServerRegistry {
    private static GDataServerRegistry INSTANCE;

    private static final Log LOGGER = LogFactory
            .getLog(GDataServerRegistry.class);

    private final Map<String, ProvidedService> serviceTypeMap = new HashMap<String, ProvidedService>();

    private final Map<ComponentType, ComponentBean> componentMap = new HashMap<ComponentType, ComponentBean>(
            10);

    private GDataServerRegistry() {
        // private - singleton
    }

    /**
     * @return a Sinleton registry instance
     */
    public static synchronized GDataServerRegistry getRegistry() {
        if (INSTANCE == null)
            INSTANCE = new GDataServerRegistry();
        return INSTANCE;
    }

    /**
     * Registers a {@link ProvidedService}
     * 
     * @param configurator -
     *            the configurator to register in the registry
     */
    public void registerService(ProvidedService configurator) {
        if (configurator == null) {
            LOGGER.warn("Feedconfigurator is null -- skip registration");
            return;
        }
        this.serviceTypeMap.put(configurator.getName(), configurator);
    }

    /**
     * Looks up the {@link ProvidedServiceConfig} by the given service name.
     * 
     * @param service
     * @return - the {@link ProvidedServiceConfig} or <code>null</code> if the
     *         no configuration for this service has been registered
     */
    public ProvidedService getProvidedService(String service) {
        if (service == null)
            throw new IllegalArgumentException(
                    "Service is null - must not be null to get registered feedtype");
        return this.serviceTypeMap.get(service);
    }

    protected void flushRegistry() {
        this.serviceTypeMap.clear();
        this.componentMap.clear();
    }

    /**
     * @param service -
     *            the name of the service
     * @return - <code>true</code> if and only if the service is registered,
     *         otherwise <code>false</code>.
     */
    public boolean isServiceRegistered(String service) {
        return this.serviceTypeMap.containsKey(service);

    }

    /**
     * Destroys the registry and release all resources
     */
    public void destroy() {
        for (ComponentBean component : this.componentMap.values()) {
            component.getObject().destroy();
        }
        flushRegistry();

    }

    /**
     * This method is the main interface to the Component Lookup Service of the
     * registry. Every GDATA - Server component like STORAGE or the INDEXER
     * component will be accessible via this method. To get a Component from the
     * lookup service specify the expected Class as an argument and the
     * component type of the component to return. For a lookup of the
     * STORAGECONTORLER the code looks like:
     * <p>
     * <code> registryInstance.lookup(StorageController.class,ComponentType.STORAGECONTROLLER);</code>
     * </p>
     * 
     * @param <R>
     *            the type of the expected return value
     * @param clazz -
     *            Class object of the expected return value
     * @param compType -
     *            The component type
     * @return the registered component or <code>null</code> if the component
     *         can not looked up.
     */
    @SuppressWarnings("unchecked")
    public <R> R lookup(Class<R> clazz, ComponentType compType) {
        ComponentBean bean = this.componentMap.get(compType);
        if (bean == null)
            return null;
        if (bean.getSuperType().equals(clazz))
            return (R) bean.getObject();
        return null;
    }

    /**
     * @param <E> 
     * @param componentClass
     * @throws RegistryException
     */
    @SuppressWarnings("unchecked")
    public  <E extends ServerComponent> void  registerComponent(final Class<E> componentClass)
            throws RegistryException {
        
        if (componentClass == null)
            throw new IllegalArgumentException(
                    "component class must not be null");
  
        if(!checkImplementsServerComponent(componentClass))
            throw new RegistryException("can not register component. the given class does not implement ServerComponent interface -- "+componentClass.getName());
        try {

            Component annotation =  componentClass.getAnnotation(Component.class);
            if (annotation == null)
                throw new RegistryException(
                        "can not register component. the given class is not a component -- "
                                + componentClass.getName());
            ComponentType type = annotation.componentType();
            if (this.componentMap.containsKey(type))
                throw new RegistryException("component already registered -- "
                        + type.name());
            Class superType = type.getClass().getField(type.name())
                    .getAnnotation(SuperType.class).superType();
            if (!checkSuperType(componentClass, superType))
                throw new RegistryException("Considered Supertype <"
                        + superType.getName() + "> is not a super type of <"
                        + componentClass + ">");
            ServerComponent comp = componentClass.newInstance();
            comp.initialize();
            ComponentBean bean = new ComponentBean(comp, superType);
            
            this.componentMap.put(type, bean);

        } catch (Exception e) {
            throw new RegistryException("Can not register component -- "
                    + e.getMessage(), e);
        }

    }
    
    private static boolean checkImplementsServerComponent(Class type){
        if(type == null)
            return false;
        if(type.equals(Object.class))
            return false;
        if(type.equals(ServerComponent.class))
            return true;
        Class[] compInterfaces = type.getInterfaces();
        for (int i = 0; i < compInterfaces.length; i++) {
           if(checkImplementsServerComponent(compInterfaces[i]))
               return true;
        }
        return checkImplementsServerComponent(type.getSuperclass());
        
    }

    private static boolean checkSuperType(Class type, Class consideredSuperType) {

        if (type.equals(Object.class))
            return false;
        if (type.equals(consideredSuperType))
            return true;
        Class[] interfaces = type.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            if (interfaces[i].equals(consideredSuperType))
                return true;
        }
        return checkSuperType(type.getSuperclass(), consideredSuperType);
    }

    private class ComponentBean {
        private final Class superType;

        private final ServerComponent object;

        ComponentBean(final ServerComponent object, final Class superType) {
            this.superType = superType;
            this.object = object;
        }

        ServerComponent getObject() {
            return this.object;
        }

        Class getSuperType() {
            return this.superType;
        }

    }

}
