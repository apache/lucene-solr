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
package org.apache.lucene.gdata.server.registry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.server.registry.configuration.ComponentConfiguration;
import org.apache.lucene.gdata.server.registry.configuration.PropertyInjector;
import org.apache.lucene.gdata.utils.ReflectionUtils;

/**
 * 
 * The GDataServerRegistry represents the registry component of the GData
 * Server. All provided services and server components will be registered here.
 * The GData Server serves RSS / ATOM feeds for defined services. Each service
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
 * instantiated at startup. If a component can not be loaded or an Exception
 * occurs the server will not start up. To cause of the exception or error will
 * be logged to the standard server output.
 * </p>
 * <p>
 * The GDataServerRegistry is a Singleton
 * </p>
 * 
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataServerRegistry extends EntryEventMediator{
    private static GDataServerRegistry INSTANCE;

    private static final Log LOG = LogFactory
            .getLog(GDataServerRegistry.class);

    private ScopeVisitable requestVisitable;

    private ScopeVisitable sessionVisitable;

    // not available yet
    private ScopeVisitable contextVisitable;

    private List<ScopeVisitor> visitorBuffer = new ArrayList<ScopeVisitor>(5);

    private final Map<String, ProvidedService> serviceTypeMap = new HashMap<String, ProvidedService>();

    private final Map<ComponentType, ComponentBean> componentMap = new HashMap<ComponentType, ComponentBean>(
            ComponentType.values().length);

    private GDataServerRegistry() {
        // private - singleton
    }

    /**
     * @return a Singleton registry instance
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
            LOG.warn("Feed configurator is null -- skip registration");
            return;
        }
        this.serviceTypeMap.put(configurator.getName(), configurator);
    }

    /**
     * @param visitor -
     *            the visitor to register
     * @throws RegistryException
     */
    public synchronized void registerScopeVisitor(final ScopeVisitor visitor)
            throws RegistryException {
        if (visitor == null)
            throw new IllegalArgumentException("visitor must not be null");
        Scope scope = visitor.getClass().getAnnotation(Scope.class);
        if (scope == null)
            throw new RegistryException("Visitor has not Scope");
        if (LOG.isInfoEnabled())
            LOG.info("Register scope visitor -- " + visitor.getClass());
        if (scope.scope().equals(Scope.ScopeType.REQUEST)
                && this.requestVisitable != null)
            this.requestVisitable.accept(visitor);
        else if (scope.scope() == Scope.ScopeType.SESSION
                && this.sessionVisitable != null)
            this.sessionVisitable.accept(visitor);
        else if (scope.scope() == Scope.ScopeType.CONTEXT
                && this.contextVisitable != null)
            this.sessionVisitable.accept(visitor);
        else if (!this.visitorBuffer.contains(visitor))
            this.visitorBuffer.add(visitor);
    }

    /**
     * @param visitable -
     *            the instance to register
     * @throws RegistryException
     * @see ScopeVisitable
     */
    public synchronized void registerScopeVisitable(
            final ScopeVisitable visitable) throws RegistryException {
        if (visitable == null)
            throw new IllegalArgumentException("visitable must not be null");

        Scope scope = visitable.getClass().getAnnotation(Scope.class);
        if (scope == null)
            throw new RegistryException("Visitable has not Scope");
        if (LOG.isInfoEnabled())
            LOG.info("Register scope visitable -- " + visitable.getClass());
        if (scope.scope() == Scope.ScopeType.REQUEST
                && this.requestVisitable == null)
            this.requestVisitable = visitable;
        else if (scope.scope() == Scope.ScopeType.SESSION
                && this.sessionVisitable == null)
            this.sessionVisitable = visitable;
        else if (scope.scope() == Scope.ScopeType.CONTEXT
                && this.contextVisitable == null)
            this.sessionVisitable = visitable;

        if (!this.visitorBuffer.isEmpty()) {

            List<ScopeVisitor> tempList = this.visitorBuffer;
            this.visitorBuffer = new ArrayList<ScopeVisitor>(5);
            for (ScopeVisitor visitor : tempList) {
                registerScopeVisitor(visitor);
            }
            tempList.clear();

        }

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
                    "Service is null - must not be null to get registered feed type");
        return this.serviceTypeMap.get(service);
    }

    protected void flushRegistry() {
        Collection<ProvidedService> services = this.serviceTypeMap.values();
        for (ProvidedService service : services) {
            service.destroy();
        }
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
     * All registered {@link ServerComponent} registered via this method are
     * available via the
     * {@link GDataServerRegistry#lookup(Class, ComponentType)} method. For each
     * {@link ComponentType} there will be one single instance registered in the
     * registry.
     * <p>
     * Eventually this method invokes the initialize method of the
     * ServerComponent interface to prepare the component to be available via
     * the lookup service
     * </p>
     * 
     * @param <E> -
     *            The interface of the component to register
     * @param componentClass -
     *            a implementation of a ServerComponent interface to register in
     *            the registry
     * @param configuration -
     *            the component configuration {@link ComponentConfiguration}
     * @throws RegistryException -
     *             if the provided class does not implement the
     *             {@link ServerComponent} interface, if the mandatory
     *             annotations not visible at runtime or not set, if the super
     *             type provided by the {@link ComponentType} for the class to
     *             register is not a super type of the class or if the
     *             invocation of the {@link ServerComponent#initialize()} method
     *             throws an exception.
     */
    @SuppressWarnings("unchecked")
    public <E extends ServerComponent> void registerComponent(
            final Class<E> componentClass,
            final ComponentConfiguration configuration)
            throws RegistryException {

        if (componentClass == null)
            throw new IllegalArgumentException(
                    "component class must not be null");

        if (!ReflectionUtils.implementsType(componentClass,
                ServerComponent.class))
            throw new RegistryException(
                    "can not register component. the given class does not implement ServerComponent interface -- "
                            + componentClass.getName());
        try {

            Component annotation = componentClass
                    .getAnnotation(Component.class);
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
            if (!ReflectionUtils.isTypeOf(componentClass, superType))
                throw new RegistryException("Considered super type <"
                        + superType.getName() + "> is not a super type of <"
                        + componentClass + ">");
            ServerComponent comp = componentClass.newInstance();
            if (configuration == null) {
                if (LOG.isInfoEnabled())
                    LOG.info("no configuration for ComponentType: "
                            + type.name());
            } else
                configureComponent(comp, type, configuration);
            comp.initialize();
            ComponentBean bean = new ComponentBean(comp, superType);

            this.componentMap.put(type, bean);
            if (ReflectionUtils.implementsType(componentClass,
                    ScopeVisitor.class))
                this.registerScopeVisitor((ScopeVisitor) comp);
        } catch (Exception e) {
            throw new RegistryException("Can not register component -- "
                    + e.getMessage(), e);
        }

    }

    /*
     * Injects the configured properties located in the configuration into the
     * given server component
     */
    private void configureComponent(final ServerComponent component,
            final ComponentType type, final ComponentConfiguration configuration) {
        PropertyInjector injector = new PropertyInjector();
        injector.setTargetObject(component);
        injector.injectProperties(configuration);
    }

    private static class ComponentBean {
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

    /**
     * @see org.apache.lucene.gdata.server.registry.EntryEventMediator#getEntryEventMediator()
     */
    @Override
    public EntryEventMediator getEntryEventMediator() {
        
        return this;
    }

    /**
     * @return - all registered services
     */
    public Collection<ProvidedService> getServices() {
        
        return this.serviceTypeMap.values();
    }

}
