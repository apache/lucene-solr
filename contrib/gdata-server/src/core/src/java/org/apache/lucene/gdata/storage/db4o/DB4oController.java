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

package org.apache.lucene.gdata.storage.db4o;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.Scope;
import org.apache.lucene.gdata.server.registry.ScopeVisitor;
import org.apache.lucene.gdata.server.registry.configuration.Requiered;
import org.apache.lucene.gdata.storage.IDGenerator;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.db4o.DB4oStorage.DB4oEntry;
import org.apache.lucene.gdata.utils.Pool;
import org.apache.lucene.gdata.utils.PoolObjectFactory;
import org.apache.lucene.gdata.utils.SimpleObjectPool;

import com.db4o.Db4o;
import com.db4o.ObjectContainer;
import com.db4o.ObjectServer;
import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;

/**
 * The DB4o StorageContorller can be used as a persitence component for the
 * gdata-server. To use DB4o a third party jar needs to added to the lib
 * directory of the project. If the jar is not available in the lib directory
 * all db4o dependent class won't be included in the build.
 * <p>
 * If the jar is present in the lib directory this class can be configured as a
 * {@link org.apache.lucene.gdata.server.registry.ComponentType#STORAGECONTROLLER}
 * via the <i>gdata-config.xml</i> file. For detailed config documentation see
 * the wiki page.
 * </p>
 * <p>
 * The DB4oController can run as a client or as a server to serve other running
 * db4o clients in the network. To achive the best performance out of the db4o
 * caching layer connections to the server will be reused in a connection pool.
 * A connection will not be shared withing more than one thread. The controller
 * release one connection per request and returns the connection when the
 * request has been destroyed.
 * </p>
 * @see <a href="http://www.db4o.com">db4o website</a>
 * @see org.apache.lucene.gdata.utils.Pool
 * 
 * 
 * @author Simon Willnauer
 * 
 */
@Component(componentType = ComponentType.STORAGECONTROLLER)
@Scope(scope = Scope.ScopeType.REQUEST)
public class DB4oController implements StorageController, ScopeVisitor {
    private static final Log LOG = LogFactory.getLog(DB4oController.class);

    private final ThreadLocal<Storage> threadLocalStorage = new ThreadLocal<Storage>();

    private Pool<ObjectContainer> containerPool;

    private ObjectServer server;

    private final IDGenerator idGenerator;

    private boolean weakReferences;

    private boolean runAsServer;

    private int port;

    private String filePath;

    private String user;

    private String password;

    private String host;

    private int containerPoolSize;

    /**
     * @throws NoSuchAlgorithmException
     * 
     */
    public DB4oController() throws NoSuchAlgorithmException {

        this.idGenerator = new IDGenerator(15);

    }

    ObjectContainer releaseContainer() {
        return this.server.openClient();

    }

    /**
     * @see org.apache.lucene.gdata.storage.StorageController#destroy()
     */
    public void destroy() {
        this.containerPool.destroy();
        this.idGenerator.stopIDGenerator();
        this.server.close();
    }

    /**
     * @see org.apache.lucene.gdata.storage.StorageController#getStorage()
     */
    public Storage getStorage() throws StorageException {
       Storage retVal = this.threadLocalStorage.get();
        if (retVal != null)
            return retVal;

        retVal = new DB4oStorage(this.containerPool.aquire(), this);

        this.threadLocalStorage.set(retVal);
        return retVal;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
        if (LOG.isInfoEnabled())
            LOG.info("Initialize " + this.toString());

        Db4o.configure().objectClass(DB4oEntry.class).objectField("updated")
                .indexed(true);
        Db4o.configure().objectClass(BaseEntry.class).objectField("id")
                .indexed(true);
        Db4o.configure().objectClass(BaseFeed.class).objectField("id").indexed(
                true);
        Db4o.configure().objectClass(GDataAccount.class).objectField("name")
                .indexed(true);
        Db4o.configure().objectClass(ServerBaseFeed.class).cascadeOnDelete(
                false);
        Db4o.configure().objectClass(ServerBaseFeed.class)
                .maximumActivationDepth(0);
        Db4o.configure().objectClass(BaseFeed.class).minimumActivationDepth(1);
        Db4o.configure().objectClass(BaseEntry.class)
                .minimumActivationDepth(1);
        Db4o.configure().objectClass(BaseFeed.class).cascadeOnDelete(true);
        Db4o.configure().objectClass(DB4oEntry.class).cascadeOnDelete(true);
        Db4o.configure().objectClass(GDataAccount.class).cascadeOnDelete(true);
        Db4o.configure().weakReferences(this.weakReferences);
        Db4o.configure().optimizeNativeQueries(false);
        if (this.runAsServer) {
            this.server = Db4o.openServer(this.filePath, this.port);
            if(this.server == null)
                throw new RuntimeException("Can't create server at confiugred destination -- "+this.filePath);
            this.server.grantAccess(this.user, this.password);
        } else {
            InvocationHandler handler = new ObjectServerDecorator(this.user,
                    this.password, this.host, this.port);
            this.server = (ObjectServer) Proxy.newProxyInstance(this.getClass()
                    .getClassLoader(), new Class[] { ObjectServer.class },
                    handler);
        }

        PoolObjectFactory<ObjectContainer> factory = new ObjectContinerFactory(
                this.server);
        this.containerPool = new SimpleObjectPool<ObjectContainer>(
                this.containerPoolSize, factory);
        try {
            createAdminAccount();
        } catch (StorageException e) {
            LOG.error("Can not create admin account -- ",e);
        }
    }

    private void createAdminAccount() throws StorageException {
        GDataAccount adminAccount = GDataAccount.createAdminAccount();
        visiteInitialize();
        Storage sto = this.getStorage();
        try {
            sto.getAccount(adminAccount.getName());
        } catch (Exception e) {
            this.getStorage().storeAccount(adminAccount);
        } finally {
            visiteDestroy();
        }

    }

    
    /**
     * @see org.apache.lucene.gdata.storage.StorageController#releaseId()
     */
    public String releaseId(){
        try{
        return this.idGenerator.getUID();
        }catch (InterruptedException e) {
            throw new StorageException("ID producer has been interrupted",e);
        }
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ScopeVisitor#visiteInitialize()
     */
    public void visiteInitialize() {
        if (LOG.isInfoEnabled())
            LOG.info("Opened Storage -- request initialized");
        Storage storage = this.threadLocalStorage.get();
        if (storage != null) {
            LOG.warn("Storage already opened");
            return;
        }

        storage = new DB4oStorage(this.containerPool.aquire(), this);

        this.threadLocalStorage.set(storage);
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ScopeVisitor#visiteDestroy()
     */
    public void visiteDestroy() {
        Storage storage = this.threadLocalStorage.get();
        if (storage == null) {
            LOG.warn("no Storage opened -- threadlocal returned null");
            return;
        }
        this.containerPool.release(((DB4oStorage)storage).getContainer());
        this.threadLocalStorage.remove();
        if (LOG.isInfoEnabled())
            LOG.info("Closed Storage -- request destroyed");
    }

    private static class ObjectContinerFactory implements
            PoolObjectFactory<ObjectContainer> {
        private final ObjectServer server;

        ObjectContinerFactory(final ObjectServer server) {
            this.server = server;
        }

        /**
         * @see org.apache.lucene.gdata.utils.PoolObjectFactory#getInstance()
         */
        public ObjectContainer getInstance() {

            return this.server.openClient();
        }

        /**
         * @param type -
         *            object container to destroy (close)
         * @see org.apache.lucene.gdata.utils.PoolObjectFactory#destroyInstance(Object)
         */
        public void destroyInstance(ObjectContainer type) {
            type.close();
        }

    }

    /**
     * @return Returns the filePath.
     */
    public String getFilePath() {
        return this.filePath;
    }

    /**
     * @param filePath
     *            The filePath to set.
     */
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    /**
     * @return Returns the host.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * @param host
     *            The host to set.
     */
    @Requiered
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * @param password
     *            The password to set.
     */
    @Requiered
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the port.
     */
    public int getPort() {
        return this.port;
    }

    /**
     * @param port
     *            The port to set.
     */
    @Requiered
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return Returns the runAsServer.
     */
    public boolean isRunAsServer() {
        return this.runAsServer;
    }

    /**
     * @param runAsServer
     *            The runAsServer to set.
     */
    @Requiered
    public void setRunAsServer(boolean runAsServer) {
        this.runAsServer = runAsServer;
    }

    /**
     * @return Returns the user.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * @param user
     *            The user to set.
     */
    @Requiered
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return Returns the weakReferences.
     */
    public boolean isUseWeakReferences() {
        return this.weakReferences;
    }

    /**
     * @param weakReferences
     *            The weakReferences to set.
     */
    @Requiered
    public void setUseWeakReferences(boolean weakReferences) {
        this.weakReferences = weakReferences;
    }

    /**
     * @return Returns the containerPoolSize.
     */
    public int getContainerPoolSize() {
        return this.containerPoolSize;
    }

    /**
     * @param containerPoolSize
     *            The containerPoolSize to set.
     */
    @Requiered
    public void setContainerPoolSize(int containerPoolSize) {
        this.containerPoolSize = containerPoolSize < 1 ? 1 : containerPoolSize;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getName())
                .append(" ");
        builder.append("host: ").append(this.host).append(" ");
        builder.append("port: ").append(this.port).append(" ");
        builder.append("pool size: ").append(this.containerPoolSize)
                .append(" ");
        builder.append("runs as server: ").append(this.runAsServer).append(" ");
        builder.append("use weak references: ").append(this.weakReferences)
                .append(" ");
        builder.append("user: ").append(this.user).append(" ");
        builder.append("password length: ").append(
                this.password == null ? "no password" : this.password.length())
                .append(" ");

        return builder.toString();
    }

}
