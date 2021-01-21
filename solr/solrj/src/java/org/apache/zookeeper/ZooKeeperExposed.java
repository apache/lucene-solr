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
package org.apache.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperExposed {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperTestable.class);

    private final ZooKeeper zooKeeper;
    private final ClientCnxn clientCnxn;

    public ZooKeeperExposed(ZooKeeper zooKeeper, ClientCnxn clientCnxn) {
        this.zooKeeper = zooKeeper;
        this.clientCnxn = clientCnxn;
    }

    public void setSendThreadState() {
        clientCnxn.state = ZooKeeper.States.CLOSED;
        clientCnxn.eventThread.queueEventOfDeath();
    }

    public void interruptSendThread() {
        try {
            clientCnxn.sendThread.join(10);
        } catch (InterruptedException e) {
            // okay
        }
        clientCnxn.sendThread.interrupt();
    }

    public void interruptEventThread() {
        //    while (clientCnxn.eventThread.isAlive()) {
        clientCnxn.eventThread.interrupt();
        //            try {
        //                clientCnxn.eventThread.join(5000);
        //            } catch (InterruptedException e) {
        //               // ParWork.propegateInterrupt(e);
        //            }
        //   }
    }

    public void closeCnxn() {
        if (!clientCnxn.getState().isAlive()) {
            LOG.debug("Close called on already closed client");
            return;
        }

        clientCnxn.sendThread.close();

        try {
            clientCnxn.sendThread.join(20);
        } catch (InterruptedException e) {
        }

        clientCnxn.eventThread.queueEventOfDeath();

        if (clientCnxn.zooKeeperSaslClient != null) {
            clientCnxn.zooKeeperSaslClient.shutdown();
        }
    }
    //    @Override
    //    public void injectSessionExpiration() {
    //        LOG.info("injectSessionExpiration() called");
    //
    //        clientCnxn.eventThread.queueEvent(new WatchedEvent(
    //                Watcher.Event.EventType.None,
    //                Watcher.Event.KeeperState.Expired, null));
    //        clientCnxn.eventThread.queueEventOfDeath();
    //        clientCnxn.state = ZooKeeper.States.CLOSED;
    //        clientCnxn.sendThread.getClientCnxnSocket().onClosing();
    //    }
}
