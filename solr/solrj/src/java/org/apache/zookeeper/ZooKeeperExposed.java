package org.apache.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperExposed {
    private static final Logger LOG = LoggerFactory
            .getLogger(ZooKeeperTestable.class);

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
          clientCnxn.sendThread.join(10);
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
