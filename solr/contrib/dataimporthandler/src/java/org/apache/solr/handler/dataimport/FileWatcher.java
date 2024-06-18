/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (C) 2014 SilkCloud and/or its affiliates. All rights reserved.
 */

package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Java doc.
 */
public class FileWatcher {
    //region singleton
    private static volatile FileWatcher instance;

    public static FileWatcher getInstance() {
        // lazy singleton
        if (instance == null) {
            synchronized (FileWatcher.class) {
                if (instance == null) {
                    FileWatcher instance = new FileWatcher();
                    instance.initialize();
                    FileWatcher.instance = instance;
                }
            }
        }
        return instance;
    }

    public static void setInstance(FileWatcher watcher) {
        // setInstance for unit testing
        instance = watcher;
    }
    //endregion

    //region private fields
    private static Logger logger = LoggerFactory.getLogger(FileWatcher.class);

    private Thread thread;
    private WatchService watchService;
    private FileChangeListenerMap listeners = new FileChangeListenerMap();
    //endregion

    /**.
     * File listener
     */
    public interface FileListener {
        void onFileChanged(Path path, WatchEvent.Kind<Path> kind);
    }

    public FileWatcher addListener(Path path, FileListener listener) {
        try {
            WatchKey key = path.register(watchService, ENTRY_CREATE, ENTRY_MODIFY);
            listeners.addListener(key, listener);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return this;
    }

    public void close() {
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    //region private methods

    // protected empty constructor for subclassing/mocking
    protected FileWatcher() {
    }

    private void initialize() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
        }
        catch (IOException ex) {
            throw new RuntimeException("Error creating watch service.", ex);
        }
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                FileWatcher.this.run();
            }
        });
        thread.start();
    }

    private void run() {
        for (; ; ) {
            try {
                WatchKey key = watchService.take();
                Path path = (Path) key.watchable();

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        logger.warn("Overflow happened in FileWatcher for key " + key);
                        continue;
                    }

                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    listeners.notify(key, path, ev.kind());
                }
            }
            catch (InterruptedException ex) {
                return;
            }
            catch (Exception ex) {
                logger.warn("Error occurred in file watcher. ", ex);
            }
        }
    }

    //endregion

    //region private classes

    private static final class FileChangeListenerList {
        private ArrayList<WeakReference<FileListener>> listeners = new ArrayList<>();

        public void addListener(FileListener listener) {
            listeners.add(new WeakReference<>(listener));
        }

        public void notify(Path path, WatchEvent.Kind<Path> kind) {
            ArrayList<WeakReference<FileListener>> toRemove = new ArrayList<>();
            for (WeakReference<FileListener> listenerWeakReference : listeners) {
                FileListener listener = listenerWeakReference.get();
                if (listener == null) {
                    toRemove.add(listenerWeakReference);
                }
                else {
                    try {
                        listener.onFileChanged(path, kind);
                    }
                    catch (Exception ex) {
                        logger.warn("Error occurred in file watcher. ", ex);
                    }
                }
            }
            listeners.removeAll(toRemove);
        }
    }

    private static final class FileChangeListenerMap {
        private ConcurrentHashMap<WatchKey, FileChangeListenerList> listeners = new ConcurrentHashMap<>();

        public void addListener(WatchKey watchKey, FileListener listener) {
            FileChangeListenerList list = listeners.get(watchKey);
            if (list == null) {
                final FileChangeListenerList newList = new FileChangeListenerList();
                list = listeners.putIfAbsent(watchKey, newList);
                if (list == null) {
                    list = newList;
                }
            }
            list.addListener(listener);
        }

        public void notify(WatchKey watchKey, Path path, WatchEvent.Kind<Path> kind) {
            FileChangeListenerList list = listeners.get(watchKey);
            if (list != null) {
                list.notify(path, kind);
            }
        }
    }

    //endregion
}
