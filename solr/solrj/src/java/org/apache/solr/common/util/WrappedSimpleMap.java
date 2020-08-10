package org.apache.solr.common.util;

import org.apache.solr.cluster.api.SimpleMap;

import java.util.Map;
import java.util.function.BiConsumer;

public class WrappedSimpleMap<T>  implements SimpleMap<T> {
    private final Map<String, T> delegate;

    @Override
    public T get(String key) {
        return delegate.get(key);
    }

    @Override
    public void forEachEntry(BiConsumer<String, ? super T> fun) {
        delegate.forEach(fun);

    }

    @Override
    public int size() {
        return delegate.size();
    }


    public WrappedSimpleMap(Map<String, T> delegate) {
        this.delegate = delegate;
    }

}
