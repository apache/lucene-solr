package org.apache.solr.store.blob.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serializing/deserializing Json (mostly for {@link org.apache.solr.store.blob.client.BlobCoreMetadata}).
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class ToFromJson<T> {
    /** Create easier to (human) read but a bit longer json output */
    static final boolean PRETTY_JSON = true;

    /**
     * Builds an object instance from a String representation of the Json.
     */
    public T fromJson(String input, Class<T> c) throws Exception {
        Gson gson = new Gson();
        return gson.fromJson(input, c);
    }

    /**
     * Returns the Json String of the passed object instance.
     */
    public String toJson(T t) throws Exception {
        Gson gson = PRETTY_JSON ? new GsonBuilder().setPrettyPrinting().create() : new Gson();
        return gson.toJson(t);
    }
}
