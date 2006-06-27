package org.apache.lucene.gdata.utils;

import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.data.Entry;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.Feed;

public class ProvidedServiceStub implements ProvidedService {

    public static final String SERVICE_NAME = "service";

    public ProvidedServiceStub() {
        super();
        // TODO Auto-generated constructor stub
    }

    public Class getFeedType() {

        return Feed.class;
    }

    public ExtensionProfile getExtensionProfile() {

        return new ExtensionProfile();
    }

    public Class getEntryType() {

        return Entry.class;
    }

    public String getName() {

        return SERVICE_NAME;
    }

}
