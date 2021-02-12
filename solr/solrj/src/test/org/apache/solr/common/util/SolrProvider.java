package org.apache.solr.common.util;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.nosql.NoSqlConnection;
import org.apache.logging.log4j.core.appender.nosql.NoSqlObject;
import org.apache.logging.log4j.core.appender.nosql.NoSqlProvider;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.util.Loader;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import java.lang.reflect.Method;

/**
 * The Solr implementation of {@link NoSqlProvider}.
 */
@Plugin(name = "Solr", category = "Core", printObject = true)
public class SolrProvider implements NoSqlProvider {
  private static final Logger LOGGER = StatusLogger.getLogger();

  private final String collectionName;

  private final Http2SolrClient solrClient;
  private final String description;

  private SolrProvider(final Http2SolrClient solrClient, final String collectionName, final String description) {
    this.solrClient = solrClient;
    this.collectionName = collectionName;
    this.description = "solr{ " + description + " }";
  }

  @Override
  public NoSqlConnection<?,? extends NoSqlObject<?>> getConnection() {
    return new SolrConnection(solrClient, collectionName, description);
  }

  @PluginFactory
  public static SolrProvider createNoSqlProvider(
      @PluginAttribute("collectionName") final String collectionName,
      @PluginAttribute("databaseName") final String databaseName,
      @PluginAttribute("server") final String server,
      @PluginAttribute("port") final String port,
      @PluginAttribute(value = "password", sensitive = true) final String password,
      @PluginAttribute("factoryClassName") final String factoryClassName,
      @PluginAttribute("factoryMethodName") final String factoryMethodName) {
    Http2SolrClient.Builder builder;
    Http2SolrClient solrClient = null;
    String description;
    if (factoryClassName != null && factoryClassName.length() > 0 &&
        factoryMethodName != null && factoryMethodName.length() > 0) {
      try {
        final Class<?> factoryClass = Loader.loadClass(factoryClassName);
        final Method method = factoryClass.getMethod(factoryMethodName);
        final Object object = method.invoke(null);

        if (object instanceof Http2SolrClient.Builder) {
          builder = (Http2SolrClient.Builder) object;
        } else if (object == null) {
          LOGGER.error("The factory method [{}.{}()] returned null.", factoryClassName, factoryMethodName);
          return null;
        } else {
          LOGGER.error("The factory method [{}.{}()] returned an unsupported type [{}].", factoryClassName,
              factoryMethodName, object.getClass().getName());
          return null;
        }

        description = "database=" + builder;
        solrClient = builder.withBaseUrl("").build();
//        final List<ServerAddress> addresses = database.getMongo().getAllAddress();
//        if (addresses.size() == 1) {
//          description += ", server=" + addresses.get(0).getHost() + ", port=" + addresses.get(0).getPort();
//        } else {
//          description += ", servers=[";
//          for (final ServerAddress address : addresses) {
//            description += " { " + address.getHost() + ", " + address.getPort() + " } ";
//          }
//          description += "]";
//        }
      } catch (final ClassNotFoundException e) {
        LOGGER.error("The factory class [{}] could not be loaded.", factoryClassName, e);
        return null;
      } catch (final NoSuchMethodException e) {
        LOGGER.error("The factory class [{}] does not have a no-arg method named [{}].", factoryClassName,
            factoryMethodName, e);
        return null;
      } catch (final Exception e) {
        LOGGER.error("The factory method [{}.{}()] could not be invoked.", factoryClassName, factoryMethodName,
            e);
        return null;
      }
    } else if (databaseName != null && databaseName.length() > 0) {
      description = "database=" + databaseName;
      try {
        if (server != null && server.length() > 0) {
          final int portInt = AbstractAppender.parseInt(port, 0);
          description += ", server=" + server;
          if (portInt > 0) {
            description += ", port=" + portInt;
            solrClient = new Http2SolrClient.Builder().withBaseUrl(server + ":"  + portInt).build();
          }
        } else {
        //  database = new MongoClient().getDB(databaseName);
        }
      } catch (final Exception e) {
        LOGGER.error("Failed to obtain a database instance from the MongoClient at server [{}] and "
            + "port [{}].", server, port);
        return null;
      }
    } else {
      LOGGER.error("No factory method was provided so the database name is required.");
      return null;
    }
//
//    if (!database.isAuthenticated()) {
//      if (username != null && username.length() > 0 && password != null && password.length() > 0) {
//        description += ", username=" + username + ", passwordHash="
//            + NameUtil.md5(password + SolrProvider.class.getName());
//        SolrConnection.authenticate(database, username, password);
//      } else {
//        LOGGER.error("The database is not already authenticated so you must supply a username and password "
//            + "for the MongoDB provider.");
//        return null;
//      }
//    }


      return new SolrProvider(solrClient, collectionName, description);
  }
}
