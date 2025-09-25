package com.facebook.presto.hudi;

import com.facebook.presto.Session;

import static com.facebook.presto.hudi.HudiSessionProperties.COLUMN_STATS_INDEX_ENABLED;
import static com.facebook.presto.hudi.HudiSessionProperties.COLUMN_STATS_WAIT_TIMEOUT;
import static com.facebook.presto.hudi.HudiSessionProperties.HUDI_METADATA_TABLE_ENABLED;
import static java.util.Objects.requireNonNull;

public class SessionBuilder
{
    private final Session.SessionBuilder sessionBuilder;
    private final String catalogName;

    private SessionBuilder(Session session)
    {
        requireNonNull(session, "Initial session cannot be null");
        this.sessionBuilder = Session.builder(session);
        this.catalogName = session.getCatalog()
                .orElseThrow(() -> new IllegalStateException("Session must have a catalog to configure properties."));
    }

    /**
     * Creates a new SessionPropertyConfigurator from an existing session.
     *
     * @param session The base session to build upon.
     * @return A new instance of SessionPropertyConfigurator.
     */
    public static SessionBuilder from(Session session)
    {
        return new SessionBuilder(session);
    }

    private SessionBuilder setCatalogProperty(String propertyName, String propertyValue)
    {
        this.sessionBuilder.setCatalogSessionProperty(catalogName, propertyName, propertyValue);
        return this;
    }

    /**
     * Builds the new Session with the configured properties.
     *
     * @return The newly configured Session object.
     */
    public Session build()
    {
        return this.sessionBuilder.build();
    }

    public SessionBuilder withMdtEnabled(boolean enabled)
    {
        return setCatalogProperty(HUDI_METADATA_TABLE_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withColumnStatsWaitTimeout(String durationProp)
    {
        return setCatalogProperty(COLUMN_STATS_WAIT_TIMEOUT, durationProp);
    }

    public SessionBuilder withColumnStatsEnabled(boolean enabled)
    {
        return setCatalogProperty(COLUMN_STATS_INDEX_ENABLED, String.valueOf(enabled));
    }
}
