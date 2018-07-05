package benchmark.datasource;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.reactiverse.pgclient.PgPoolOptions;

@ConfigurationProperties("postgres.reactive")
public class PostgresProperties extends PgPoolOptions {

    protected String uri;

    public String getUri() {
        return uri;
    }

}
