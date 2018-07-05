package benchmark.datasource;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.core.util.StringUtils;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgClient;
import io.reactiverse.reactivex.pgclient.PgPool;

import javax.inject.Singleton;

@Factory
public class PostgresFactory {

    @Singleton
    @Bean(preDestroy = "close")
    PgPool getPgClient(PostgresProperties postgresProperties) {
        if (StringUtils.isNotEmpty(postgresProperties.uri)) {
            return PgClient.pool(PgPoolOptions.fromUri(postgresProperties.uri));
        }
        return PgClient.pool(postgresProperties);
    }
}
