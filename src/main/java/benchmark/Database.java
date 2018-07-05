package benchmark;

import benchmark.entity.Fortune;
import benchmark.entity.World;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.reactiverse.reactivex.pgclient.*;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;


@Controller("/")
public class Database {

    private final Mustache mustache;
    private final PgPool pgPool;

    public Database(PgPool pgPool) {
        this.pgPool = pgPool;
        this.mustache = new DefaultMustacheFactory().compile("fortunes.mustache");
    }

    @Get("/db")
    public Single<World> db() {
        return pgPool.rxGetConnection()
                .doAfterSuccess(PgConnection::close)
                .flatMap((connection) -> {
            return findRandomWorld(connection, null);
        });
    }

    @Get("/queries")
    public Flowable<World> queries(@QueryValue String queries) {
        return findRandomWorlds(parseQueryCount(queries));
    }

    @Get(value = "/fortunes", produces = "text/html;charset=utf-8")
    public Writer fortune() {
        return mustache.execute(new StringWriter(), findFortunes());
    }

    @Get("/updates")
    public Flowable<World> updates(@QueryValue String queries) {
        return updateRandomWorlds(parseQueryCount(queries));
    }

    private Single<World> findRandomWorld(PgConnection connection, @Nullable Integer id) {
        return connection
                .rxPreparedQuery("SELECT * FROM world WHERE id = $1", Tuple.of(id != null ? id : nextNumber()))
                .map((result) -> {
            Row row = result.iterator().next();
            return new World(row.getInteger("id"), row.getInteger("randomnumber"));
        });
    }

    private int nextNumber() {
        return ThreadLocalRandom.current().nextInt(10000) + 1;
    }

    private Flowable<World> findRandomWorlds(int count) {
        return pgPool.rxGetConnection()
                .toFlowable()
                .doAfterNext(PgConnection::close)
                .flatMap((connection) -> {
            return findRandomWorlds(connection, count);
        });
    }

    private Flowable<World> findRandomWorlds(PgConnection connection, int count) {
        return Flowable.range(1, count).flatMap((id) -> {
            return findRandomWorld(connection, id).toFlowable();
        });
    }

    private Flowable<World> updateRandomWorlds(int count) {
        return pgPool.rxGetConnection()
                .toFlowable()
                .doAfterNext(PgConnection::close)
                .flatMap((connection) -> {
            return findRandomWorlds(connection, count).map((world) -> {
                world.setRandomNumber(nextNumber());
                connection.rxPreparedQuery("UPDATE world SET randomnumber = $1 WHERE id = $2", Tuple.of(world.getRandomNumber(), world.getId())).subscribe();
                return world;
            });
        });
    }

    private List<Fortune> findFortunes() {
        return pgPool.rxGetConnection()
                .toFlowable()
                .doAfterNext(PgConnection::close)
                .flatMap((connection) -> {
            return connection.rxPreparedQuery("SELECT * FROM fortune").toFlowable().flatMap((result) -> {
                PgIterator iterator = result.iterator();
                List<Fortune> fortunes = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    fortunes.add(new Fortune(row.getInteger("id"), row.getString("message")));
                }
                fortunes.add(new Fortune(0, "Additional fortune added at request time."));
                fortunes.sort(Comparator.comparing(Fortune::getMessage));

                return Flowable.fromIterable(fortunes);
            });
        }).toList().blockingGet();
    }

    private int parseQueryCount(String textValue) {
        if (textValue == null) {
            return 1;
        }
        int parsedValue;
        try {
            parsedValue = Integer.parseInt(textValue);
        } catch (NumberFormatException e) {
            return 1;
        }
        return Math.min(500, Math.max(1, parsedValue));
    }
}
