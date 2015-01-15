/*
 * Copyright 2014 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.nomq.store.jdbc;

import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.Startable;
import org.nomq.core.support.DefaultEvent;
import org.nomq.core.support.StreamUtil;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.util.Optional;
import java.util.stream.Stream;

import static org.nomq.core.support.StreamUtil.createStream;
import static org.nomq.core.support.StreamUtil.delegate;

/**
 * Base class for JDBC-based EventStores.
 *
 * @author Tommy Wassgren
 */
public abstract class AbstractJdbcEventStore implements EventStore, AutoCloseable, Startable<EventStore> {
    private final boolean create;
    private final DBI dbi;
    private final ResultSetMapper<Event> eventResultSetMapper =
            (index, rs, ctx) -> new DefaultEvent(
                    rs.getString("EVENT_ID"),
                    rs.getString("EVENT_TYPE"),
                    rs.getBytes("EVENT_PAYLOAD"));
    private Handle writeHandle;
    
    protected AbstractJdbcEventStore(final String jdbcUrl, final boolean create) {
        this.dbi = new DBI(jdbcUrl);
        this.create = create;
    }

    @Override
    public void append(final Event event) {
        final int numberOfRecords = writeHandle.createStatement(sqlInsertEvent())
                .bind("eventId", event.id())
                .bind("eventType", event.type())
                .bind("eventPayload", event.payload())
                .execute();

        if (numberOfRecords != 1) {
            throw new IllegalStateException("Unable to insert event in event store");
        }
    }

    @Override
    public void close() {
        StreamUtil.close(writeHandle);
    }

    @Override
    public Optional<Event> latest() {
        return dbi.withHandle(handle ->
                Optional.ofNullable(handle.createQuery(sqlSelectLatestEvent())
                        .map(eventResultSetMapper)
                        .first()));
    }

    @Override
    public Stream<Event> replay(final String latestProcessedId) {
        final Handle h = dbi.open();
        final Long startId = h.createQuery(sqlSelectOneEvent())
                .bind("eventId", latestProcessedId)
                .map((index, rs, ctx) -> rs.getLong("ID"))
                .first();

        return createStream(
                delegate(
                        h.createQuery(sqlSelectEventsStartingWithId())
                                .bind("startId", startId)
                                .map(eventResultSetMapper)
                                .iterator(),
                        itr -> itr,
                        itr -> StreamUtil.close(itr, h)));

    }

    @Override
    public Stream<Event> replayAll() {
        final Handle h = dbi.open();

        return createStream(
                delegate(
                        h.createQuery(sqlSelectAllEvents())
                                .map(eventResultSetMapper)
                                .iterator(),
                        itr -> itr,
                        itr -> StreamUtil.close(itr, h))
        );
    }

    @Override
    public EventStore start() {
        writeHandle = dbi.open();
        if (create) {
            doCreate(writeHandle);
        }

        return this;
    }

    protected void doCreate(final Handle writeHandle) {
        // Default to nothing
    }

    protected String sqlInsertEvent() {
        return "INSERT INTO NOMQ_EVENTS(EVENT_ID, EVENT_TYPE, EVENT_PAYLOAD) VALUES(:eventId, :eventType, :eventPayload)";
    }

    protected String sqlSelectAllEvents() {
        return "SELECT EVENT_ID, EVENT_TYPE, EVENT_PAYLOAD FROM NOMQ_EVENTS ORDER BY ID ASC";
    }

    protected String sqlSelectEventsStartingWithId() {
        return "SELECT " +
                "EVENT_ID, EVENT_TYPE, EVENT_PAYLOAD " +
                "FROM NOMQ_EVENTS  " +
                "WHERE ID > :startId " +
                "ORDER BY ID ASC";
    }

    protected String sqlSelectLatestEvent() {
        return "SELECT EVENT_ID, EVENT_TYPE, EVENT_PAYLOAD FROM NOMQ_EVENTS ORDER BY ID DESC LIMIT 1";
    }

    protected String sqlSelectOneEvent() {
        return "SELECT ID FROM NOMQ_EVENTS WHERE EVENT_ID = :eventId";
    }
}
