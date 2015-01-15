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

import org.skife.jdbi.v2.Handle;

/**
 * @author Tommy Wassgren
 */
public class H2EventStore extends AbstractJdbcEventStore {
    public H2EventStore(final String jdbcUrl) {
        this(jdbcUrl, true);
    }

    public H2EventStore(final String jdbcUrl, final boolean create) {
        super(jdbcUrl, create);
    }

    @Override
    protected void doCreate(final Handle handle) {
        final String createStatement =
                "CREATE TABLE IF NOT EXISTS " +
                        "NOMQ_EVENTS(" +
                        "ID INT PRIMARY KEY AUTO_INCREMENT NOT NULL, " +
                        "EVENT_ID VARCHAR(255) NOT NULL, " +
                        "EVENT_TYPE VARCHAR(255) NOT NULL, " +
                        "EVENT_PAYLOAD VARCHAR(100000))";

        handle.execute(createStatement);
    }
}
