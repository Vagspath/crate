/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import io.crate.exceptions.RelationAlreadyExists;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;


public class LogicalReplicationITest extends LogicalReplicationIntegrationTest {

    @Test
    public void test_create_publication_for_concrete_table() {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
            " FROM pg_publication p" +
            " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
            " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
                   is("-119974068| pub1| -450373579| false| doc| t1| true| true| true\n"));
    }

    @Test
    public void test_create_publication_for_all_tables() {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub1 FOR ALL TABLES");
        executeOnPublisher("CREATE TABLE my_schema.t2 (id INT)");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
            " FROM pg_publication p" +
            " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
            " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
                   is("284890074| pub1| -450373579| true| doc| t1| true| true| true\n" +
                      "284890074| pub1| -450373579| true| my_schema| t2| true| true| true\n"));
    }

    @Test
    public void test_drop_publication() {
        executeOnPublisher("CREATE TABLE doc.t3 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub2 FOR TABLE doc.t3");
        executeOnPublisher("DROP PUBLICATION pub2");

        var response = executeOnPublisher("SELECT * FROM pg_publication WHERE pubname = 'pub2'");
        assertThat(response.rowCount(), is(0L));
        response = executeOnPublisher("SELECT * FROM pg_publication_tables WHERE pubname = 'pub2'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void test_subscribing_to_single_publication() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");
        ensureGreenOnSubscriber();

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
    }

    @Test
    public void test_subscribing_to_publication_while_table_exists_raises_error() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE TABLE doc.t1 (id int)");
        assertThrows(
            "Subscription 'sub1' cannot be created as included relation 'doc.t1' already exists",
            RelationAlreadyExists.class,
            () -> executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1")
        );
    }

    @Test
    public void test_subscribing_to_single_publication_with_multiple_tables() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE TABLE doc.t2 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t2 (id) VALUES (3), (4)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1, doc.t2");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");
        ensureGreenOnSubscriber();

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));

        executeOnSubscriber("REFRESH TABLE doc.t2");
        response = executeOnSubscriber("SELECT * FROM doc.t2 ORDER BY id");
        assertThat(printedTable(response.rows()), is("3\n" +
                                                     "4\n"));
    }

    @Test
    public void test_subscribing_to_single_publication_with_partitioned_table() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT, p INT) CLUSTERED INTO 1 SHARDS PARTITIONED BY (p) WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id, p) VALUES (1, 1), (2, 2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");
        ensureGreenOnSubscriber();

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 2\n"));
    }

    @Test
    public void test_subscribing_to_single_publication_for_all_tables() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT, p INT) CLUSTERED INTO 1 SHARDS PARTITIONED BY (p) WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id, p) VALUES (1, 1), (2, 2)");
        executeOnPublisher("CREATE TABLE my_schema.t2 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO my_schema.t2 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR ALL TABLES");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");
        ensureGreenOnSubscriber();

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 2\n"));

        executeOnSubscriber("REFRESH TABLE my_schema.t2");
        response = executeOnSubscriber("SELECT * FROM my_schema.t2 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
    }

    @Test
    public void test_subscribed_tables_are_followed_and_updated() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");
        ensureGreenOnSubscriber();

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));

        int numDocs = 100; // <- should be greater than REPLICATION_CHANGE_BATCH_SIZE to test repeated polls
        var bulkArgs = new Object[numDocs][1];
        for (int i = 0; i < numDocs; i++) {
            bulkArgs[i][0] = i;
        }
        executeBulkOnPublisher("INSERT INTO doc.t1 (id) VALUES (?)", bulkArgs);

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE doc.t1");
            var res = executeOnSubscriber("SELECT * FROM doc.t1");
            assertThat(res.rowCount(), is((long) (numDocs + 2)));
        }, 10, TimeUnit.SECONDS);
    }
}
