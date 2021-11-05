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

package io.crate.replication.logical.action;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.user.Privilege;
import io.crate.user.User;
import io.crate.user.UserLookup;
import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.MockLogAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.crate.user.Privilege.Type.READ_WRITE_DEFINE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PublicationsStateActionTest extends CrateDummyClusterServiceUnitTest {

    private MockLogAppender appender;

    @Before
    public void appendLogger() throws Exception {
        appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(Loggers.getLogger(PublicationsStateAction.class), appender);

    }

    @After
    public void removeLogger() {
        Loggers.removeAppender(Loggers.getLogger(PublicationsStateAction.class), appender);
        appender.stop();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_with_soft_delete_disabled() throws Exception {
        var user = new User("dummy", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true; // This test case doesn't check privileges.
            }
        };
        var s = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int) with (\"soft_deletes.enabled\" = false)")
            .build();
        var publication = new Publication("some_user", true, List.of());

        var expectedLogMessage = "Table 'doc.t2' won't be replicated as the required table setting " +
            "'soft_deletes.enabled' is set to: false";
        appender.addExpectation(new MockLogAppender.SeenEventExpectation(
            expectedLogMessage,
            Loggers.getLogger(PublicationsStateAction.class).getName(),
            Level.WARN,
            expectedLogMessage
        ));

        var resolvedRelations = PublicationsStateAction.TransportAction.resolveRelationsNames(
            publication,
            s.schemas(),
            user,
            user
        );
        assertThat(resolvedRelations, contains(new RelationName("doc", "t1")));
        appender.assertAllExpectationsMatched();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_when_pub_owner_doesnt_have_read_write_define_permissions() throws Exception {
        var publicationOwner = new User("publisher", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return (READ_WRITE_DEFINE.contains(type) && clazz.equals(Privilege.Clazz.TABLE) && ident.equals("doc.t1"));
            }
        };
        var subscriber = new User("subscriber", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true;
            }
        };

        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser("publisher")).thenReturn(publicationOwner);
        when(userLookup.findUser("subscriber")).thenReturn(subscriber);

        var s = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t3 (id int)")
            .build();
        var publication = new Publication("publisher", true, List.of());

        var resolvedRelations = PublicationsStateAction.TransportAction.resolveRelationsNames(
            publication,
            s.schemas(),
            publicationOwner,
            subscriber
        );
        assertThat(resolvedRelations, contains(new RelationName("doc", "t1")));
        assertThat(resolvedRelations, not(contains(new RelationName("doc", "t3"))));
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_when_subscriber_doesnt_have_read_permissions() throws Exception {
        var publicationOwner = new User("publisher", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true;
            }
        };

        var subscriber = new User("subscriber", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return (type.equals(Privilege.Type.DQL) && clazz.equals(Privilege.Clazz.TABLE) && ident.equals("doc.t1"));
            }
        };

        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser("publisher")).thenReturn(publicationOwner);
        when(userLookup.findUser("subscriber")).thenReturn(subscriber);

        var s = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t3 (id int)")
            .build();
        var publication = new Publication("publisher", true, List.of());

        var resolvedRelations = PublicationsStateAction.TransportAction.resolveRelationsNames(
            publication,
            s.schemas(),
            publicationOwner,
            subscriber
        );
        assertThat(resolvedRelations, contains(new RelationName("doc", "t1")));
        assertThat(resolvedRelations, not(contains(new RelationName("doc", "t3"))));
    }

    @Test
    public void test_publication_owner_not_found_throws_exception() {
        String publicationOwner = "publisher";
        String publicationName = "pub 1";
        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser(publicationOwner)).thenReturn(null);

        String expected = String.format(Locale.ENGLISH, "User %s owning the publication %s is not found, stopping replication.",
            publicationOwner,
            publicationName
        );

        Asserts.assertThrowsMatches(
            () -> PublicationsStateAction.TransportAction.ensurePublicationOwnerExists(publicationOwner, userLookup, publicationName),
            IllegalStateException.class,
            expected
        );
    }

    @Test
    public void test_subscriber_not_found_throws_exception() {
        String subscriber = "subscriber";
        String publicationName = "pub 1";
        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser(subscriber)).thenReturn(null);

        String expected = String.format(Locale.ENGLISH, "User %s subscribed to the publication %s is not found, stopping replication.",
            subscriber,
            publicationName
        );

        Asserts.assertThrowsMatches(
            () -> PublicationsStateAction.TransportAction.ensureSubscriberExists(subscriber, userLookup, publicationName),
            IllegalStateException.class,
            expected
        );
    }
}
