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

package io.crate.replication.logical;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC;

public final class SynchronizeTableDefinitionsTask implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(SynchronizeTableDefinitionsTask.class);

    private final ThreadPool threadPool;
    private final Function<String, Client> remoteClient;
    private final ClusterService clusterService;
    private final TimeValue pollDelay;
    private final Iterator<TimeValue> delay;

    private volatile Set<String> subscriptionsToTrack = new HashSet<>();
    private Scheduler.Cancellable cancellable;
    private boolean isStarted = false;

    public SynchronizeTableDefinitionsTask(Settings settings, ThreadPool threadPool, Function<String, Client> remoteClient, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.remoteClient = remoteClient;
        this.clusterService = clusterService;
        this.pollDelay = LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION.get(settings);
        this.delay = BackoffPolicy.exponentialBackoff(pollDelay, 8).iterator();
    }

    public void start() {
        assert isStarted == false : "SynchronizeTableDefinitionsTask is already started";
        assert clusterService.state().getNodes().getLocalNode().isMasterNode() : "SynchronizeTableDefinitionsTask must only be executed on the master node";
        var executor = threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION);

        try {
            executor.execute(this::run);
        } catch (EsRejectedExecutionException e) {
            threadPool.schedule(
                this::run,
                delay.next(),
                ThreadPool.Names.LOGICAL_REPLICATION
            );
        }
        isStarted = true;
    }

    public synchronized boolean addSubscriptionsToTrack(String subscriptionName) {
        return subscriptionsToTrack.add(subscriptionName);
    }

    public synchronized boolean removeRemoteClusterToTrack(String subscriptionName) {
        return subscriptionsToTrack.remove(subscriptionName);
    }

    public boolean isStarted() {
        return isStarted;
    }

    private void run() {
        for (String subscriptionName : subscriptionsToTrack) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Start syncing table definitions for subscription {}", subscriptionName);
            }
            getRemoteClusterState(subscriptionName, new ActionListener<>() {
                @Override
                public void onResponse(ClusterState remoteClusterState) {
                    clusterService.submitStateUpdateTask("track-remote-cluster-metadata-changes",
                        new ClusterStateUpdateTask() {

                            @Override
                            public ClusterState execute(ClusterState localClusterState) throws Exception {
                                return syncMappings(localClusterState, remoteClusterState);
                            }

                            @Override
                            public void onFailure(String source, Exception e) {
                                LOGGER.error(e);
                            }
                        }
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error(e);
                }
            });
        }
        cancellable = threadPool.schedule(
            this::run,
            this.pollDelay,
            ThreadPool.Names.LOGICAL_REPLICATION
        );
    }

    @VisibleForTesting
    static ClusterState syncMappings(ClusterState localClusterState, ClusterState remoteClusterState) {
        PublicationsMetadata publicationsMetadata = remoteClusterState.metadata().custom(PublicationsMetadata.TYPE);
        SubscriptionsMetadata subscriptionsMetadata = localClusterState.metadata().custom(SubscriptionsMetadata.TYPE);
        // Find all subscribed tables
        var subscribedTables = new HashSet<RelationName>();
        for (var subscription : subscriptionsMetadata.subscription().values()) {
            for (var publicationName : subscription.publications()) {
                var publication = publicationsMetadata.publications().get(publicationName);
                subscribedTables.addAll(publication.tables());
            }
        }
        // Check for all the subscribed tables if the index metadata changed and apply
        // the changes from the publisher cluster state to the subscriber cluster state
        var metadataBuilder = Metadata.builder(localClusterState.metadata());
        boolean mappingsChanged = false;
        for (var followedTable : subscribedTables) {
            var remoteIndexMetadata = remoteClusterState.metadata().index(followedTable.indexNameOrAlias());
            var localIndexMetadata = localClusterState.metadata().index(followedTable.indexNameOrAlias());
            var remoteMapping = remoteIndexMetadata.mapping();
            var localMapping = localIndexMetadata.mapping();
            if (remoteMapping != null && localMapping != null) {
                if (!remoteMapping.equals(localMapping)) {
                    if (remoteIndexMetadata.getMappingVersion() > localIndexMetadata.getMappingVersion()) {
                        var indexMetadataBuilder = IndexMetadata.builder(localIndexMetadata).putMapping(
                            remoteMapping).mappingVersion(remoteIndexMetadata.getMappingVersion());
                        metadataBuilder.put(indexMetadataBuilder.build(), true);
                        mappingsChanged = true;
                    }
                }
            }
        }
        if (mappingsChanged) {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.trace("Updated index metadata from remote clusterr");
            }
            return ClusterState.builder(localClusterState).metadata(metadataBuilder).build();
        } else {
            return localClusterState;
        }
    }

    private void getRemoteClusterState(String subscriptionName, ActionListener<ClusterState> listener) {
        var client = remoteClient.apply(subscriptionName);

        var clusterStateRequest = client.admin().cluster().prepareState()
            .setWaitForTimeOut(new TimeValue(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC))
            .request();

        client.admin().cluster().execute(
            ClusterStateAction.INSTANCE, clusterStateRequest, new ActionListener<>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    listener.onResponse(clusterStateResponse.getState());
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
            isStarted = false;
        }
    }

}
