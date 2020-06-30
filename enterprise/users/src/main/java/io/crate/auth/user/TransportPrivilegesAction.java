/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.UsersMetadata;
import io.crate.metadata.UsersPrivilegesMetadata;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class TransportPrivilegesAction extends TransportMasterNodeAction<PrivilegesRequest, PrivilegesResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/privileges/grant";

    @Inject
    public TransportPrivilegesAction(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            PrivilegesRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PrivilegesResponse read(StreamInput in) throws IOException {
        return new PrivilegesResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PrivilegesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(PrivilegesRequest request, ClusterState state, ActionListener<PrivilegesResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("grant_privileges",
            new AckedClusterStateUpdateTask<PrivilegesResponse>(Priority.IMMEDIATE, request, listener) {

                long affectedRows = -1;
                List<String> unknownUserNames = null;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                    unknownUserNames = validateUserNames(currentMetadata, request.userNames());
                    if (unknownUserNames.isEmpty()) {
                        affectedRows = applyPrivileges(mdBuilder, request);
                    }
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                protected PrivilegesResponse newResponse(boolean acknowledged) {
                    return new PrivilegesResponse(acknowledged, affectedRows, unknownUserNames);
                }
            });

    }

    @VisibleForTesting
    static List<String> validateUserNames(Metadata metadata, Collection<String> userNames) {
        UsersMetadata usersMetadata = metadata.custom(UsersMetadata.TYPE);
        if (usersMetadata == null) {
            return new ArrayList<>(userNames);
        }
        List<String> unknownUserNames = null;
        for (String userName : userNames) {
            //noinspection PointlessBooleanExpression
            if (usersMetadata.userNames().contains(userName) == false) {
                if (unknownUserNames == null) {
                    unknownUserNames = new ArrayList<>();
                }
                unknownUserNames.add(userName);
            }
        }
        if (unknownUserNames == null) {
            return Collections.emptyList();
        }
        return unknownUserNames;
    }

    @VisibleForTesting
    static long applyPrivileges(Metadata.Builder mdBuilder,
                                PrivilegesRequest request) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetadata newMetadata = UsersPrivilegesMetadata.copyOf(
            (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE));

        long affectedRows = newMetadata.applyPrivileges(request.userNames(), request.privileges());
        mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, newMetadata);
        return affectedRows;
    }
}
