/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.auth;

import io.crate.auth.user.User;
import io.crate.auth.user.UserLookup;
import io.crate.protocols.postgres.ConnectionProperties;
import org.elasticsearch.common.settings.SecureString;


public class TrustAuthenticationMethod implements AuthenticationMethod {

    static final String NAME = "trust";
    private final UserLookup userLookup;

    public TrustAuthenticationMethod(UserLookup userLookup) {
        this.userLookup = userLookup;
    }

    @Override
    public User authenticate(String userName, SecureString passwd, ConnectionProperties connectionProperties) {
        User user = userLookup.findUser(userName);
        if (user == null) {
            throw new RuntimeException("trust authentication failed for user \"" + userName + "\"");
        }
        return user;
    }

    @Override
    public String name() {
        return NAME;
    }
}
