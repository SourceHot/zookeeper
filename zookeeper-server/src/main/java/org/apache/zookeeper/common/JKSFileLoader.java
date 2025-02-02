/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import java.security.KeyStore;
import java.security.KeyStoreException;

/**
 * Implementation of {@link FileKeyStoreLoader} that loads from JKS files.
 */
class JKSFileLoader extends StandardTypeFileKeyStoreLoader {
    private JKSFileLoader(String keyStorePath,
                          String trustStorePath,
                          String keyStorePassword,
                          String trustStorePassword) {
        super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
    }

    @Override
    protected KeyStore keyStoreInstance() throws KeyStoreException {
        return KeyStore.getInstance("JKS");
    }

    static class Builder extends FileKeyStoreLoader.Builder<JKSFileLoader> {
        @Override
        JKSFileLoader build() {
            return new JKSFileLoader(keyStorePath, trustStorePath, keyStorePassword,
                    trustStorePassword);
        }
    }
}
