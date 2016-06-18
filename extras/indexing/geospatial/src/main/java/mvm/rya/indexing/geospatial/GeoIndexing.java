package mvm.rya.indexing.geospatial;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.ConfigUtils;
import mvm.rya.indexing.DelegatingFilterFunctionOptimizer;
import mvm.rya.indexing.ExternalTupleSetProvider;
import mvm.rya.indexing.IndexingSPI;
import mvm.rya.indexing.geospatial.mongodb.MongoGeoIndexer;

import java.util.Collection;
import java.util.Collections;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class GeoIndexing implements IndexingSPI {
    @Override
    public Configurator getCongigurator() {
        return new Configurator() {

            @Override
            public void configure(RdfCloudTripleStoreConfiguration conf) {

            }

            @Override
            public Collection<String> getIndexers(RdfCloudTripleStoreConfiguration conf) {
                if(GeoConfig.getUseGeo(conf)) {
                    if (ConfigUtils.getUseMongo(conf)) {
                        return Collections.singleton(MongoGeoIndexer.class.getName());
                    } else {
                        return Collections.singleton(GeoMesaGeoIndexer.class.getName());
                    }
                }
                return Collections.emptySet();
            }

            @Override
            public Collection<String> getOptimizers(RdfCloudTripleStoreConfiguration conf) {
                if(GeoConfig.getUseGeo(conf)) {
                    return Collections.singleton(DelegatingFilterFunctionOptimizer.class.getName());
                }
                return Collections.emptySet();
            }

            @Override
            public Class<? extends ExternalTupleSetProvider> provider() {
                return GeoTupleSetProvider.class;
            }
        };
    }
}
