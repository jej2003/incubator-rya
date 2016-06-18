package mvm.rya.indexing.geospatial;

import mvm.rya.indexing.ConfigUtils;
import mvm.rya.indexing.ExternalTupleSetProvider;
import mvm.rya.indexing.IndexingExpr;
import mvm.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.indexing.geospatial.mongodb.MongoGeoIndexer;
import org.apache.hadoop.conf.Configuration;

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

public class GeoTupleSetProvider implements ExternalTupleSetProvider {
    GeoIndexer indexer;
    @Override
    public void configure(Configuration conf) {
        if (ConfigUtils.getUseMongo(conf)) {
            indexer = new MongoGeoIndexer();
            indexer.setConf(conf);

        } else {
            indexer = new GeoMesaGeoIndexer();
            indexer.setConf(conf);
        }
    }

    @Override
    public FUNCTION_TYPE getFunctionType() {
        return FUNCTION_TYPE.GEO;
    }

    @Override
    public ExternalTupleSet get(IndexingExpr expr) {
        return new GeoTupleSet(expr, indexer);
    }
}
