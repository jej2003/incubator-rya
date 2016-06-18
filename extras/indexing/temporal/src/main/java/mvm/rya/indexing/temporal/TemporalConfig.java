package mvm.rya.indexing.temporal;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.ConfigUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import java.util.HashSet;
import java.util.Set;

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

public class TemporalConfig extends ConfigUtils {
    //FIXME how is this not higher than indexing?
    public static final String USE_MONGO = "sc.useMongo";
    public static final String TEMPORAL_PREDICATES_LIST = "sc.temporal.predicates";
    public static final String TEMPORAL_TABLENAME = "sc.temporal.index";
    public static final String USE_TEMPORAL = "sc.use_temporal";

    public static boolean getUseTemporal(Configuration conf) {
        return conf.getBoolean(USE_TEMPORAL, false);
    }

    public static String getTemporalTableName(Configuration conf) {
        return getIndexTableName(conf, TEMPORAL_TABLENAME, "temporal");
    }

    /**
     * Used for indexing statements about date & time instances and intervals.
     * @param conf
     * @return Set of predicate URI's whose objects should be date time literals.
     */
    public static Set<URI> getTemporalPredicates(final Configuration conf) {
        return getPredicates(conf, TEMPORAL_PREDICATES_LIST);
    }

}
