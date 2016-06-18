package mvm.rya.indexing.geospatial;

import mvm.rya.indexing.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.URI;

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

public class GeoConfig extends ConfigUtils {
    public static final String USE_GEO = "sc.use_geo";
    public static final String GEO_TABLENAME = "sc.geo.table";
    public static final String GEO_NUM_PARTITIONS = "sc.geo.numPartitions";
    public static final String GEO_PREDICATES_LIST = "sc.geo.predicates";

    public static boolean getUseGeo(Configuration conf) {
        return conf.getBoolean(USE_GEO, false);
    }

    public static String getGeoTablename(final Configuration conf) {
        return getIndexTableName(conf, GEO_TABLENAME, "geo");
    }

    public static int getGeoNumPartitions(final Configuration conf) {
        return conf.getInt(GEO_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static Set<URI> getGeoPredicates(final Configuration conf) {
        return getPredicates(conf, GEO_PREDICATES_LIST);
    }

}
