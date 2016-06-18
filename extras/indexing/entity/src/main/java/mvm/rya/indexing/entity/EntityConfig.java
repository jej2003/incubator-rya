package mvm.rya.indexing.entity;

import mvm.rya.indexing.ConfigUtils;
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

public class EntityConfig extends ConfigUtils {

    public static final String USE_ENTITY = "sc.use_entity";
    public static final String ENTITY_TABLENAME = "sc.entity.index";

    public static boolean getUseEntity(Configuration conf) {
        return conf.getBoolean(USE_ENTITY, false);
    }

    public static String getEntityTableName(Configuration conf) {
        return getIndexTableName(conf, ENTITY_TABLENAME, "entity");
    }
}
