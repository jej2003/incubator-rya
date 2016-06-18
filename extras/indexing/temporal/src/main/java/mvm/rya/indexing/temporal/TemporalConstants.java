package mvm.rya.indexing.temporal;

import mvm.rya.indexing.IndexingFunctionRegistry;
import mvm.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import org.openrdf.model.impl.URIImpl;

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

public class TemporalConstants {
    static {

        String TEMPORAL_NS = "tag:rya-rdf.org,2015:temporal#";

        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"after"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"before"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"equals"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"beforeInterval"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"afterInterval"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"insideInterval"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"hasBeginningInterval"), FUNCTION_TYPE.TEMPORAL);
        IndexingFunctionRegistry.register(new URIImpl(TEMPORAL_NS+"hasEndInterval"), FUNCTION_TYPE.TEMPORAL);

    }

}
