package mvm.rya.indexing.freetext;

import mvm.rya.indexing.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
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

public class FreeTextConfig extends ConfigUtils {
    public static final String FREETEXT_PREDICATES_LIST = "sc.freetext.predicates";
    public static final String FREETEXT_DOC_NUM_PARTITIONS = "sc.freetext.numPartitions.text";
    public static final String FREETEXT_TERM_NUM_PARTITIONS = "sc.freetext.numPartitions.term";

    public static final String FREE_TEXT_QUERY_TERM_LIMIT = "sc.freetext.querytermlimit";

    public static final String FREE_TEXT_DOC_TABLENAME = "sc.freetext.doctable";
    public static final String FREE_TEXT_TERM_TABLENAME = "sc.freetext.termtable";
    public static final String USE_FREETEXT = "sc.use_freetext";

    public static boolean getUseFreeText(Configuration conf) {
        return conf.getBoolean(USE_FREETEXT, false);
    }


    public static Set<URI> getFreeTextPredicates(final Configuration conf) {
        return getPredicates(conf, FREETEXT_PREDICATES_LIST);
    }

    public static String getFreeTextDocTablename(final Configuration conf) {
        return getIndexTableName(conf, FREE_TEXT_DOC_TABLENAME, "freetext");
    }

    public static String getFreeTextTermTablename(final Configuration conf) {
        return getIndexTableName(conf, FREE_TEXT_TERM_TABLENAME, "freetext_term");
    }

    public static int getFreeTextTermLimit(final Configuration conf) {
        return conf.getInt(FREE_TEXT_QUERY_TERM_LIMIT, 100);
    }

    public static Tokenizer getFreeTextTokenizer(final Configuration conf) {
        final Class<? extends Tokenizer> c = conf.getClass(TOKENIZER_CLASS, LuceneTokenizer.class, Tokenizer.class);
        return ReflectionUtils.newInstance(c, conf);
    }
}
