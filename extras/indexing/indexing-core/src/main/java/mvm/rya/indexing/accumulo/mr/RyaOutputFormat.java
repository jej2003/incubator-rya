package mvm.rya.indexing.accumulo.mr;

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



import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.index.RyaSecondaryIndexer;
import mvm.rya.api.resolver.RdfToRyaConversions;

import mvm.rya.indexing.ConfigUtils;
import mvm.rya.indexing.accumulo.StatementSerializer;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;

/**
 * Hadoop Map/Reduce class to use Rya and any {@link RyaSecondaryIndexer} as the sink of {@link Statement} data.
 * wrapped in an {@link StatementWritable} objects. This {@link OutputFormat} ignores the Keys and only writes the Values to Rya.
 *
 * The user must specify connection parameters for Rya and any {@link RyaSecondaryIndexer} that is on the classpath and enabled.
 */
public class RyaOutputFormat extends OutputFormat<Writable, StatementWritable> {
    private static final Logger logger = Logger.getLogger(RyaOutputFormat.class);

    private static final String PREFIX = RyaOutputFormat.class.getSimpleName();
    private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();

        getRyaIndexer(conf);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        // copied from AccumuloOutputFormat
        return new NullOutputFormat<Text, Mutation>().getOutputCommitter(context);
    }

    @Override
    public RecordWriter<Writable, StatementWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new RyaRecordWriter(context);
    }

//    private static GeoIndexer getGeoIndexer(Configuration conf) throws IOException {
//        if (!conf.getBoolean(ENABLE_GEO, true)) {
//            return new NullGeoIndexer();
//        }
//
//        GeoMesaGeoIndexer geo = new GeoMesaGeoIndexer();
//        geo.setConf(conf);
//        return geo;
//
//    }
//
//    private static FreeTextIndexer getFreeTextIndexer(Configuration conf) throws IOException {
//        if (!conf.getBoolean(ENABLE_FREETEXT, true)) {
//            return new NullFreeTextIndexer();
//        }
//
//        AccumuloFreeTextIndexer freeText = new AccumuloFreeTextIndexer();
//        freeText.setConf(conf);
//        return freeText;
//
//    }
//
//    private static TemporalIndexer getTemporalIndexer(Configuration conf) throws IOException {
//        if (!conf.getBoolean(ENABLE_TEMPORAL, true)) {
//            return new NullTemporalIndexer();
//        }
//        AccumuloTemporalIndexer temporal = new AccumuloTemporalIndexer();
//        temporal.setConf(conf);
//        return temporal;
//    }

    private static AccumuloRyaDAO getRyaIndexer(Configuration conf) throws IOException {
        try {
            //FIXME why don't we just use the secondary indexers on the DAO?
            AccumuloRyaDAO ryaIndexer = new AccumuloRyaDAO();
            Connector conn = ConfigUtils.getConnector(conf);
            ryaIndexer.setConnector(conn);

            AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();

            String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
            if (tablePrefix != null) {
                ryaConf.setTablePrefix(tablePrefix);
            }
            ryaConf.setDisplayQueryPlan(false);
            ryaIndexer.setConf(ryaConf);
            ryaIndexer.init();
            return ryaIndexer;
        } catch (AccumuloException e) {
            logger.error("Cannot create RyaIndexer", e);
            throw new IOException(e);
        } catch (AccumuloSecurityException e) {
            logger.error("Cannot create RyaIndexer", e);
            throw new IOException(e);
        } catch (RyaDAOException e) {
            logger.error("Cannot create RyaIndexer", e);
            throw new IOException(e);
        }
    }

    public static class RyaRecordWriter extends RecordWriter<Writable, StatementWritable> implements Closeable, Flushable {
        private static final Logger logger = Logger.getLogger(RyaRecordWriter.class);

        Collection<RyaSecondaryIndexer> indexers;
        private AccumuloRyaDAO ryaIndexer;

        private static final long ONE_MEGABYTE = 1024L * 1024L;
        private static final long AVE_STATEMENT_SIZE = 100L;

        private long bufferSizeLimit;
        private long bufferCurrentSize = 0;

        private ArrayList<RyaStatement> buffer;

        public RyaRecordWriter(TaskAttemptContext context) throws IOException {
            this(context.getConfiguration());
        }

        public RyaRecordWriter(Configuration conf) throws IOException {
            // set up the buffer
            bufferSizeLimit = conf.getLong(MAX_MUTATION_BUFFER_SIZE, ONE_MEGABYTE);
            int bufferCapacity = (int) (bufferSizeLimit / AVE_STATEMENT_SIZE);
            buffer = new ArrayList<RyaStatement>(bufferCapacity);

            // set up the indexers
            AccumuloRdfConfiguration configuration = new AccumuloRdfConfiguration(conf);
            ConfigUtils.setIndexers(configuration);
            indexers = ConfigUtils.getSecondaryIndexers(configuration);

            ryaIndexer = getRyaIndexer(conf);

            // update fields used for metrics
            startTime = System.currentTimeMillis();
            lastCommitFinishTime = startTime;
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
        }

        @Override
        public void close() throws IOException {
            close(null);
        }

        @Override
        public void close(TaskAttemptContext paramTaskAttemptContext) throws IOException {
            // close everything. log errors
            try {
                flush();
            } catch (IOException e) {
                logger.error("Error flushing the buffer on RyaOutputFormat Close", e);
            }
            for(RyaSecondaryIndexer indexer : indexers) {
                try {
                    indexer.close();
                } catch (IOException e) {
                    logger.error("Error closing the secondary indexer " + indexer.getClass().getName() + " on RyaOutputFormat Close", e);
                }
            }

            try {
                ryaIndexer.destroy();
            } catch (RyaDAOException e) {
                logger.error("Error closing RyaDAO on RyaOutputFormat Close", e);
            }
        }

        public void write(Statement statement) throws IOException, InterruptedException {
            write(null, new StatementWritable(statement));
        }

        @Override
        public void write(Writable key, StatementWritable value) throws IOException, InterruptedException {
            buffer.add(RdfToRyaConversions.convertStatement(value));

            bufferCurrentSize += StatementSerializer.writeStatement(value).length();

            if (bufferCurrentSize >= bufferSizeLimit) {
                flushBuffer();
            }
        }

        // fields for storing metrics
        private long startTime = 0;
        private long lastCommitFinishTime = 0;
        private long totalCommitRecords = 0;

        private double totalReadDuration = 0;
        private double totalWriteDuration = 0;

        private long commitCount = 0;

        private void flushBuffer() throws IOException {
            totalCommitRecords += buffer.size();
            commitCount++;

            long startCommitTime = System.currentTimeMillis();

            logger.info(String.format("(C-%d) Flushing buffer with %,d objects and %,d bytes", commitCount, buffer.size(),
                    bufferCurrentSize));

            double readingDuration = (startCommitTime - lastCommitFinishTime) / 1000.;
            totalReadDuration += readingDuration;
            double currentReadRate = buffer.size() / readingDuration;
            double totalReadRate = totalCommitRecords / totalReadDuration;

            // Print "reading" metrics
            logger.info(String.format("(C-%d) (Reading) Duration, Current Rate, Total Rate: %.2f %.2f %.2f ", commitCount, readingDuration,
                    currentReadRate, totalReadRate));

            for(RyaSecondaryIndexer indexer : indexers) {
                indexer.storeStatements(buffer);
                indexer.flush();
            }

            // write to rya
            try {
                ryaIndexer.add(buffer.iterator());
            } catch (RyaDAOException e) {
                logger.error("Cannot writing statement to Rya", e);
                throw new IOException(e);
            }

            lastCommitFinishTime = System.currentTimeMillis();

            double writingDuration = (lastCommitFinishTime - startCommitTime) / 1000.;
            totalWriteDuration += writingDuration;
            double currentWriteRate = buffer.size() / writingDuration;
            double totalWriteRate = totalCommitRecords / totalWriteDuration;

            // Print "writing" stats
            logger.info(String.format("(C-%d) (Writing) Duration, Current Rate, Total Rate: %.2f %.2f %.2f ", commitCount, writingDuration,
                    currentWriteRate, totalWriteRate));

            double processDuration = writingDuration + readingDuration;
            double totalProcessDuration = totalWriteDuration + totalReadDuration;
            double currentProcessRate = buffer.size() / processDuration;
            double totalProcessRate = totalCommitRecords / (totalProcessDuration);

            // Print "total" stats
            logger.info(String.format("(C-%d) (Total) Duration, Current Rate, Total Rate: %.2f %.2f %.2f ", commitCount, processDuration,
                    currentProcessRate, totalProcessRate));

            // clear the buffer
            buffer.clear();
            bufferCurrentSize = 0L;
        }
    }
}
