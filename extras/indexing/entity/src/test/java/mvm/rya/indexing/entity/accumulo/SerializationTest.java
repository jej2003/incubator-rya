package mvm.rya.indexing.entity.accumulo;

import com.google.common.primitives.Bytes;
import junit.framework.Assert;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.RyaTableMutationsFactory;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.indexing.accumulo.documentIndex.DocumentIndexIntersectingIterator;
import mvm.rya.indexing.accumulo.documentIndex.TextColumn;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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

public class SerializationTest {

    private Connector accCon;
    String tablename = "table";


    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException {

        accCon = new MockInstance().getConnector("root", "".getBytes());
        accCon.tableOperations().create(tablename);

    }

    @org.junit.Test
    public void testSerialization1() throws Exception {

        BatchWriter bw = null;
        AccumuloRdfConfiguration acc = new AccumuloRdfConfiguration();
        acc.set(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS, EntityCentricIndex.class.getName());
        RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(acc));

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);



        for (int i = 0; i < 20; i++) {


            RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaType(XMLSchema.STRING, "cq1"));
            RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
            RyaStatement rs3 = null;
            RyaStatement rs4 = null;

            if(i == 5 || i == 15) {
                rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                rs4 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.STRING,Integer.toString(i)));
            }



            Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
            for (Mutation m : m1) {
                bw.addMutation(m);
            }
            Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
            for (Mutation m : m2) {
                bw.addMutation(m);
            }
            if (rs3 != null) {
                Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                for (Mutation m : m3) {
                    bw.addMutation(m);
                }
            }
            if (rs4 != null) {
                Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
                for (Mutation m : m4) {
                    bw.addMutation(m);
                }
            }





        }

        String q1 = "" //
                + "SELECT ?X ?Y1 ?Y2 " //
                + "{"//
                +  "?X <uri:cf1> ?Y1 ."//
                +  "?X <uri:cf2> ?Y2 ."//
                +  "?X <uri:cf3> 5 ."//
                +  "}";


        String q2 = "" //
                + "SELECT ?X ?Y1 ?Y2 " //
                + "{"//
                +  "?X <uri:cf1> ?Y1  ."//
                +  "?X <uri:cf2> ?Y2 ."//
                +  "?X <uri:cf3> \"15\" ."//
                +  "}";



        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = parser.parseQuery(q1, null);
        ParsedQuery pq2 = parser.parseQuery(q2, null);

        TupleExpr te1 = pq1.getTupleExpr();
        TupleExpr te2 = pq2.getTupleExpr();

        List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
        List<StatementPattern> spList2 = StatementPatternCollector.process(te2);

        System.out.println(spList1);
        System.out.println(spList2);

        RyaType rt1 = RdfToRyaConversions.convertValue(spList1.get(2).getObjectVar().getValue());
        RyaType rt2 = RdfToRyaConversions.convertValue(spList2.get(2).getObjectVar().getValue());

        RyaURI predURI1 = (RyaURI) RdfToRyaConversions.convertValue(spList1.get(0).getPredicateVar().getValue());
        RyaURI predURI2 = (RyaURI) RdfToRyaConversions.convertValue(spList1.get(1).getPredicateVar().getValue());
        RyaURI predURI3 = (RyaURI) RdfToRyaConversions.convertValue(spList1.get(2).getPredicateVar().getValue());

//            System.out.println("to string" + spList1.get(2).getObjectVar().getValue().stringValue());
//            System.out.println("converted obj" + rt1.getData());
//            System.out.println("equal: " + rt1.getData().equals(spList1.get(2).getObjectVar().getValue().stringValue()));


        System.out.println(rt1);
        System.out.println(rt2);

        RyaContext rc = RyaContext.getInstance();

        byte[][] b1 = rc.serializeType(rt1);
        byte[][] b2 = rc.serializeType(rt2);

        byte[] b3 = Bytes.concat("object".getBytes(), "\u0000".getBytes(), b1[0], b1[1]);
        byte[] b4 = Bytes.concat("object".getBytes(), "\u0000".getBytes(), b2[0], b2[1]);

        System.out.println(new String(b3));
        System.out.println(new String(b4));

        TextColumn tc1 = new TextColumn(new Text(predURI1.getData()), new Text("object"));
        TextColumn tc2 = new TextColumn(new Text(predURI2.getData()), new Text("object"));
        TextColumn tc3 = new TextColumn(new Text(predURI3.getData()), new Text(b3));

        tc1.setIsPrefix(true);
        tc2.setIsPrefix(true);

        TextColumn[] tc = new TextColumn[3];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));

        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 21****************************");
        Text t = null;
        for (Map.Entry<Key, Value> e : scan) {
            t = e.getKey().getColumnQualifier();
            System.out.println(e);
            results++;
        }

        Assert.assertEquals(1, results);
        String [] s = t.toString().split("\u001D" + "\u001E");
        String[] s1 = s[2].split("\u0000");
        RyaType rt = rc.deserialize(s1[2].getBytes());
        System.out.println("Rya type is " + rt);
        org.openrdf.model.Value v = RyaToRdfConversions.convertValue(rt);
        Assert.assertTrue(v.equals(spList1.get(2).getObjectVar().getValue()));

        tc1 = new TextColumn(new Text(predURI1.getData()), new Text("object"));
        tc2 = new TextColumn(new Text(predURI2.getData()), new Text("object"));
        tc3 = new TextColumn(new Text(predURI3.getData()), new Text(b4));

        tc1.setIsPrefix(true);
        tc2.setIsPrefix(true);

        tc = new TextColumn[3];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;

        is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

        scan = accCon.createScanner(tablename, new Authorizations("auths"));

        scan.addScanIterator(is);

        results = 0;
        System.out.println("************************Test 21****************************");

        for (Map.Entry<Key, Value> e : scan) {
            t = e.getKey().getColumnQualifier();
            System.out.println(e);
            results++;
        }

        Assert.assertEquals(1, results);
        s = t.toString().split("\u001D" + "\u001E");
        s1 = s[2].split("\u0000");
        rt = rc.deserialize(s1[2].getBytes());
        System.out.println("Rya type is " + rt);
        v = RyaToRdfConversions.convertValue(rt);
        Assert.assertTrue(v.equals(spList2.get(2).getObjectVar().getValue()));




    }
}
