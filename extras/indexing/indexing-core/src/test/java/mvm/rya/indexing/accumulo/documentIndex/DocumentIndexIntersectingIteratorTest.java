package mvm.rya.indexing.accumulo.documentIndex;

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

import java.util.Map;

import junit.framework.Assert;


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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryException;

public class DocumentIndexIntersectingIteratorTest {

    
 
    private Connector accCon;
    String tablename = "table";
    

    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException {

        accCon = new MockInstance().getConnector("root", "".getBytes());
        accCon.tableOperations().create(tablename);

    }
    
    
    
    
    
    
    
@Test
    public void testBasicColumnObj() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                m.put(new Text("cf"), new Text(null + "\u0000" + "obj" + "\u0000" + "cq"), new Value(new byte[0]));
                m.put(new Text("cF"), new Text(null + "\u0000" +"obj" + "\u0000" + "cQ"), new Value(new byte[0]));

                if (i == 30 || i == 60) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"obj" + "\u0000" + "CQ"), new Value(new byte[0]));
                }

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq" ));
            TextColumn tc2 = new TextColumn(new Text("cF"), new Text("obj" + "\u0000" + "cQ" ));
            TextColumn tc3 = new TextColumn(new Text("CF"), new Text("obj" + "\u0000" + "CQ" ));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 1****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(2, results);

            
            

    }
    
    
    
    
    
    
    
@Test
    public void testBasicColumnObjPrefix()  throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                m.put(new Text("cf"), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" ), new Value(new byte[0]));
                m.put(new Text("cF"), new Text(null + "\u0000" +"obj" + "\u0000" + "cQ"), new Value(new byte[0]));

                if (i == 30 || i == 60) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"obj" + "\u0000" + "CQ" ), new Value(new byte[0]));
                }
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq"));
            TextColumn tc2 = new TextColumn(new Text("cF"), new Text("obj" + "\u0000" + "cQ"));
            TextColumn tc3 = new TextColumn(new Text("CF"), new Text("obj"));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;
            
            tc3.setIsPrefix(true);

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 2****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(2, results);

            
            

    }
    
    
    
    
@Test
    public void testBasicColumnSubjObjPrefix() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                m.put(new Text("cf"), new Text(null + "\u0000" +"obj" + "\u0000" + "cq"), new Value(new byte[0]));
                m.put(new Text("cF"), new Text(null + "\u0000" +"obj" + "\u0000" + "cQ"), new Value(new byte[0]));

                if (i == 30 ) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"obj" + "\u0000" + "CQ"), new Value(new byte[0]));
                }
                
                if  (i == 60) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"subj" + "\u0000" + "CQ"), new Value(new byte[0]));
                }
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq" ));
            TextColumn tc2 = new TextColumn(new Text("cF"), new Text("obj" + "\u0000" + "cQ"));
            TextColumn tc3 = new TextColumn(new Text("CF"), new Text("subj"));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;
            
            tc3.setIsPrefix(true);

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 3****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(1, results);

            
            

    }
    
    
    
    
@Test
    public void testOneHundredColumnSubjObj() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                
                for(int j= 0; j < 100; j++) {
                    m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
                }
                
                if (i == 30 ) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 100), new Value(new byte[0]));
                }
                
                if  (i == 60) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 100), new Value(new byte[0]));
                }
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
            TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
            TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("obj" + "\u0000" + "cq" + 100));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 4****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(1, results);

            
            

    }
    
    
    
    
@Test
    public void testOneHundredColumnObjPrefix() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                
                for(int j= 0; j < 100; j++) {
                    m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j ), new Value(new byte[0]));
                }
                
                if (i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                }
                
                
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
            TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
            TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("obj"));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;
            
            tc3.setIsPrefix(true);

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 5****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(4, results);

            
            

    }
    
    
    
    
    
    
    
@Test
    public void testOneHundredColumnMultipleEntriesPerSubject() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                
                for(int j= 0; j < 100; j++) {
                    m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j ), new Value(new byte[0]));
                }
                
                if (i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i + 1)), new Value(new byte[0]));
                }
                
                
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20 ));
            TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
            TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("obj"));

            tc3.setIsPrefix(true);
            
            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 6****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(8, results);

            
            

    }
    
    
    
    

@Test
public void testOneHundredColumnSubjObjPrefix() throws Exception {

    BatchWriter bw = null;

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

        for (int i = 0; i < 100; i++) {

            Mutation m = new Mutation(new Text("" + i));
            
            for(int j= 0; j < 100; j++) {
                m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
            }
            
            if (i == 30 || i == 60 || i == 90 || i == 99) {
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + (100 + i + 1)), new Value(new byte[0]));
            }
            
            
            
            
            

            bw.addMutation(m);

        }
        
        DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
        TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
        TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
        TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("subj"));

        tc3.setIsPrefix(true);
        
        TextColumn[] tc = new TextColumn[3];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        dii.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 7****************************");
        for (Map.Entry<Key, Value> e : scan) {
            System.out.println(e);
            results++;
        }
        
        
        Assert.assertEquals(4, results);

        
        

}






@Test
public void testOneHundredColumnSubjObjPrefixFourTerms() throws Exception {

    BatchWriter bw = null;

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

        for (int i = 0; i < 100; i++) {

            Mutation m = new Mutation(new Text("" + i));
            
            for(int j= 0; j < 100; j++) {
                m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
            }
            
            if (i == 30 || i == 60 || i == 90 || i == 99) {
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + (100 + i + 1)), new Value(new byte[0]));
            }
            
            
            
            
            

            bw.addMutation(m);

        }
        
        DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
        TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
        TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
        TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("subj"));
        TextColumn tc4 = new TextColumn(new Text("cf" + 100), new Text("obj"));

        tc3.setIsPrefix(true);
        tc4.setIsPrefix(true);
        
        TextColumn[] tc = new TextColumn[4];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;
        tc[3] = tc4;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        dii.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 8****************************");
        for (Map.Entry<Key, Value> e : scan) {
            System.out.println(e);
            results++;
        }
        
        
        Assert.assertEquals(4, results);

        
        

}






//@Test
public void testOneHundredColumnSameCf() throws Exception {

    BatchWriter bw = null;

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

        for (int i = 0; i < 100; i++) {

            Mutation m = new Mutation(new Text("" + i));
            
            for(int j= 0; j < 100; j++) {
                m.put(new Text("cf"), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
            }
             
            

            bw.addMutation(m);

        }
        
        DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
        TextColumn tc1 = new TextColumn(new Text("cf" ), new Text("obj" + "\u0000" + "cq" + 20));
        TextColumn tc2 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq" + 50));
        TextColumn tc3 = new TextColumn(new Text("cf" ), new Text("obj" + "\u0000" + "cq" + 80));
        TextColumn tc4 = new TextColumn(new Text("cf"), new Text("obj"));

        tc4.setIsPrefix(true);
        
        TextColumn[] tc = new TextColumn[4];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;
        tc[3] = tc4;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        dii.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 9****************************");
        for (Map.Entry<Key, Value> e : scan) {
            //System.out.println(e);
            results++;
        }
        
        
        Assert.assertEquals(10000, results);

        
        

}





@Test
public void testGeneralStarQuery() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 ) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
      DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 3));

      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      dii.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 10****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(2, results);

      
      

}







@Test
public void testGeneralStarQuerySubjPrefix() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
      DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      dii.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 11****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(4, results);

      
      

}





@Test
public void testGeneralStarQueryMultipleSubjPrefix() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 12****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(12, results);

      
      

}




@Test
public void testFixedRangeColumnValidateExact() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 3));
      TextColumn tc4 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 4));
      TextColumn tc5 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 5));


      
      TextColumn[] tc = new TextColumn[5];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
      tc[3] = tc4;
      tc[4] = tc5;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.setRange(Range.exact(new Text("" + 30)));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 14****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(1, results);

      
      

}






@Test
public void testLubmLikeTest() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m1 = new Mutation(new Text("ProfessorA" + i));
                Mutation m2= new Mutation(new Text("ProfessorB" + i));
    
                m1.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://www.University" + i + ".edu"), new Value(new byte[0]));
                m2.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://www.University" + i + ".edu"), new Value(new byte[0]));
                m1.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://Course" + i), new Value(new byte[0]));
                m2.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://Course" + i), new Value(new byte[0]));
            
                
                bw.addMutation(m1);
                bw.addMutation(m2);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom" ), 
              new Text("object" + "\u0000" + "http://www.University" + 30 + ".edu"));
      TextColumn tc2 = new TextColumn(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"), 
              new Text("object" + "\u0000" + "http://Course" + 30));
      


      
      TextColumn[] tc = new TextColumn[2];
      tc[0] = tc1;
      tc[1] = tc2;
     

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 15****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(2, results);

      
      

}

















@Test
public void testFixedRangeColumnValidateSubjPrefix() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.setRange(Range.exact(new Text("" + 30)));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 13****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(3, results);

      
      

}





//@Test
//public void testRangeBound() {
//
//  BatchWriter bw = null;
//
//  try {
//    
//
//     
//      
//      for (int i = 0; i < 100; i++) {
//
//                Mutation m = new Mutation(new Text("" + i));
//    
//                m.put(new Text("cf" + 1), new Text("obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
//                m.put(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
//                m.put(new Text("cf" + 1), new Text("subj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
//                m.put(new Text("cf" + 2), new Text("subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
//                
//                
//
//                if(i == 30 || i == 60 || i == 90 || i == 99) {
//                    m.put(new Text("cf" + 3), new Text("obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("obj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
//                }
//                
//                bw.addMutation(m);
//
//      }
//     
//    
//      
//     Text cf = new Text("cf" + 3); 
//     Text cq = new Text("obj" + "\u0000" + "cq" + 3);
//    
//      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
//      scan.fetchColumn(cf, cq );
//      scan.setRange(new Range());
//      
//
//      int results = 0;
//      System.out.println("************************Test 14****************************");
//      for (Map.Entry<Key, Value> e : scan) {
//          System.out.println(e);
//          results++;
//      }
//      
//      
//      
//
//      
//      
//  } catch (MutationsRejectedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//  } catch (TableNotFoundException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//  }
//
//}



  

@Test
public void testContext1() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" + "obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context1");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 14****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(8, results);

      
      

}







@Test
public void testContext2() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context3" + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context2");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 15****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(0, results);

      
      

}








@Test
public void testContext3() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 1 + "\u0000" + "context1"), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2 + "\u0000" + "context1"), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 1 + "\u0000" + "context2"), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 2 + "\u0000" + "context2"), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context3" + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context2");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 16****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(4, results);

      
      

}









@Test
public void testContext4() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
     

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 17****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(8, results);

      
      


}





@Test
public void testContext5() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" + "obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" + "obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000"  + "obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
     

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 18****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(12, results);

      
      

}






@Test
public void testContext6() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("row" + i));
               
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "subj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "subj" + "\u0000" + "cq" + i), new Value(new byte[0]));
     
                
                bw.addMutation(m);
                

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" ));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("subj" ));
      

      tc1.setIsPrefix(true);
      tc2.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[2];
      tc[0] = tc1;
      tc[1] = tc2;
      
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context2");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 19****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(100, results);

      
      

}



@Test
public void testContext7() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 10; i++) {

                Mutation m = new Mutation(new Text("row" + i));
               
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 100 + i), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 100+i), new Value(new byte[0]));
     
                
                bw.addMutation(m);
                

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" ));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" ));
      

      tc1.setIsPrefix(true);
      tc2.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[2];
      tc[0] = tc1;
      tc[1] = tc2;
      
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 20****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(40, results);

      
      

}







}
