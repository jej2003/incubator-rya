package mvm.rya.indexing.pcj;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mvm.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import mvm.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import mvm.rya.indexing.IndexPlanValidator.ThreshholdPlanSelector;
import mvm.rya.indexing.IndexPlanValidator.TupleExecutionPlanGenerator;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import mvn.rya.indexing.pcj.matching.PCJOptimizer;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.beust.jcommander.internal.Lists;

public class ThreshholdPlanSelectorTest {

    @Test
    public void testCost3() throws Exception {

        String q1 = ""//
                + "SELECT ?f ?m ?d ?e ?l ?c " //
                + "{" //
                + "  Filter(?f > \"5\")." //
                + "  Filter(?e > \"6\")." //
                + "  ?f a ?m ."//
                + "  ?e a ?l ."//
                + "  ?d <uri:talksTo> ?f . "//
                + "  ?c <uri:talksTo> ?e . "//
                + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
                + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
                + "}";//

        String q2 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//

        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = parser.parseQuery(q1, null);
        ParsedQuery pq2 = parser.parseQuery(q2, null);

        SimpleExternalTupleSet sep = new SimpleExternalTupleSet(
                (Projection) pq2.getTupleExpr());
        List<ExternalTupleSet> eList = Lists.newArrayList();
        eList.add(sep);

        final TupleExpr te = pq1.getTupleExpr().clone();
        final PCJOptimizer pcj = new PCJOptimizer(eList, false);
        pcj.optimize(te, null, null);

        ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
                pq1.getTupleExpr());
        double cost = tps.getCost(te, .4, .3, .3);
        Assert.assertEquals(.575, cost, .0001);

    }

    public static class NodeCollector extends
            QueryModelVisitorBase<RuntimeException> {

        List<QueryModelNode> qNodes = Lists.newArrayList();

        public List<QueryModelNode> getNodes() {
            return qNodes;
        }

        @Override
        public void meetNode(QueryModelNode node) {
            if (node instanceof StatementPattern
                    || node instanceof ExternalTupleSet) {
                qNodes.add(node);
            }
            super.meetNode(node);

        }

    }

}
