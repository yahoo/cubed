/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.json.filter.LogicalRule;
import com.yahoo.cubed.json.filter.RelationalRule;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;

/**
 * Test pipeline data access object.
 */
public class FunnelGroupDAOTest {

    private static Session session;

    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        session = HibernateSessionFactoryManager.getSessionFactory().openSession();
    }

    /**
     * Close database.
     */
    @AfterClass
    public static void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }

    /**
     * Test funnel group setting of fields / filters / etc.
     */
    @Test
    public void testAll() throws IOException {

        // Create fields and projections based on the fields.
        Field[] fields = createThreeTestFields();
        Field field1 = fields[0];
        Field field2 = fields[1];
        Field field3 = fields[2];

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("fieldAlias1");

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("fieldAlias2");

        List<PipelineProjection> projections12 = new ArrayList<>();
        projections12.add(projection1);
        projections12.add(projection2);

        // Create filters and filter groups.
        RelationalRule[] filters = createThreeTestFilters(field1, field2, field3);
        RelationalRule filter1 = filters[0];
        RelationalRule filter2 = filters[1];
        RelationalRule filter3 = filters[2];

        List<Filter> filters1 = new ArrayList<>();
        filters1.add(filter1);
        filters1.add(filter2);

        LogicalRule filter4 = new LogicalRule();
        filter4.setCondition("AND");
        filter4.setRules(filters1);

        List<Filter> filters2 = new ArrayList<>();
        filters2.add(filter3);
        filters2.add(filter4);

        LogicalRule filter5 = new LogicalRule();
        filter5.setCondition("OR");
        filter4.setRules(filters2);

        // Create funnel groups.
        FunnelGroup funnelGroup1 = new FunnelGroup();
        funnelGroup1.setFunnelGroupName("funnelGroup1");
        funnelGroup1.setFunnelGroupDescription("funnelGroupDescription1");
        funnelGroup1.setFunnelGroupOwner("userName1");
        funnelGroup1.setProjections(projections12);
        funnelGroup1.setFunnelGroupFilterJson(Filter.toJson(filter5));
        funnelGroup1.setFunnelGroupOozieJobId("ooziejobid-11111111");
        funnelGroup1.setFunnelGroupOozieJobStatus("RUNNING");
        funnelGroup1.setFunnelGroupSchemaName("schema1");

        DAOFactory.funnelGroupDAO().save(session, funnelGroup1);
        long id1 = funnelGroup1.getPrimaryIdx();

        Assert.assertNotEquals(id1, 0);

        FunnelGroup funnelGroup2 = DAOFactory.funnelGroupDAO().fetch(session, id1);
        Assert.assertEquals(funnelGroup2.getFunnelGroupId(), funnelGroup1.getFunnelGroupId());
        Assert.assertEquals(funnelGroup2.getFunnelGroupName(), funnelGroup1.getFunnelGroupName());
        Assert.assertEquals(funnelGroup2.getFunnelGroupDescription(), funnelGroup1.getFunnelGroupDescription());
        Assert.assertEquals(funnelGroup2.getFunnelGroupOwner(), funnelGroup1.getFunnelGroupOwner());
        Assert.assertEquals(funnelGroup2.getProjections().size(), funnelGroup1.getProjections().size());
        Assert.assertEquals(funnelGroup2.getFunnelGroupOozieJobId(), funnelGroup1.getFunnelGroupOozieJobId());
        Assert.assertEquals(funnelGroup2.getFunnelGroupOozieJobStatus(), funnelGroup1.getFunnelGroupOozieJobStatus());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node1 = mapper.readTree(funnelGroup1.getFunnelGroupFilterJson());
        JsonNode node2 = mapper.readTree(funnelGroup2.getFunnelGroupFilterJson());

        Assert.assertEquals(node1, node2);

        for (PipelineProjection projection : funnelGroup2.getProjections()) {
            Assert.assertTrue(projection.getPipelineProjectionId() == projection1.getPipelineProjectionId()
                    || projection.getPipelineProjectionId() == projection2.getPipelineProjectionId());
            if (projection.getPipelineProjectionId() == projection1.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection1.getAlias());
            }
            if (projection.getPipelineProjectionId() == projection2.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection2.getAlias());
            }
        }

        Assert.assertEquals(DAOFactory.funnelGroupDAO().fetchByName(session, "funnelGroup1").getFunnelGroupDescription(), funnelGroup1.getFunnelGroupDescription());

        PipelineProjection projection3 = new PipelineProjection();
        projection3.setField(field1);
        projection3.setAlias("fieldAlias3");

        PipelineProjection projection4 = new PipelineProjection();
        projection4.setField(field2);
        projection4.setAlias("fieldAlias4");

        PipelineProjection projection5 = new PipelineProjection();
        projection5.setField(field3);
        projection5.setAlias("fieldAlias5");

        List<PipelineProjection> projections345 = new ArrayList<>();
        projections345.add(projection3);
        projections345.add(projection4);
        projections345.add(projection5);

        RelationalRule filter6 = new RelationalRule();
        filter6.setId(String.valueOf(field3.getFieldId()));
        filter6.setField(field3.getFieldName());
        filter6.setType("string");
        filter6.setOperator("operator6");
        filter6.setValue("v6");

        LogicalRule filter7 = new LogicalRule();
        filter7.setCondition("AND");

        List<Filter> filters3 = new ArrayList<>();
        filters3.add(filter6);
        filter7.setRules(filters3);

        FunnelGroup funnelGroup3 = new FunnelGroup();
        funnelGroup3.setFunnelGroupId(id1);
        funnelGroup3.setFunnelGroupName("funnelGroup3");
        funnelGroup3.setFunnelGroupDescription("pipelineDescription3");
        funnelGroup3.setFunnelGroupOwner("userName3");
        funnelGroup3.setProjections(projections345);
        funnelGroup3.setFunnelGroupFilterJson(Filter.toJson(filter7));
        funnelGroup3.setFunnelGroupOozieJobId("ooziejobid-333333333333");
        funnelGroup3.setFunnelGroupOozieJobStatus("KILLED");
        funnelGroup3.setFunnelGroupSchemaName("schema1");

        DAOFactory.funnelGroupDAO().update(session, funnelGroup3);

        List<FunnelGroup> groups = DAOFactory.funnelGroupDAO().fetchAll(session);
        Assert.assertEquals(groups.size(), 1);

        FunnelGroup funnelGroup4 = DAOFactory.funnelGroupDAO().fetch(session, id1);
        Assert.assertEquals(funnelGroup4.getFunnelGroupDescription(), funnelGroup3.getFunnelGroupDescription());
        Assert.assertEquals(funnelGroup4.getProjections().size(), projections345.size());
        Assert.assertEquals(funnelGroup4.getFunnelGroupOozieJobId(), funnelGroup3.getFunnelGroupOozieJobId());
        Assert.assertEquals(funnelGroup4.getFunnelGroupOozieJobStatus(), funnelGroup3.getFunnelGroupOozieJobStatus());

        JsonNode node3 = mapper.readTree(funnelGroup3.getFunnelGroupFilterJson());
        JsonNode node4 = mapper.readTree(funnelGroup4.getFunnelGroupFilterJson());

        Assert.assertEquals(node3, node4);

        for (PipelineProjection projection : funnelGroup4.getProjections()) {
            Assert.assertTrue(projection.getPipelineProjectionId() == projection3.getPipelineProjectionId()
                    || projection.getPipelineProjectionId() == projection4.getPipelineProjectionId()
                    || projection.getPipelineProjectionId() == projection5.getPipelineProjectionId());
            if (projection.getPipelineProjectionId() == projection3.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection3.getAlias());
            }
            if (projection.getPipelineProjectionId() == projection4.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection4.getAlias());
            }
            if (projection.getPipelineProjectionId() == projection5.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection5.getAlias());
            }
        }

        Assert.assertEquals(DAOFactory.funnelGroupDAO().fetchByName(session, "funnelGroup3").getFunnelGroupDescription(), funnelGroup3.getFunnelGroupDescription());

        DAOFactory.funnelGroupDAO().delete(session, funnelGroup4);
        Assert.assertEquals(DAOFactory.funnelGroupDAO().fetchAll(session).size(), 0);
        Assert.assertEquals(DAOFactory.pipelineProjectionDAO().fetchAll(session).size(), 0);

        DAOFactory.fieldDAO().delete(session, field1);
        DAOFactory.fieldDAO().delete(session, field2);
        DAOFactory.fieldDAO().delete(session, field3);

        session.flush();
    }

    /**
     * Create 3 filters used for test.
     */
    private RelationalRule[] createThreeTestFilters(Field field1, Field field2, Field field3) {
        RelationalRule filter1 = new RelationalRule();
        filter1.setId(String.valueOf(field1.getFieldId()));
        filter1.setField(field1.getFieldName());
        filter1.setType("string");
        filter1.setOperator("operator1");
        filter1.setValue("v1");

        RelationalRule filter2 = new RelationalRule();
        filter2.setId(String.valueOf(field2.getFieldId()));
        filter2.setField(field2.getFieldName());
        filter2.setType("string");
        filter2.setOperator("operator2");
        filter2.setValue("v2");

        RelationalRule filter3 = new RelationalRule();
        filter3.setId(String.valueOf(field3.getFieldId()));
        filter3.setField(field3.getFieldName());
        filter3.setType("string");
        filter3.setOperator("operator3");
        filter3.setValue("v3");

        return new RelationalRule[]{filter1, filter2, filter3};
    }

    /**
     * Create 3 fields used for test.
     */
    private Field[] createThreeTestFields() {
        Field field1 = new Field();
        field1.setFieldName("field1");
        field1.setFieldType("string");
        field1.setFieldId(1100);
        field1.setSchemaName("schema1");

        Field field2 = new Field();
        field2.setFieldName("field2");
        field2.setFieldType("string");
        field2.setFieldId(1101);
        field2.setSchemaName("schema1");

        Field field3 = new Field();
        field3.setFieldName("field3");
        field3.setFieldType("string");
        field3.setFieldId(1102);
        field3.setSchemaName("schema1");

        DAOFactory.fieldDAO().save(session, field1);
        DAOFactory.fieldDAO().save(session, field2);
        DAOFactory.fieldDAO().save(session, field3);

        return new Field[]{field1, field2, field3};
    }

}
