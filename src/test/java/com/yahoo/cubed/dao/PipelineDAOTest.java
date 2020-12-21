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
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;

/**
 * Test pipeline data access object.
 */
public class PipelineDAOTest {

    private final String schemaName = "schema1";
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
     * Test pipeline setting of fields / filters / etc.
     */
    @Test
    public void testAll() throws IOException {
        Field field1 = new Field();
        field1.setFieldName("field1");
        field1.setFieldType("string");
        field1.setFieldId(1100);
        field1.setSchemaName(schemaName);

        Field field2 = new Field();
        field2.setFieldName("field2");
        field2.setFieldType("string");
        field2.setFieldId(1101);
        field2.setSchemaName(schemaName);

        Field field3 = new Field();
        field3.setFieldName("field3");
        field3.setFieldType("string");
        field3.setFieldId(1102);
        field3.setSchemaName(schemaName);

        DAOFactory.fieldDAO().save(session, field1);
        DAOFactory.fieldDAO().save(session, field2);
        DAOFactory.fieldDAO().save(session, field3);



        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("fieldAlias1");

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("fieldAlias2");

        List<PipelineProjection> projections12 = new ArrayList<>();
        projections12.add(projection1);
        projections12.add(projection2);

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

        Pipeline pipeline1 = new Pipeline();
        pipeline1.setPipelineName("pipeline1");
        pipeline1.setPipelineDescription("pipelineDescription1");
        pipeline1.setPipelineOwner("userName1");
        pipeline1.setProjections(projections12);
        pipeline1.setPipelineFilterJson(Filter.toJson(filter5));
        pipeline1.setPipelineOozieJobId("ooziejobid-11111111");
        pipeline1.setPipelineOozieJobStatus("RUNNING");
        pipeline1.setPipelineSchemaName("schema1");

        DAOFactory.pipelineDAO().save(session, pipeline1);
        long id1 = pipeline1.getPrimaryIdx();

        Assert.assertFalse(id1 == 0);

        Pipeline pipeline2 = DAOFactory.pipelineDAO().fetch(session, id1);

        Assert.assertEquals(pipeline2.getPipelineId(), pipeline1.getPipelineId());
        Assert.assertEquals(pipeline2.getPipelineName(), pipeline1.getPipelineName());
        Assert.assertEquals(pipeline2.getPipelineDescription(), pipeline1.getPipelineDescription());
        Assert.assertEquals(pipeline2.getPipelineOwner(), pipeline1.getPipelineOwner());
        Assert.assertEquals(pipeline2.getProjections().size(), pipeline1.getProjections().size());
        Assert.assertEquals(pipeline2.getPipelineOozieJobId(), pipeline1.getPipelineOozieJobId());
        Assert.assertEquals(pipeline2.getPipelineOozieJobStatus(), pipeline1.getPipelineOozieJobStatus());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node1 = mapper.readTree(pipeline2.getPipelineFilterJson());
        JsonNode node2 = mapper.readTree(pipeline1.getPipelineFilterJson());

        Assert.assertEquals(node1.equals(node2), true);

        for (PipelineProjection projection : pipeline2.getProjections()) {
            Assert.assertTrue(projection.getPipelineProjectionId() == projection1.getPipelineProjectionId()
                    || projection.getPipelineProjectionId() == projection2.getPipelineProjectionId());
            if (projection.getPipelineProjectionId() == projection1.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection1.getAlias());
            }
            if (projection.getPipelineProjectionId() == projection2.getPipelineProjectionId()) {
                Assert.assertEquals(projection.getAlias(), projection2.getAlias());
            }
        }

        Assert.assertEquals(DAOFactory.pipelineDAO().fetchByName(session, "pipeline1").getPipelineDescription(), pipeline1.getPipelineDescription());

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


        Pipeline pipeline3 = new Pipeline();
        pipeline3.setPipelineId(id1);
        pipeline3.setPipelineName("pipeline3");
        pipeline3.setPipelineDescription("pipelineDescription3");
        pipeline3.setPipelineOwner("userName3");
        pipeline3.setProjections(projections345);
        pipeline3.setPipelineFilterJson(Filter.toJson(filter7));
        pipeline3.setPipelineOozieJobId("ooziejobid-333333333333");
        pipeline3.setPipelineOozieJobStatus("KILLED");
        pipeline3.setPipelineSchemaName("schema1");

        DAOFactory.pipelineDAO().update(session, pipeline3);

        List<Pipeline> pipelines = DAOFactory.pipelineDAO().fetchAll(session);
        Assert.assertEquals(pipelines.size(), 1);

        Pipeline pipeline4 = DAOFactory.pipelineDAO().fetch(session, id1);
        Assert.assertEquals(pipeline4.getPipelineDescription(), pipeline3.getPipelineDescription());
        Assert.assertEquals(pipeline4.getProjections().size(), projections345.size());
        Assert.assertEquals(pipeline4.getPipelineOozieJobId(), pipeline3.getPipelineOozieJobId());
        Assert.assertEquals(pipeline4.getPipelineOozieJobStatus(), pipeline3.getPipelineOozieJobStatus());

        JsonNode node3 = mapper.readTree(pipeline3.getPipelineFilterJson());
        JsonNode node4 = mapper.readTree(pipeline4.getPipelineFilterJson());

        Assert.assertEquals(node3.equals(node4), true);

        for (PipelineProjection projection : pipeline4.getProjections()) {
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

        Assert.assertEquals(DAOFactory.pipelineDAO().fetchByName(session, "pipeline3").getPipelineDescription(), pipeline3.getPipelineDescription());

        DAOFactory.pipelineDAO().delete(session, pipeline4);
        Assert.assertEquals(DAOFactory.pipelineDAO().fetchAll(session).size(), 0);
        Assert.assertEquals(DAOFactory.pipelineProjectionDAO().fetchAll(session).size(), 0);

        DAOFactory.fieldDAO().delete(session, field1);
        DAOFactory.fieldDAO().delete(session, field2);
        DAOFactory.fieldDAO().delete(session, field3);

        session.flush();

    }

}
