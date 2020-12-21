/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;
import com.yahoo.cubed.util.Aggregation;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test pipeline projection data access object.
 */
public class PipelineProjectionDAOTest {
    private final String schemaName = "schema1";

    private static Session session;

    /**
     * Setup the database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        session = HibernateSessionFactoryManager.getSessionFactory().openSession();
    }

    /**
     * Close the database.
     */
    @AfterClass
    public static void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }

    /**
     * Test pipeline projections.
     */
    @Test
    public void testAll() {

        Field field1 = new Field();
        field1.setFieldName("field1");
        field1.setFieldType("string");
        field1.setFieldId(1101);
        field1.setSchemaName(schemaName);

        Field field2 = new Field();
        field2.setFieldName("field2");
        field2.setFieldType("string");
        field2.setFieldId(1102);
        field2.setSchemaName(schemaName);

        DAOFactory.fieldDAO().save(session, field1);
        long fid1 = field1.getFieldId();
        DAOFactory.fieldDAO().save(session, field2);
        long fid2 = field2.getFieldId();

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("fieldAlias1");
        projection1.setKey("key1");
        projection1.setAggregation(Aggregation.COUNT);

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("fieldAlias2");
        projection2.setKey("key2");

        List<PipelineProjection> projections12 = new ArrayList<>();
        projections12.add(projection1);
        projections12.add(projection2);

        PipelineProjectionDAO pipelineProjectionDAO = DAOFactory.pipelineProjectionDAO();
        pipelineProjectionDAO.save(session, projections12);

        session.flush();

        Assert.assertEquals(pipelineProjectionDAO.fetchAll(session).size(), 2);

        for (PipelineProjection projection : pipelineProjectionDAO.fetchAll(session)) {
            Assert.assertTrue(projection.getField().getFieldId() == fid1 || projection.getField().getFieldId() == fid2);
            if (projection.getField().getFieldId() == fid1) {
                Assert.assertEquals(projection.getField().getFieldName(), field1.getFieldName());
            }
            if (projection.getField().getFieldId() == fid2) {
                Assert.assertEquals(projection.getField().getFieldName(), field2.getFieldName());
            }
        }

        Field field3 = new Field();
        field3.setFieldName("field3");
        field3.setFieldType("string");
        field3.setFieldId(1103);
        field3.setSchemaName(schemaName);

        Field field4 = new Field();
        field4.setFieldName("field4");
        field4.setFieldType("string");
        field4.setFieldId(1104);
        field4.setSchemaName(schemaName);

        Field field5 = new Field();
        field5.setFieldName("field5");
        field5.setFieldType("string");
        field5.setFieldId(1105);
        field5.setSchemaName(schemaName);

        DAOFactory.fieldDAO().save(session, field3);
        long fid3 = field3.getFieldId();
        DAOFactory.fieldDAO().save(session, field4);
        long fid4 = field4.getFieldId();
        DAOFactory.fieldDAO().save(session, field5);
        long fid5 = field5.getFieldId();

        PipelineProjection projection3 = new PipelineProjection();
        projection3.setField(field3);
        projection3.setAlias("fieldAlias3");
        projection3.setKey("key3");
        projection3.setAggregation(Aggregation.COUNT_DISTINCT);

        PipelineProjection projection4 = new PipelineProjection();
        projection4.setField(field4);
        projection4.setAlias("fieldAlias4");

        PipelineProjection projection5 = new PipelineProjection();
        projection5.setField(field5);
        projection5.setAlias("fieldAlias5");
        projection5.setKey("key5");

        List<PipelineProjection> projections345 = new ArrayList<>();
        projections345.add(projection3);
        projections345.add(projection4);
        projections345.add(projection5);

        pipelineProjectionDAO.update(session, projections12, projections345);

        session.flush();

        Assert.assertEquals(pipelineProjectionDAO.fetchAll(session).size(), 3);

        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection3.getPipelineProjectionId()).getAlias(), projection3.getAlias());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection4.getPipelineProjectionId()).getAlias(), projection4.getAlias());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection5.getPipelineProjectionId()).getAlias(), projection5.getAlias());

        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection3.getPipelineProjectionId()).getKey(), projection3.getKey());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection4.getPipelineProjectionId()).getKey(), projection4.getKey());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection5.getPipelineProjectionId()).getKey(), projection5.getKey());

        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection3.getPipelineProjectionId()).getAggregationName(), projection3.getAggregationName());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection4.getPipelineProjectionId()).getAggregationName(), projection4.getAggregationName());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection5.getPipelineProjectionId()).getAggregationName(), projection5.getAggregationName());

        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection3.getPipelineProjectionId()).getField().getFieldId(), fid3);
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection4.getPipelineProjectionId()).getField().getFieldId(), fid4);
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection5.getPipelineProjectionId()).getField().getFieldId(), fid5);

        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection3.getPipelineProjectionId()).getField().getFieldName(), field3.getFieldName());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection4.getPipelineProjectionId()).getField().getFieldName(), field4.getFieldName());
        Assert.assertEquals(pipelineProjectionDAO.fetch(session, projection5.getPipelineProjectionId()).getField().getFieldName(), field5.getFieldName());


        pipelineProjectionDAO.delete(session, projections345);

        DAOFactory.fieldDAO().delete(session, field1);
        DAOFactory.fieldDAO().delete(session, field2);
        DAOFactory.fieldDAO().delete(session, field3);
        DAOFactory.fieldDAO().delete(session, field4);
        DAOFactory.fieldDAO().delete(session, field5);

        session.flush();

        Assert.assertEquals(pipelineProjectionDAO.fetchAll(session).size(), 0);

    }

}
