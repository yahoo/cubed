/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.PipelineProjectionVM;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test pipeline projection value mapping data access object.
 */
public class PipelineProjectionVMDAOTest {
    private String schemaName = "schema1";

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
     * Test pipeline projection value mappings.
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

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("fieldAlias2");
        projection2.setKey("key2");

        List<String> pair1 = new ArrayList<>(Arrays.asList("fieldValue1", "fieldValueMapping1"));
        List<String> pair2 = new ArrayList<>(Arrays.asList("fieldValue2", "fieldValueMapping2"));
        List<List<String>> vas1 = new ArrayList<>();
        vas1.add(pair1);
        vas1.add(pair2);

        projection1.setProjectionVMs(vas1);

        List<String> pair3 = new ArrayList<>(Arrays.asList("fieldValue3", "fieldValueMapping3"));
        List<List<String>> vas2 = new ArrayList<>();
        vas2.add(pair3);
        projection2.setProjectionVMs(vas2);


        List<PipelineProjection> projections12 = new ArrayList<>();
        projections12.add(projection1);
        projections12.add(projection2);

        PipelineProjectionDAO pipelineProjectionDAO = DAOFactory.pipelineProjectionDAO();
        // Triggers the pipelineProjectionVMDAO through pipelineProjectionDAO.save
        pipelineProjectionDAO.save(session, projections12);

        session.flush();

        List<PipelineProjection> pplsit = pipelineProjectionDAO.fetchAll(session);
        Assert.assertEquals(pplsit.size(), 2);

        for (PipelineProjection projection : pplsit) {
            Assert.assertTrue(projection.getField().getFieldId() == fid1 || projection.getField().getFieldId() == fid2);
            List<PipelineProjectionVM> vas = projection.getProjectionVMs();

            if (projection.getField().getFieldId() == fid1) {
                Assert.assertEquals(projection.getField().getFieldName(), field1.getFieldName());
                Assert.assertEquals(vas.size(), 2);
                for (PipelineProjectionVM v: vas) {
                    Assert.assertEquals(v.getPipelineProjectionId(), projection.getPipelineProjectionId());
                    if (v.getFieldValue() == "fieldValue1") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping1");
                    } else if (v.getFieldValue() == "fieldValue2") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping2");
                    }
                }
            }
            if (projection.getField().getFieldId() == fid2) {
                Assert.assertEquals(projection.getField().getFieldName(), field2.getFieldName());
                Assert.assertEquals(vas.size(), 1);
                for (PipelineProjectionVM v: vas) {
                    Assert.assertEquals(v.getPipelineProjectionId(), projection.getPipelineProjectionId());
                    Assert.assertEquals(v.getFieldValue(), "fieldValue3");
                    Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping3");
                }
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

        DAOFactory.fieldDAO().save(session, field3);
        long fid3 = field3.getFieldId();
        DAOFactory.fieldDAO().save(session, field4);
        long fid4 = field4.getFieldId();

        PipelineProjection projection3 = new PipelineProjection();
        projection3.setField(field3);
        projection3.setAlias("fieldAlias3");
        projection3.setKey("key3");

        PipelineProjection projection4 = new PipelineProjection();
        projection4.setField(field4);
        projection4.setAlias("fieldAlias4");
        projection4.setKey("key4");

        List<String> pair4 = new ArrayList<>(Arrays.asList("fieldValue4", "fieldValueMapping4"));
        vas1.add(pair4);

        List<String> pair5 = new ArrayList<>(Arrays.asList("fieldValue5", "fieldValueMapping5"));
        List<String> pair6 = new ArrayList<>(Arrays.asList("fieldValue6", "fieldValueMapping6"));

        vas2.add(pair5);
        vas2.add(pair6);

        projection3.setProjectionVMs(vas1);
        projection4.setProjectionVMs(vas2);
        List<PipelineProjection> projections34 = new ArrayList<>();
        projections34.add(projection3);
        projections34.add(projection4);

        pipelineProjectionDAO.update(session, projections12, projections34);

        session.flush();

        pplsit = pipelineProjectionDAO.fetchAll(session);
        Assert.assertEquals(pplsit.size(), 2);

        for (PipelineProjection projection : pplsit) {
            Assert.assertTrue(projection.getField().getFieldId() == fid3 || projection.getField().getFieldId() == fid4);
            List<PipelineProjectionVM> vas = projection.getProjectionVMs();

            if (projection.getField().getFieldId() == fid3) {
                Assert.assertEquals(projection.getField().getFieldName(), field3.getFieldName());
                Assert.assertEquals(vas.size(), 3);
                for (PipelineProjectionVM v: vas) {
                    Assert.assertEquals(v.getPipelineProjectionId(), projection.getPipelineProjectionId());
                    if (v.getFieldValue() == "fieldValue1") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping1");
                    } else if (v.getFieldValue() == "fieldValue2") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping2");
                    } else if (v.getFieldValue() == "fieldValue4") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping4");
                    }
                }
            }
            if (projection.getField().getFieldId() == fid4) {
                Assert.assertEquals(projection.getField().getFieldName(), field4.getFieldName());
                Assert.assertEquals(vas.size(), 3);
                for (PipelineProjectionVM v: vas) {
                    Assert.assertEquals(v.getPipelineProjectionId(), projection.getPipelineProjectionId());
                    if (v.getFieldValue() == "fieldValue3") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping3");
                    } else if (v.getFieldValue() == "fieldValue5") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping5");
                    } else if (v.getFieldValue() == "fieldValue6") {
                        Assert.assertEquals(v.getFieldValueMapping(), "fieldValueMapping6");
                    }
                }
            }

        }
        pipelineProjectionDAO.delete(session, projections34);

        DAOFactory.fieldDAO().delete(session, field1);
        DAOFactory.fieldDAO().delete(session, field2);
        DAOFactory.fieldDAO().delete(session, field3);
        DAOFactory.fieldDAO().delete(session, field4);

        session.flush();

        Assert.assertEquals(pipelineProjectionDAO.fetchAll(session).size(), 0);

        PipelineProjectionVMDAO pipelineProjectionVMDAO = DAOFactory.pipelineProjectionVMDAO();
        Assert.assertEquals(pipelineProjectionVMDAO.fetchAll(session).size(), 0);
    }

}
