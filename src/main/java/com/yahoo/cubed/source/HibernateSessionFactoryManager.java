/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.PipelineProjectionVM;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.model.FunnelGroup;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

/**
 * Factory manager for Hibernate sessions.
 */
public class HibernateSessionFactoryManager {
    private static SessionFactory sessionFactory;

    /**
     * Get a new session factory.
     */
    public static SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            Configuration cfg = new Configuration();
            // add class
            cfg.addPackage("com.yahoo.cubed.model");
            cfg.addAnnotatedClass(Field.class);
            cfg.addAnnotatedClass(Pipeline.class);
            cfg.addAnnotatedClass(PipelineProjection.class);
            cfg.addAnnotatedClass(PipelineProjectionVM.class);
            cfg.addAnnotatedClass(FieldKey.class);
            cfg.addAnnotatedClass(Schema.class);
            cfg.addAnnotatedClass(FunnelGroup.class);

            // add connection
            cfg.setProperty("hibernate.connection.driver_class", ConfigurationLoader.getProperty(ConfigurationLoader.DRIVER));
            cfg.setProperty("hibernate.connection.url", ConfigurationLoader.getProperty(ConfigurationLoader.DATABASEURL));
            cfg.setProperty("hibernate.connection.username", ConfigurationLoader.getProperty(ConfigurationLoader.USERNAME));
            cfg.setProperty("hibernate.connection.password", ConfigurationLoader.getProperty(ConfigurationLoader.PASSWORD));
            cfg.setProperty("hibernate.dialect", ConfigurationLoader.getProperty(ConfigurationLoader.DIALECT));
            cfg.setProperty("hibernate.connection.autoReconnect", "true");

            ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().applySettings(cfg.getProperties()).buildServiceRegistry();

            sessionFactory = cfg.buildSessionFactory(serviceRegistry);
        }
        return sessionFactory;
    }
}
