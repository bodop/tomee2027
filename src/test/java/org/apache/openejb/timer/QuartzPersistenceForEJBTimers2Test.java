/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.openejb.timer;

import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerService;

import org.apache.openejb.config.AppModule;
import org.apache.openejb.config.EjbModule;
import org.apache.openejb.jee.EjbJar;
import org.apache.openejb.jee.SessionBean;
import org.apache.openejb.jee.SessionType;
import org.apache.openejb.junit.ApplicationComposer;
import org.apache.openejb.quartz.impl.jdbcjobstore.HSQLDBDelegate;
import org.apache.openejb.quartz.impl.jdbcjobstore.JobStoreCMT;
import org.apache.openejb.quartz.simpl.SimpleThreadPool;
import org.apache.openejb.testing.Configuration;
import org.apache.openejb.testing.Module;
import org.apache.openejb.testng.PropertiesBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(ApplicationComposer.class)
public class QuartzPersistenceForEJBTimers2Test {

    @EJB
    private MyTimedEjb bean;

    @Test
    public void doTest() {
        bean.newTimer();
        long l=0;
        do {
            long v;
            for (int loop=0; true; loop++) {
                assertTrue("Waited too long for timeout",loop<20);
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    // no-op
                }
                v=bean.value();
                if (l!=v) {
                    l=v;
                    break;
                }
            }
        } while (l<=100);

    }

    @Module
    public AppModule application() {
        final EjbModule ejbModule = new EjbModule(new EjbJar());
        ejbModule.getEjbJar().addEnterpriseBean(new SessionBean("MyTimedEjb",MyTimedEjb.class.getName(),SessionType.STATELESS).localBean());

        final Properties quartzConfig = new PropertiesBuilder()
            .p("org.apache.openejb.quartz.scheduler.instanceName", "TestScheduler")
            .p("org.apache.openejb.quartz.scheduler.instanceId", "AUTO")
            .p("org.apache.openejb.quartz.threadPool.class", SimpleThreadPool.class.getName())
            .p("org.apache.openejb.quartz.threadPool.threadCount", "4")
            .p("org.apache.openejb.quartz.threadPool.threadPriority", "5")
            .p("org.apache.openejb.quartz.jobStore.class", JobStoreCMT.class.getName())
            .p("org.apache.openejb.quartz.jobStore.driverDelegateClass", HSQLDBDelegate.class.getName())
            .p("org.apache.openejb.quartz.jobStore.dataSource", "QUARTZ")
            .p("org.apache.openejb.quartz.jobStore.nonManagedTXDataSource", "QUARTZ_NOTX")
            .p("org.apache.openejb.quartz.jobStore.tablePrefix", "qrtz_")
            .p("org.apache.openejb.quartz.jobStore.isClustered", "true")
            .p("org.apache.openejb.quartz.jobStore.clusterCheckinInterval", "60000")
            .p("org.apache.openejb.quartz.jobStore.txIsolationLevelSerializable", "true")
            .p("org.apache.openejb.quartz.jobStore.maxMisfiresToHandleAtATime", "100")
            .p("org.apache.openejb.quartz.dataSource.QUARTZ.jndiURL", "openejb:Resource/QuartzPersistenceForEJBTimersDB")
            .p("org.apache.openejb.quartz.dataSource.QUARTZ_NOTX.jndiURL", "openejb:Resource/QuartzPersistenceForEJBTimersDBNoTx")
            .build();


        final AppModule appModule = new AppModule(Thread.currentThread().getContextClassLoader(), null);
        appModule.getEjbModules().add(ejbModule);
        appModule.getProperties().putAll(quartzConfig);
        return appModule;
    }

    @Configuration
    public Properties configuration() {
        return new PropertiesBuilder()
            // see src/test/resources/import-QuartzPersistenceForEJBTimersDB.sql for the init script
            .p("QuartzPersistenceForEJBTimersDB", "new://Resource?type=DataSource")
            .p("QuartzPersistenceForEJBTimersDB.JtaManaged", "true")
            .p("QuartzPersistenceForEJBTimersDB.DataSourceCreator","tomcat")
            .p("QuartzPersistenceForEJBTimersDB.JdbcUrl", "jdbc:hsqldb:mem:QuartzPersistenceForEJBTimersDB")
            .p("QuartzPersistenceForEJBTimersDB.UserName", "SA")
            .p("QuartzPersistenceForEJBTimersDB.Password", "")

            // see src/test/resources/import-QuartzPersistenceForEJBTimersDBNoTx-.sql for the init script
            .p("QuartzPersistenceForEJBTimersDBNoTx", "new://Resource?type=DataSource")
            .p("QuartzPersistenceForEJBTimersDBNoTx.JtaManaged", "false")
            .p("QuartzPersistenceForEJBTimersDBNoTx.DataSourceCreator","tomcat")
            .p("QuartzPersistenceForEJBTimersDBNoTx.JdbcUrl", "jdbc:hsqldb:mem:QuartzPersistenceForEJBTimersDB")
            .p("QuartzPersistenceForEJBTimersDBNoTx.UserName", "SA")
            .p("QuartzPersistenceForEJBTimersDBNoTx.Password", "")
            .build();
    }

    @LocalBean
    @Stateless
    public static class MyTimedEjb {
        @Resource
        private TimerService timerService;

        private final AtomicLong counter=new AtomicLong(0);

        @Timeout
        public void timeout(final Timer t) {
            long l=((Long) t.getInfo()).longValue();
            System.out.println("@Timeout " + l);
            if (l<100) newTimer();
            else counter.incrementAndGet();
        }

        public long value() {
            return counter.get();
        }

        public void newTimer() {
            timerService.createTimer(200,Long.valueOf(counter.incrementAndGet()));
        }
    }
}
