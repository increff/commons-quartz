/*
 * Copyright (c) 2021. Increff
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.increff.commons.quartz;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public class QuartzJobTest {

	private static Scheduler scheduler;

	@BeforeClass
	public static void beforeClass() throws SchedulerException {
		SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();
		scheduler = schedFact.getScheduler();
		scheduler.start();
	}

	@AfterClass
	public static void afterClass() throws SchedulerException {
		scheduler.shutdown();
	}

	@Before
	public void beforeTest() throws SchedulerException {
		TestJob.count = 0;
		QuartzUtil.deleteAllJobs(scheduler);
		assertEquals(0, QuartzUtil.getAllJobs(scheduler).size());
	}

	@Test
	public void testGetMap() {
		JobDataMap jdMap = new JobDataMap();
		jdMap.put("key1", "value1");
		jdMap.put("key2", "value2");
		Map<String, String> map = QuartzUtil.convert(jdMap);
		assertEquals("value1", map.get("key1"));
		assertEquals("value2", map.get("key2"));
	}

	@Test
	public void testGetJobData() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		JobDataMap jdMap = QuartzUtil.convert(map);
		assertEquals("value1", jdMap.get("key1"));
		assertEquals("value2", jdMap.get("key2"));
	}

	@Test
	public void testAddJob() throws ClassNotFoundException, ClassCastException, SchedulerException {
		QuartzJobForm form = createJobForm("test1");
		form.setJobName("test1");
		QuartzUtil.scheduleJob(scheduler, form);
		QuartzJobData data = QuartzUtil.getJob(scheduler, "test1");
		assertNotNull(data);
	}

	@Test
	public void testGetJob() throws ClassNotFoundException, ClassCastException, SchedulerException {
		QuartzJobForm form = createJobForm("test2");
		QuartzUtil.scheduleJob(scheduler, form);
		QuartzJobData data = QuartzUtil.getJob(scheduler, "test2");
		assertNotNull(data);
		assertEquals(form.getJobName(), data.getJobName());
		assertEquals(form.getJobClass(), data.getJobClass());
		assertEquals(form.getSchedule(), data.getSchedule());
		assertEquals(form.getTimezone(), data.getTimezone());
	}

	@Test
	public void testGetAllJobs() throws SchedulerException, ClassNotFoundException, ClassCastException {
		QuartzJobForm form1 = createJobForm("test3");
		QuartzJobForm form2 = createJobForm("test4");
		QuartzUtil.scheduleJob(scheduler, form1);
		QuartzUtil.scheduleJob(scheduler, form2);
		List<QuartzJobData> list = QuartzUtil.getAllJobs(scheduler);
		assertEquals(2, list.size());
	}

	@Test
	public void testDeleteJob() throws ClassNotFoundException, ClassCastException, SchedulerException {
		QuartzJobForm form1 = createJobForm("test5");
		QuartzJobForm form2 = createJobForm("test6");
		QuartzUtil.scheduleJob(scheduler, form1);
		QuartzUtil.scheduleJob(scheduler, form2);
		QuartzUtil.deleteJob(scheduler, "test5");
		List<QuartzJobData> list = QuartzUtil.getAllJobs(scheduler);
		assertEquals(1, list.size());
	}

	@Test
	public void testDeleteAllJobs() throws ClassNotFoundException, ClassCastException, SchedulerException {
		QuartzJobForm form1 = createJobForm("test5");
		QuartzJobForm form2 = createJobForm("test6");
		QuartzUtil.scheduleJob(scheduler, form1);
		QuartzUtil.scheduleJob(scheduler, form2);
		QuartzUtil.deleteAllJobs(scheduler);
		List<QuartzJobData> list = QuartzUtil.getAllJobs(scheduler);
		assertEquals(0, list.size());
	}

	@Test
	public void testJobScheduling() throws InterruptedException, SchedulerException {
		int count = 10;
		// define the job and tie it to our HelloJob class
		JobDetail jobd = JobBuilder.newJob(TestJob.class).withIdentity("myJob", "group1").build();
		// Trigger the job to run for 10 times
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity("myTrigger", "group1").startNow()
				.withSchedule(SimpleScheduleBuilder.repeatSecondlyForTotalCount(count)).build();
		scheduler.scheduleJob(jobd, trigger);
		Thread.sleep(count * 1000);
		// Test within marging of error
		assertTrue(TestJob.count >= count && TestJob.count <= count + 1);
	}

	private static QuartzJobForm createJobForm(String name) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("key1", "value1");
		params.put("key2", "value2");
		QuartzJobForm form = new QuartzJobForm();
		form.setSchedule("0 0 0 1 1 ? 2099");
		form.setJobName(name);
		form.setParams(params);
		form.setTimezone(TimeZone.getTimeZone("GMT+5:30"));
		form.setJobClass("com.increff.commons.quartz.TestJob");
		return form;
	}

}
