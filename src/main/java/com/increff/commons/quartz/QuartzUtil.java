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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

public class QuartzUtil {

	// JOBDATA HELPERS
	public static Map<String, String> convert(JobDataMap jdMap) {
		String[] keys = jdMap.getKeys();
		Map<String, String> map = new HashMap<String, String>(keys.length);
		for (String key : keys) {
			map.put(key, jdMap.getString(key));
		}
		return map;
	}

	public static JobDataMap convert(Map<String, String> params) {
		JobDataMap map = new JobDataMap();
		if (params != null) {
			for (String key : params.keySet()) {
				map.put(key, params.get(key));
			}
		}
		return map;
	}

	// UTILITY METHODS
	public static Trigger getTrigger(QuartzJobForm form) {
		CronScheduleBuilder schedule = getSchedule(form);
		Trigger trigger = TriggerBuilder.newTrigger()//
				.withIdentity(form.getJobName())//
				.withSchedule(schedule)//
				.build();
		return trigger;
	}

	public static JobDetail getJobDetail(QuartzJobForm form, Class<? extends Job> clazz) {
		JobDataMap map = convert(form.getParams());
		JobDetail jobDetail = JobBuilder//
				.newJob(clazz)//
				.withIdentity(form.getJobName())//
				.usingJobData(map)//
				.build();
		return jobDetail;
	}

	public static CronScheduleBuilder getSchedule(QuartzJobForm form) {
		CronScheduleBuilder schedule = CronScheduleBuilder//
				.cronSchedule(form.getSchedule())//
				.inTimeZone(form.getTimezone());
		return schedule;
	}

	public static void scheduleJob(Scheduler scheduler, QuartzJobForm form)
			throws SchedulerException, ClassNotFoundException, ClassCastException {
		Class<? extends Job> clazz = (Class<? extends Job>) Class.forName(form.getJobClass());
		// Create Job Detail
		JobDetail jobDetail = QuartzUtil.getJobDetail(form, clazz);
		// Create Job Trigger
		Trigger trigger = QuartzUtil.getTrigger(form);
		// Schedule it
		scheduler.scheduleJob(jobDetail, trigger);
	}

	public static List<QuartzJobData> getAllJobs(Scheduler scheduler) throws SchedulerException {
		Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.anyJobGroup());
		List<QuartzJobData> jobDetails = new ArrayList<QuartzJobData>(jobKeys.size());
		for (JobKey jobKey : jobKeys) {
			jobDetails.add(getJob(scheduler, jobKey.getName()));
		}
		return jobDetails;
	}

	public static QuartzJobData getJob(Scheduler scheduler, String jobName) throws SchedulerException {
		// Get Data
		JobKey jobKey = new JobKey(jobName);
		TriggerKey triggerKey = new TriggerKey(jobName);
		JobDetail jd = scheduler.getJobDetail(jobKey);
		CronTrigger tg = (CronTrigger) scheduler.getTrigger(triggerKey);
		Map<String, String> params = QuartzUtil.convert(jd.getJobDataMap());
		// Populate
		QuartzJobData jobData = new QuartzJobData();
		jobData.setJobClass(jd.getJobClass().getName());
		jobData.setJobName(jobName);
		jobData.setParams(params);
		jobData.setSchedule(tg.getCronExpression());
		jobData.setTimezone(tg.getTimeZone());
		// return
		return jobData;
	}

	public static void deleteJob(Scheduler scheduler, String jobName) throws SchedulerException {
		JobKey jobKey = new JobKey(jobName);
		scheduler.deleteJob(jobKey);
	}

	public static void deleteAllJobs(Scheduler scheduler) throws SchedulerException {
		// Delete all jobs
		// enumerate each job in group
		for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.anyJobGroup())) {
			scheduler.deleteJob(jobKey);
		}
	}

}
