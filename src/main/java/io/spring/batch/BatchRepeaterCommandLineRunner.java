/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch;

import java.util.UUID;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author Michael Minella
 */
@Component
public class BatchRepeaterCommandLineRunner implements CommandLineRunner {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private Job job;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void run(String... strings) throws Exception {

		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();

		taskExecutor.setMaxPoolSize(4);
		taskExecutor.setQueueCapacity(100);
		taskExecutor.setCorePoolSize(4);

		taskExecutor.afterPropertiesSet();

		for (long count = 0; count < 100; count++) {
			JobParameters parameters = new JobParametersBuilder()
					.addLong("startValue", count * 1000)
					.addLong("endValue", (count + 1) * 1000)
					.addString("random", UUID.randomUUID().toString())
					.toJobParameters();

			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						JobExecution jobExecution = jobLauncher.run(job, parameters);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
	}
}
