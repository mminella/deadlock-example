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
package io.spring.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author Michael Minella
 */
@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public Job partitionedJob() {
		return this.jobBuilderFactory.get("partitionedJob")
				.start(step1(null))
				.next(step2())
				.next(step3(null, null))
				.build();
	}

	@Bean
	@StepScope
	public JdbcBatchItemWriter<Integer> writer() {
		return new JdbcBatchItemWriterBuilder<Integer>()
				.dataSource(this.dataSource)
				.sql("INSERT INTO FOO VALUES (?)")
				.itemPreparedStatementSetter((integer, preparedStatement) -> preparedStatement.setInt(1, integer)).build();
	}

	@Bean
	@StepScope
	public ListItemReader<Integer> listItemReader(@Value("#{jobParameters['startValue']}") Integer startValue,
			@Value("#{jobParameters['endValue']}") Integer endValue) {

		List<Integer> items = new ArrayList<>();

		for (int i = startValue; i < endValue; i++) {
			items.add(i);
		}

		return new ListItemReader<>(items);
	}

	@Bean
	public Step step1(ListItemReader<Integer> reader) {

		return this.stepBuilderFactory.get("step1")
				.<Integer, Integer>chunk(100)
				.reader(reader)
				.writer(writer())
				.build();
	}

	@Bean
	public Step step2() {
		final JdbcTemplate template = new JdbcTemplate(this.dataSource);

		return this.stepBuilderFactory.get("step2")
				.tasklet((stepContribution, chunkContext) -> {
					Long count = template.queryForObject("SELECT COUNT(*) FROM FOO", Long.class);

					System.out.println(">> Count = " + count);

					return RepeatStatus.FINISHED;
				}).build();
	}

	@Bean
	public Partitioner partitioner() {
		final JdbcTemplate template = new JdbcTemplate(this.dataSource);

		return i -> {
			Map<String, ExecutionContext> partitions = new HashMap<>();

			Long min = template.queryForObject("SELECT MIN(VALUE) FROM FOO", Long.class);
			Long max = template.queryForObject("SELECT MAX(VALUE) FROM FOO", Long.class);

			long interval = (max - min) / 2;

			ExecutionContext context = new ExecutionContext();

			context.put("startIndex", min);
			context.put("maxIndex", min + interval);

			partitions.put("partition1", context);

			context = new ExecutionContext();

			context.put("startIndex", min + interval + 1);
			context.put("maxIndex", max);

			partitions.put("partition2", context);

			return partitions;
		};
	}

	@Bean
	public PartitionHandler partitionHandler() {
		TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();

		partitionHandler.setTaskExecutor(new SimpleAsyncTaskExecutor());
		partitionHandler.setStep(step3Worker());

		return partitionHandler;
	}


	@Bean
	public Step step3(Partitioner partitioner, PartitionHandler partitionHandler) {
		return this.stepBuilderFactory.get("step3master")
				.partitioner("step3worker", partitioner)
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean
	@StepScope
	public JdbcPagingItemReader<Integer> jdbcItemReader(@Value("#{stepExecutionContext['startIndex']}") Long minValue,
			@Value("#{stepExecutionContext['maxIndex']}") Long maxValue) {
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("value", Order.ASCENDING);

		return new JdbcPagingItemReaderBuilder<Integer>()
				.name("deletionItemReader")
				.dataSource(this.dataSource)
				.selectClause("SELECT *")
				.fromClause("FOO")
				.whereClause("VALUE < " + maxValue + " && VALUE >=" + minValue)
				.sortKeys(sortKeys)
				.rowMapper((resultSet, i) -> resultSet.getInt(1))
				.build();
	}

	@Bean
	public JdbcBatchItemWriter<Integer> deletingItemWriter() {
		return new JdbcBatchItemWriterBuilder<Integer>()
				.dataSource(this.dataSource)
				.sql("DELETE FROM FOO WHERE VALUE = ?")
				.assertUpdates(false)
				.itemPreparedStatementSetter((item, ps) -> ps.setInt(1, item)).build();
	}

	@Bean
	public Step step3Worker() {
		return this.stepBuilderFactory.get("step3worker")
				.<Integer, Integer>chunk(100)
				.reader(jdbcItemReader(null, null))
				.writer(deletingItemWriter())
				.build();
	}
}
