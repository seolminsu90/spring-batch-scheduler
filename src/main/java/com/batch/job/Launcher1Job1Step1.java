package com.batch.job;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableBatchProcessing
@EnableScheduling
public class Launcher1Job1Step1 {

    private static final String BATCH_NAME = "BaseJobStep";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    @Scheduled(cron = "1 * * * * *")
    public void scheduler() throws Exception {
        String jobId = String.valueOf(System.currentTimeMillis());

        System.out.println("Started jobId : " + jobId);

        JobParameters param = new JobParametersBuilder().addString("JobID", jobId).toJobParameters();
        JobExecution execution = jobLauncher.run(baseJob(), param);

        System.out.println("end : " + param.getString("JobID") + ":::" + execution.getStatus());
    }

    @Bean
    public Job baseJob() {
        return jobBuilderFactory.get("[Job - " + BATCH_NAME + "]").start(baseStep()).build();
    }

    @Bean
    public Step baseStep() {
        return stepBuilderFactory.get("[Step - " + BATCH_NAME + "]").<String, String>chunk(20)
                .reader(sampleItemReader()).processor(sampleItemProcessor()).writer(sampleItemWriter()).build();
    }

    @Bean
    public ItemReader<String> sampleItemReader() {
        return new ItemReader<String>() {
            String[] messages = { "sample data", "Welcome to Spring Batch Example", "Database for this example" };
            int count = 0;

            @Override
            public String read()
                    throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                if (count < messages.length) {
                    return messages[count++];
                } else {
                    count = 0;
                }
                return null;
            }
        };
    }

    @Bean
    public ItemProcessor<String, String> sampleItemProcessor() {
        return new ItemProcessor<String, String>() {

            @Override
            public String process(String data) throws Exception {
                return data.toUpperCase();
            }
        };
    }

    @Bean
    public ItemWriter<String> sampleItemWriter() {
        return new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> items) throws Exception {
                for (String msg : items) {
                    System.out.println("the data " + msg);
                }
            }
        };
    }

}
