package com.example.ISB.Infocrush.Configuration;



import com.example.ISB.Infocrush.Model.Data;
import com.example.ISB.Infocrush.Processor.TheProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {


    @Autowired
    private ResourceLoader resourceLoader;


    @Autowired
    private MongoTemplate template;


    //1. Item Reader from CSV file
    @Bean


    public ItemReader<Data> reader(){
        FlatFileItemReader<Data> reader=new FlatFileItemReader<Data>();
        //loading file and reading it from AWS
       // reader.setResource(new ClassPathResource("mydata.csv"));

        String url = "https://mybucketcsvfolder.s3.amazonaws.com/input/mydata.csv";
//        reader.setResource(resourceLoader.getResource("s3://" + "mybucketcsvfolder/"  + "input/" +"mydata.csv"));
        reader.setResource(resourceLoader.getResource(url));
        reader.setName("CSV-Reader");

        reader.setLineMapper(new DefaultLineMapper<Data>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("transaction_date","posted_date","transaction_description", "debit", "credited", "balance");
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Data>() {{
                setTargetType(Data.class);
            }});
        }});
        return reader;
    }

    //2. Item Processor
    @Bean
    public ItemProcessor<Data, Data> processor(){
        return (ItemProcessor<Data, Data>) new TheProcessor();

    }


    //#. Item Writer
    @Bean
    public ItemWriter<Data> writer(){


        MongoItemWriter<Data> writer=new MongoItemWriter<>();
        writer.setTemplate(template);
        writer.setCollection("Data");


            return writer;

    }

    //STEP
    @Autowired
    private StepBuilderFactory sf;

    @Bean
    public Step stepA() {
        return sf.get("stepA")
                .<Data,Data>chunk(3)
                .reader(reader())
                .writer(writer())
                .processor(processor())
                .build();
    }


    //JOB
    @Autowired
    private JobBuilderFactory jf;
    @Bean
    public Job jobA() {
        return jf.get("jobA")
                .incrementer(new RunIdIncrementer())
                .start(stepA())
                .build();
    }


}
