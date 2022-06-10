package com.example.ISB.Infocrush.Processor;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.example.ISB.Infocrush.Configuration.AWSConfig;
import com.example.ISB.Infocrush.Model.Data;
import org.springframework.batch.item.ItemProcessor;


public class TheProcessor implements ItemProcessor<Data, Data> {


    @Override
    public Data process(Data item) throws Exception {


        //It processes after writing to DB and move it to archive folder
        String accessKey= "AKIAXUTQR2WHTKC6XSMO";
        String secretKey="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";
        AWSConfig a2=new AWSConfig();
        AWSCredentialsProvider a3= new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(a3).build();

        //AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(.credentialsProvider()).build();

        try{
            s3.copyObject("mybucketcsvfolder","input"+"/mydata.csv","mybucketcsvfolder","archive"+"/"+"mydata.csv");
            DeleteObjectRequest deleteObjRequest = new DeleteObjectRequest("mybucketcsvfolder",
                    "input"+"/"+ "mydata.csv");
            s3.deleteObject(deleteObjRequest);

        }catch (AmazonServiceException e){
            System.err.println(e.getErrorMessage());
        }

        return item;
    }
}

