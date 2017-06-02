package com.sunil.gulabani.chapter6;

import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.StringUtils;
import com.sunil.gulabani.chapter6.core.AWSClientInitializer;

import java.io.File;
import java.io.IOException;

public class S3Operations  extends AWSClientInitializer {

    private AmazonS3Client amazonS3Client;

    public S3Operations() {
        super();

        if(!StringUtils.isNullOrEmpty(getClientConfiguration().getProxyHost())) {
            amazonS3Client = new AmazonS3Client(getCredentials(), getClientConfiguration());
        } else {
            amazonS3Client = new AmazonS3Client(getCredentials());
        }
        amazonS3Client.setRegion(region);
    }

    public String putObject(String bucketName, String keyName, File file) throws IOException {
        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, file);
        request.setCannedAcl(CannedAccessControlList.PublicRead);
        PutObjectResult response = amazonS3Client.putObject(request);
        printObject(response);
        return response.getETag();
    }
}
