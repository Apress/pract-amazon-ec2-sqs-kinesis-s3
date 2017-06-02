package com.sunil.gulabani.chapter6.core;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class AWSClientInitializer {

    private AWSCredentials credentials = null;
    protected Region region;

    public AWSClientInitializer() {
        try {
//			credentials = new BasicAWSCredentials("your_access_key_id", "your_secret_access_key");

            credentials = new ProfileCredentialsProvider("sunilgulabani").getCredentials();
            region = Region.getRegion(Regions.US_WEST_2);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
    }

    protected ClientConfiguration getClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProtocol(Protocol.HTTPS);
        clientConfiguration.withProxyHost("");
        clientConfiguration.withProxyPort(80);
        clientConfiguration.withProxyUsername("");
        clientConfiguration.withProxyPassword("");
        return clientConfiguration;
    }

    public AWSCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(AWSCredentials credentials) {
        this.credentials = credentials;
    }

    protected void seperator(String featureName) {
        System.out.println("\n\n*********************************************************************************\n\n");
        System.out.println("+++++++++++++++++++++");
        System.out.println(featureName);
        System.out.println("+++++++++++++++++++++");
    }

    protected void printObject(Object object) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(object));
    }
}
