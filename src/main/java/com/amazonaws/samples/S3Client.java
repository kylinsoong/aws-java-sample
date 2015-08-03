package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * 1. set up credentials
 *     http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds
 * 
 * 
 * @author kylin
 *
 */
public class S3Client {

	public static void main(String[] args) throws IOException {
		
		String bucketName = "teiid-" + UUID.randomUUID();
		String key = "MyObjectKey";

		AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
		s3client.setRegion(Region.getRegion(Regions.US_WEST_2));
		
		try {
			if(!(s3client.doesBucketExist(bucketName))){
				s3client.createBucket(new CreateBucketRequest(bucketName));
				String bucketLocation = s3client.getBucketLocation(new GetBucketLocationRequest(bucketName));
				System.out.println("bucket location = " + bucketLocation);
			}
			
			System.out.println("Listing buckets");
            for (Bucket bucket : s3client.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            System.out.println();
            
            System.out.println("Uploading a new object to S3 from a file\n");
            s3client.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));
            
            System.out.println("Downloading an object");
            S3Object object = s3client.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());
            
            System.out.println("Deleting an object\n");
            s3client.deleteObject(bucketName, key);
            
            System.out.println("Deleting bucket " + bucketName + "\n");
            s3client.deleteBucket(bucketName);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (AmazonClientException e) {
			e.printStackTrace();
		} 		
	}
	
	private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }
	
	private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
