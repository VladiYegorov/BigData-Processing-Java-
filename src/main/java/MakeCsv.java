import java.io.PrintWriter;
import java.util.List;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class MakeCsv {
	public static void main(String[] args) throws Exception {
		String bucketName = "yourBucketName";
		Region region = Region.US_EAST_1;
		S3Client s3 = S3Client.builder().region(region).build();
		int depVecSize = -1;
		String line = null;
		ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()	
				.bucket(bucketName)
			    .prefix("output/step3/part")
			    .build();
		PrintWriter writer = new PrintWriter("src/main/resources/final.csv", "UTF-8");
		
		List<S3Object> files = s3.listObjects(listObjectsRequest).contents();
		for(S3Object it : files) {
			GetObjectRequest objectRequest = GetObjectRequest.builder().key(it.key()).bucket(bucketName).build();
			ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
			String[] fileData = objectBytes.asUtf8String().split("\n");
			for (String iter : fileData) {		
				String[] lineData = iter.split("\t");
				if(depVecSize == -1) {
					depVecSize = lineData.length - 2;
					line = "Word1,Word2,Boolean";
					for(int i=0;i<depVecSize;i++){
						line += ",Dep" + (i+1);
					}
					writer.println(line);
				} else {
					String[] words = lineData[0].split("\\s+");
					line = words[0] + "," +words[1] + "," + lineData[1];
					for(int i=0;i<depVecSize;i++) {
						line += "," + lineData[i+2];
					}
					writer.println(line);
				}
			}
		}
		writer.close();
	}
	
}