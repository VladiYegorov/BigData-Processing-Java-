import java.io.IOException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.services.emr.model.StepConfig;

public class Main {
	private static final Region region = Region.US_EAST_1;
    public static void main(String[] args) throws IOException {	

    	String bucketName = "yourBucketName";
    	EmrClient emr = EmrClient.builder().region(region).build();
    	String DPmin = "50";
    	
        //Step 1
        HadoopJarStepConfig step1cfg = HadoopJarStepConfig.builder()
        		.jar("s3://"+bucketName+"/Step1.jar")
        		.args("s3://"+bucketName+"/hypernym.txt","s3://"+bucketName+"/input/*","s3://"+bucketName+"/output/step1")
                .build();
        StepConfig step1 = StepConfig.builder() 
                .name("Step1")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step1cfg)
                .build(); 
        
      //Step 2
        HadoopJarStepConfig step2cfg = HadoopJarStepConfig.builder()
        		.jar("s3://"+bucketName+"/Step2.jar")
                .args("s3://"+bucketName+"/output/step1/*","s3://"+bucketName+"/output/step2",DPmin)
                .build();
        StepConfig step2 = StepConfig.builder() 
                .name("Step2")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step2cfg)
                .build(); 
      //Step 3
        HadoopJarStepConfig step3cfg = HadoopJarStepConfig.builder()
        		.jar("s3://"+bucketName+"/Step3.jar")
                .args("s3://"+bucketName+"/output/step1/*","s3://"+bucketName+"/output/step2/*","s3://"+bucketName+"/output/step3")
                .build();
        StepConfig step3 = StepConfig.builder() 
                .name("Step3")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step3cfg)
                .build(); 
       
        
        RunJobFlowRequest flowRequest = RunJobFlowRequest.builder()
        		.name("emr-spot-example")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3://"+bucketName+"/elasticmapreduce/")
                .steps(step1,step2,step3)
                .visibleToAllUsers(true)
                .releaseLabel("emr-5.0.0")
                .instances(JobFlowInstancesConfig.builder()
                		.ec2KeyName("linux-key")
                        .instanceCount(4) // CLUSTER SIZE
                        .keepJobFlowAliveWhenNoSteps(false)
                        .masterInstanceType("m4.large")
                        .slaveInstanceType("m4.large")
                        .build())
                .build();
        RunJobFlowResponse response = emr.runJobFlow(flowRequest);  
        System.out.println("JobFlow id: "+response.jobFlowId());
        System.out.println("*****************************************************************************************");
        System.out.println("Your request is in progress. Your output can be found in: "+bucketName+"/output/step3 once it's finished");
        System.out.println("*****************************************************************************************");               
      
    }
}
