package com.example.cloudwatchlogs;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilteredLogEvent;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author charankumar
 */
public class CloudwatchEvents {
    private CloudWatchLogsClient cloudWatchLogsClient;

    public static long convertStringDateToEpochMs(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static long getCurrentTimeInUtcEpochMs(boolean flag) {
        LocalDateTime now = LocalDateTime.now();
        ZonedDateTime utcTime = now.atZone(ZoneOffset.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
        return flag ? utcTime.minusMinutes(15).toInstant().toEpochMilli() : utcTime.toInstant().toEpochMilli();
    }

    public static void main(String[] args) throws IOException {
        CloudwatchEvents cloudwatchEvents = new CloudwatchEvents();
        try {
            cloudwatchEvents.init();
            Map<String,  Map<String, String>> result = cloudwatchEvents.getStats("",
                    null, getCurrentTimeInUtcEpochMs(true), getCurrentTimeInUtcEpochMs(false));
            System.out.println(getDataInTableFormat(result));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            cloudwatchEvents.deinit();
        }
    }

    private void init() {
        Properties props = new Properties();  //Replcae it with PropertiesLoader.getProperties()

        String region = Region.US_EAST_1.toString();
        if ("1".equals(props.getProperty("is.ec2.role.access.allowed"))) {
            this.cloudWatchLogsClient = CloudWatchLogsClient.builder()
                    .credentialsProvider((AwsCredentialsProvider) InstanceProfileCredentialsProvider.getInstance())
                    .region(Region.of(region))
                    .build();
        } else if ("1".equals(props.getProperty("is.container.role.access.allowed"))) {
            this.cloudWatchLogsClient = CloudWatchLogsClient.builder()
                    .credentialsProvider((AwsCredentialsProvider) WebIdentityTokenCredentialsProvider.create())
                    .region(Region.of(region))
                    .build();
        } else {
            String accessKey = props.getProperty("aws.access.key", "");
            String secretKey = props.getProperty("aws.secret.key", "");
            if (accessKey != null && secretKey != null) {
                StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
                this.cloudWatchLogsClient = CloudWatchLogsClient.builder().credentialsProvider(staticCredentialsProvider).region(Region.of(region)).build();
            }
        }

        if (this.cloudWatchLogsClient == null) {
            throw new RuntimeException("Unable to create CloudWatchLogsClient");
        }
    }

    public Map<String,  Map<String, String>> getStats(String logGroupName, String filterPattern, Long startTimeEpochMsInUTC, Long endTimeEpochMsInUTC) throws IOException {
        Map<String,  Map<String, String>> compExecpMap = new HashMap<>();
        System.out.println("Execution started at  "+new Date());
        List<String> componentNames = new ArrayList<>();
        componentNames.add("campaigncore");
       // componentNames.add("PROFILESCHEDULER");
        try {
            String nextToken = null;
            do {
                FilterLogEventsRequest.Builder requestBuilder = FilterLogEventsRequest.builder().logGroupName(logGroupName)
                        .filterPattern(filterPattern).startTime(startTimeEpochMsInUTC)
                        .endTime(endTimeEpochMsInUTC).limit(10000); // Maximum number of events per request
                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }
                FilterLogEventsRequest request = requestBuilder.build();
                FilterLogEventsResponse response = this.cloudWatchLogsClient.filterLogEvents(request);
                for (FilteredLogEvent event : response.events()) {
                    String message = event.message();

                   //boolean componentName= componentNames.stream().anyMatch(message::contains);
                    System.out.println(message);

                    if (message.indexOf("~") != -1 && message.lastIndexOf(" ~ ") != -1 ) {
                        int firstTilde = message.indexOf("~");
                        int secondTilde = message.indexOf(" ~ ", firstTilde + 1);
                        int thirdTilde = message.indexOf(" ~ ", secondTilde + 1);
                        int fourthTilde = message.indexOf(" ~ ", thirdTilde + 1);
                        fourthTilde = fourthTilde == -1 ? message.length() : fourthTilde;
                        String component = message.substring(firstTilde + 1, secondTilde).trim();
                        if (thirdTilde != -1 && fourthTilde != -1) {
                            String result = extractExceptionClass(message, thirdTilde, fourthTilde);
                            Map<String, String> exceptionMap = compExecpMap.computeIfAbsent(component, k -> new HashMap<>());
                            exceptionMap.merge(result, "1", (oldValue, newValue) -> String.valueOf(Integer.parseInt(oldValue) + 1));
                        }

                    }
                }

                nextToken = response.nextToken();

            } while (nextToken != null);
            System.out.println("Execution Completed at  " + new Date());
        }catch (Exception e){
            e.printStackTrace();
        }
        return compExecpMap;
    }
    public static String getDataInTableFormat(Map<String, Map<String, String>> compExecpMap) {
        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table border='1'>")
                .append("<tr><th>Component</th><th>Exception</th><th>Count</th></tr>");
        for (Map.Entry<String, Map<String, String>> entry : compExecpMap.entrySet()) {
            String component = entry.getKey();
            Map<String, String> exceptionMap = entry.getValue();
            for (Map.Entry<String, String> exceptionEntry : exceptionMap.entrySet()) {
                String exception = exceptionEntry.getKey();
                String count = exceptionEntry.getValue();
                htmlTable.append("<tr><td>").append(component).append("</td><td>")
                        .append(exception).append("</td><td>").append(count).append("</td></tr>");
            }
        }
        return htmlTable.toString();
    }

    private static String extractExceptionClass(String message, int thirdTilde, int fourthTilde) {
        String exceptionMessage = message.substring(thirdTilde + 1, fourthTilde).trim();
        String exceptionClassName = extractExceptionData(exceptionMessage);
        return exceptionClassName;
    }

    public static String extractExceptionData(String message) {
        String exceptionKeyword = "Exception:";
        int exceptionIndex = message.indexOf(exceptionKeyword);
        if (exceptionIndex == -1) {
            return "";
        }
        int lastDotIndex = message.lastIndexOf('.', exceptionIndex);
        if (lastDotIndex == -1) {
            return "";
        }
        String exceptionData=  message.substring(lastDotIndex + 1, exceptionIndex+(exceptionKeyword.length()-1)).trim();
        if(message.contains(".Exception")){
            exceptionData="";
        }
        if(exceptionData.isEmpty()) {
            exceptionData=  extractSubExceptionData(message, "Exception:");
        }
        return exceptionData;
    }

    public static String extractSubExceptionData(String str, String subStr) {
        int firstIndex = str.indexOf(subStr);
        if (firstIndex == -1) return "";

        int secondIndex = str.indexOf(subStr, firstIndex + subStr.length());
        if (secondIndex == -1) return "";

        int lastDotIndex = str.lastIndexOf('.', secondIndex);
        if (lastDotIndex == -1) return "";

        return str.substring(lastDotIndex + 1, secondIndex + subStr.length() - 1).trim();
    }



    public void deinit() {
        if (this.cloudWatchLogsClient != null) {
            this.cloudWatchLogsClient.close();
        }
    }

}