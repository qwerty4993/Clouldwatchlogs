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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author charankumar
 */
public class CloudwatchEventsTest {
    private CloudWatchLogsClient cloudWatchLogsClient;
    private static final String COMP_PATTERN = "~\\s*(.*?)\\s*~";
    private static final String EXCEPTION_PATTERN = "(\\w+(\\.\\w+)*Exception):";

    public static long convertStringDateToEpochMs(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static long getCurrentTimeInUtcEpochMs(boolean flag) {
        LocalDateTime now = LocalDateTime.now();
        ZonedDateTime utcTime = now.atZone(ZoneOffset.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
        return flag ? utcTime.minusMinutes(5).toInstant().toEpochMilli() : utcTime.toInstant().toEpochMilli();
    }

    public static void main(String[] args) throws IOException {
        CloudwatchEventsTest cloudwatchEvents = new CloudwatchEventsTest();
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
        Properties props = new Properties();

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
        //componentNames.add("campaigncore");
       // componentNames.add("cmapi");
        Pattern compPattern = Pattern.compile(COMP_PATTERN);
        Pattern exceptionPattern = Pattern.compile(EXCEPTION_PATTERN);
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

                    Matcher compMatcher = compPattern.matcher(message);
                    Matcher exceptionMatcher = exceptionPattern.matcher(message);

                    if (compMatcher.find() && exceptionMatcher.find()) {
                        if(message.contains("RedshiftJDBC41-no-awssdk-1.")){
                            System.out.println(message);
                        }

                        String componentName = compMatcher.group(1);
                        String exceptionClass = exceptionMatcher.group(1);
                        exceptionClass=  exceptionClass.substring(exceptionClass.lastIndexOf(".")+1,  exceptionClass.length());
                        Map<String, String> exceptionMap = compExecpMap.computeIfAbsent(componentName, k -> new HashMap<>());
                        exceptionMap.merge(exceptionClass, "1", (oldValue, newValue) -> String.valueOf(Integer.parseInt(oldValue) + 1));
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
        htmlTable.append("<table width=\"100%\" border=\"1\" style=\"border-collapse: collapse; text-align: left;\">")
                .append("<thead>")
                .append("<tr style=\"background-color: #f2f2f2;\">")
                .append("<th style=\"padding: 8px;\">S.No</th>")
                .append("<th style=\"padding: 8px;\">Component</th>")
                .append("<th style=\"padding: 8px;\">Exception</th>")
                .append("<th style=\"padding: 8px;\">Count</th>")
                .append("</tr>")
                .append("</thead>")
                .append("<tbody>");

        int countSno = 1;
        for (Map.Entry<String, Map<String, String>> entry : compExecpMap.entrySet()) {
            String component = entry.getKey();
            Map<String, String> exceptionMap = entry.getValue();
            boolean firstRow = true;
            for (Map.Entry<String, String> exceptionEntry : exceptionMap.entrySet()) {
                String exception = exceptionEntry.getKey();
                String count = exceptionEntry.getValue();
                htmlTable.append("<tr>");
                if (firstRow) {
                    htmlTable.append("<td rowspan='").append(exceptionMap.size()).append("' style=\"padding: 8px;\">").append(countSno).append("</td>");
                    htmlTable.append("<td rowspan='").append(exceptionMap.size()).append("' style=\"padding: 8px;\">").append(component).append("</td>");
                    firstRow = false;
                }
                htmlTable.append("<td style=\"padding: 8px;\">").append(exception).append("</td>")
                        .append("<td style=\"padding: 8px;\">").append(count).append("</td>")
                        .append("</tr>");
            }
            countSno++;
        }

        htmlTable.append("</tbody>")
                .append("</table>");
        return htmlTable.toString();
    }

    public void deinit() {
        if (this.cloudWatchLogsClient != null) {
            this.cloudWatchLogsClient.close();
        }
    }

}