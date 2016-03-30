package org.vams.rxjava.app;

import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import rx.Observable;
import sun.net.www.http.HttpClient;

import java.io.*;
import java.util.*;

/**
 * Created by vamsp7 on 27/03/16.
 */
public class Application {

    public static final Gson gson = new Gson();
    public static final String bulkIndexURL = "http://192.168.99.100:9200/twitter/tweet/_bulk";

    public static final String FILE_PATH = "/Users/vicmejia/Documents/Grooming/Nissan Inventory/Inventory Sample Data/NewCarInventory_nissan_US_es_20160308T074521.txt";

    public static void main(String[] args) throws FileNotFoundException {

        Calendar startTime = Calendar.getInstance();

        Observable<String> fileReadObservable2 = Application.getFileLineObservable(FILE_PATH);
        fileReadObservable2
                .filter(Application::validLine)
                .map(Application::toEntity)
                .buffer(500)
                .map(Application::toBulkRequest)
                .map(Application::storeData)
                .count()
                .subscribe(Application::processMessage,e -> System.out.println(e.getMessage()), Application::processComplete);

        System.out.println("Total time (ms): " + (Calendar.getInstance().getTimeInMillis()-startTime.getTimeInMillis()));

    }

    private static String storeData(String request) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(bulkIndexURL);
        String statusResponse =  500+"";
        try {
            post.setEntity(new StringEntity(request));
            HttpResponse response = client.execute(post);
            statusResponse = response.getStatusLine().getStatusCode()+"";
        }
        catch ( IOException e) {
            e.printStackTrace();
        }
        return statusResponse;
    }

    public static boolean validLine(String line){
        return !line.startsWith("#");
    }

    public static Object toEntity(String line){
        Map<String, Object> entity = new HashMap<>();
        entity.put("code", UUID.randomUUID().toString());
        entity.put("description", line.replace("|"," "));
        return entity;
    }

    public static String toBulkRequest(List<Object> entities){
        StringBuilder bulkRequest = new StringBuilder();
        for (Object entity:entities) {
            bulkRequest.append("{ \"create\" : {\"_id\" : \""+((Map)entity).get("code")+"\" } }\n");
            bulkRequest.append(toJSON(entity)+"\n");
        }

        return bulkRequest.toString();
    }

    public static Object validateEntity(Object entity) {
        if(((String) entity).contains("5N1AL0MM0EC524565")){
            throw new RuntimeException("This model is not allowed");
        }
        return entity;
    }

    public static void processMessage(Object message){
        System.out.println(message);
    }

    public static void processComplete(){
        System.out.println("Message processing has finished");
    }

    public static Observable<String> getFileLineObservable(String filePath){

        return Observable.create(subscriber -> {
            try {
                BufferedReader br  = new BufferedReader(new FileReader(filePath));
                br.lines().forEach(s -> subscriber.onNext(s));
                subscriber.onCompleted();

            } catch (Exception e) {
                subscriber.onError(e);
            }
        });


    }

    public static String toJSON(Object entity){
        return gson.toJson(entity);
    }
}
