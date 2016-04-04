package org.vams.rxjava.app;

import com.google.gson.Gson;
import javaslang.collection.List;
import javaslang.control.Validation;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vamsp7 on 27/03/16.
 */
public class Application {

    public static final Gson gson = new Gson();
    public static final String bulkIndexURL = "http://192.168.99.100:9200/twitter/tweet/_bulk";
    public static final String FILE_PATH = "/Users/vicmejia/Documents/Grooming/Nissan Inventory/Inventory Sample Data/NewCarInventory_nissan_US_es_20160308T074521.txt";

    public static boolean isCompleted = false;

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        Calendar startTime = Calendar.getInstance();

        Observable<String> fileReadObservable2 = Application.getFileLineObservable(FILE_PATH);
        fileReadObservable2
                .filter(Application::validLine)
                .flatMap(Application::toEntity)
                .map(Application::validateEntity)
                .buffer(500)
                .flatMap(Application::toBulkRequest)
                .flatMap(Application::storeData)
                .subscribeOn(Schedulers.io())
                .count()
                .subscribe(Application::processMessage,e -> System.out.println(e.getMessage()), Application::processComplete);

        while (true){
            Thread.sleep(1000L);
            if(isCompleted)
                break;
        }

        System.out.println("Total time (ms): " + (Calendar.getInstance().getTimeInMillis()-startTime.getTimeInMillis()));
    }

    //FILE LINE OBSERVABLE
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

    //PRE-PROCESSING OPERATIONS
    public static boolean validLine(String line){
        return !line.startsWith("#");
    }

    //PARSING OPERATIONS
    public static Observable<Object> toEntity(String line){
        Map<String, Object> entity = new HashMap<>();
        entity.put("code", UUID.randomUUID().toString());
        entity.put("description", line.replace("|"," "));
        return Observable.just(entity);
    }

    //ENTITY VALIDATIONS
    public static Validation<List<String>, Object> validateEntity(Object entity) {
        Map<String, Object> entityMap= (Map<String, Object>) entity;
        String code = (String)entityMap.get("code");
        String description = (String)entityMap.get("description");

        return Validation.combine(validateCode(code),validateDescription(description))
                .ap((o, o2) -> entity);
    }

    private static Validation<String, Object> validateDescription(String description){
        return description.contains("5N1AL0MM0EC524565")
                ? Validation.invalid("Model 5N1AL0MM0EC524565 not allowed ")
                : Validation.valid(description);
    }

    private static Validation<String, Object> validateCode(String code){
        return code.contains("11")
                ? Validation.invalid("Invalid model code "+code+" contains 11")
                : Validation.valid(code);
    }

    //STORE OPERATIONS
    public static Observable<String> toBulkRequest(Iterable<Validation<Iterable<String>, Object>> validatedEntities){
        StringBuilder bulkRequest = new StringBuilder();
        for (Validation validatedEntity:validatedEntities) {
            bulkRequest.append("{ \"create\" : {\"_id\" : \""+((Map)validatedEntity).get("code")+"\" } }\n");
            bulkRequest.append(toJSON(validatedEntity.)+"\n");
        }
        return Observable.just(bulkRequest.toString());
    }


    public static String toJSON(Object entity){
        return gson.toJson(entity);
    }

    private static Observable<String> storeData(String request) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(bulkIndexURL);
        return Observable.create(subscriber -> {
            String statusResponse =  "500";
            try {
                post.setEntity(new StringEntity(request));
                HttpResponse response = client.execute(post);
                statusResponse = response.getStatusLine().getStatusCode()+"";
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            subscriber.onNext(statusResponse);
            subscriber.onCompleted();
        });
    }

    //SUBSCRIPTION METHODS
    public static void processMessage(Object message){
        System.out.println(message);
    }

    public static void processComplete(){
        System.out.println("Message processing has finished");
        Application.isCompleted=true;
    }
}
