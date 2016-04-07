package org.vams.rxjava.app;

import com.google.gson.Gson;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.List;
import javaslang.control.Try;
import javaslang.control.Validation;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import rx.Observable;
import rx.exceptions.Exceptions;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static javaslang.API.*;

/**
 * Created by vamsp7 on 27/03/16.
 */
public class Application {

    public static final Gson gson = new Gson();
    public static final String bulkIndexURL = "http://192.168.99.100:9200/twitter/tweet/_bulk";
    public static final String FILE_PATH = "/Users/vicmejia/Documents/Grooming/Nissan Inventory/Inventory Sample Data/NewCarInventory_nissan_US_es_20160308T074521.txt";
    public static final int BUFFER_SIZE=1000;

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        Calendar startTime = Calendar.getInstance();
        Observable<String> fileReadObservable2 = Application.getFileLineObservable(FILE_PATH);
        fileReadObservable2
                .filter(Application::validLine)
                .flatMap(Application::toEntity)
                .map(Application::validateEntity)
                .buffer(BUFFER_SIZE)
                .map(Application::tuple)
                .map(t -> Tuple.of(Application.log(t._1),Application.store(t._2)))
                .reduce(Tuple.of(0,0), (totalTuple, tuple) -> Tuple.of(totalTuple._1 + tuple._1,totalTuple._2 +tuple._2))
                .subscribe(Application::processMessage,e -> System.out.println(e.getMessage()), Application::processComplete);

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
        entity.put("code", UUID.randomUUID().toString()+Math.random()*1000);
        entity.put("description", line.replace("|"," "));
        return Observable.just(entity);
    }

    //ENTITY VALIDATIONS
    public static Validation<String, Object> validateEntity(Object entity) {
        Map<String, Object> entityMap= (Map<String, Object>) entity;
        String code = (String)entityMap.get("code");
        String description = (String)entityMap.get("description");
        return Validation.combine(validateCode(code),validateDescription(description))
                .ap((o, o2) -> entity).leftMap(errors -> errors.mkString("ERRORS for entity: "+code+"\n","\n","\n---"));
    }

    private static Validation<String, Object> validateDescription(String description){
        return Math.random() <= 0.95
                ? Validation.invalid("Model not allowed ")
                : Validation.valid(description);
    }

    private static Validation<String, Object> validateCode(String code){
        return code.contains("111")
                ? Validation.invalid("Invalid model code "+code+" contains 111")
                : Validation.valid(code);
    }

    //STORE OPERATIONS
    public static int store(Iterable<Object> entities){
        create(toBulkRequest(entities));
        return List.ofAll(entities).size();
    }

    public static String toBulkRequest(Iterable<Object> entities){
        return List.ofAll(entities)
                    .flatMap(entity -> List.of("{ \"create\" : {\"_id\" : \"" + ((Map) entity).get("code") + "\" } }", toJSON(entity)))
                    .mkString("\n");
    }

    public static String toJSON(Object entity){
        return gson.toJson(entity);
    }

    private static String create(String request) {
        String statusCode="200";
        try {
            HttpPost post = new HttpPost(bulkIndexURL);
            post.setEntity(new StringEntity(request));
            statusCode = HttpClientBuilder.create().build().execute(post).getStatusLine().getStatusCode() + "";
        } catch (Exception e) {
            e.printStackTrace();
            statusCode="500";
        }
        return statusCode;
    }

    //SUBSCRIPTION METHODS
    public static void processMessage(Tuple2 message){
        System.out.println("Processed: "+message._2+" Errors: "+message._1 );
        //upload log
    }

    public static void processComplete(){
        System.out.println("Message processing has finished");
        //confirm stored data
    }

    //UTILS
    public static <E,T> Tuple2<Iterable<E>, Iterable<T>> tuple(Iterable<Validation<E, T>> validations ){
        return List.ofAll(validations)
                .groupBy(Validation::isValid)
                .transform(validationsMap -> Tuple.of(validationsMap.get(false),validationsMap.get(true)))
                .map((errorOptions, validOptions) -> Tuple.of(errorOptions.getOrElse(List.empty()),validOptions.getOrElse(List.empty())))
                .map((errors, valids) -> Tuple.of(errors.map(Validation::getError),valids.map(Validation::get)));
    }
    public static int log(Iterable<String> logEntries){
        return List.ofAll(logEntries).map(s -> {
            System.out.println(s);
            return s;
        }).size();
    }
}
