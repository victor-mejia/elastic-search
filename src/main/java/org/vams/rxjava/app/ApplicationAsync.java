package org.vams.rxjava.app;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.List;
import javaslang.control.Try;
import javaslang.control.Validation;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import rx.Observable;
import rx.observables.ConnectableObservable;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by vamsp7 on 27/03/16.
 */
public class ApplicationAsync {

    public static final Gson gson = new Gson();
    public static final String bulkIndexURL = "http://localhost:9200/twitter/tweet/_bulk";
    public static final String FILE_PATH = "/home/vamsp7/Downloads/popcorn-android/Inventory Sample Data/NewCarInventory_infiniti_US_en_20160308T074521.txt";
    public static final int BUFFER_SIZE=500;
    public static boolean isCompleted=false;

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        Calendar startTime = Calendar.getInstance();

        ConnectableObservable<Tuple2<Observable<Integer>, Observable<Integer>>> mainProcess = ApplicationAsync.getFileLineObservable(FILE_PATH)
                .map(ApplicationAsync::validLine)
                .map(validation -> validation.flatMap(ApplicationAsync::toEntity))
                .map(validation -> validation.flatMap(ApplicationAsync::validateEntity))
                .buffer(BUFFER_SIZE)
                .map(ApplicationAsync::tuple)
                .map(t -> Tuple.of(ApplicationAsync.log(t._1), ApplicationAsync.store(t._2))).publish();

        //Store observable processing
        mainProcess.flatMap(t -> t._2)
                .reduce((x,y) -> x+y)
                .subscribe(totalProcessed -> System.out.println("Total processed records: "+totalProcessed),Throwable::printStackTrace, () -> isCompleted = true);

        //Log observable processing
        mainProcess.flatMap(t -> t._1)
                .reduce((x,y) -> x+y)
                .subscribe(totalErrors -> System.out.println("Total records with errors: "+totalErrors));

        mainProcess.connect();

        System.out.println("Total time (ms): " + (Calendar.getInstance().getTimeInMillis()-startTime.getTimeInMillis()));
    }

    //FILE LINE OBSERVABLE
    public static Observable<String> getFileLineObservable(String filePath){
        return Observable.create(subscriber ->
                Try.of(() -> FileUtils.lineIterator(new File(filePath)))
                        .andThen(li -> li.forEachRemaining(subscriber::onNext) )
                        .andThen(li -> subscriber.onCompleted())
                        .andThen(LineIterator::closeQuietly)
                        .orElseRun(subscriber::onError));
    }

    //PRE-PROCESSING OPERATIONS
    public static Validation<String, String> validLine(String line){

        return !line.startsWith("#") ? Validation.valid(line) : Validation.invalid("Omiting comment line "+line);
    }

    //PARSING OPERATIONS
    public static Validation<String, Object> toEntity(String line){

        return ApplicationAsync.toValidation(Try.of(() -> ApplicationAsync.parse(line)));
    }

    public static Object parse(String line) {

        if(Math.random() <= 0.01)
            throw new RuntimeException("Error parsing line "+line+" \n Error Detail: ..Put parser specific message error...");

        Map<String, Object> entity = new HashMap<>();
        entity.put("code", UUID.randomUUID().toString()+Math.random()*1000);
        entity.put("description", line.replace("|"," "));
        return entity;
    }

    //ENTITY VALIDATIONS
    public static Validation<String, Object> validateEntity(Object entity) {
        Map<String, Object> entityMap= (Map<String, Object>) entity;
        String code = (String)entityMap.get("code");
        String description = (String)entityMap.get("description");
        return Validation.combine(validateCode(code),validateDescription(description))
                .ap((x,y) -> entity).leftMap(errors -> errors.mkString("ERRORS for entity: "+code+"\n","\n","\n---"));
    }

    private static Validation<String, Object> validateDescription(String description){
        return Validation.valid(description);
//        return Math.random() <= 0.01
//                ? Validation.invalid("Model not allowed ")
//                : Validation.valid(description);
    }

    private static Validation<String, Object> validateCode(String code){
//        return Validation.valid(code);
        return code.contains("111")
                ? Validation.invalid("Invalid model code "+code+" contains 111")
                : Validation.valid(code);
    }

    //STORE OPERATIONS
    public static Observable<Integer> store(Iterable<Object> entities){
        return Observable.create(subscriber -> {
            create(toBulkRequest(entities));
            subscriber.onNext(List.ofAll(entities).size());
            subscriber.onCompleted();
        });
    }

    public static String toBulkRequest(Iterable<Object> entities){
        return List.ofAll(entities)
                    .flatMap(entity -> List.of("{ \"create\" : {\"_id\" : \"" + ((Map) entity).get("code") + "\" } }", toJSON(entity)))
                    .mkString("","\n","\n");
    }

    public static String toJSON(Object entity){
        return gson.toJson(entity);
    }

    private static String create(String request) {
        String statusCode="200";
        try {
            HttpPost post = new HttpPost(bulkIndexURL);
            post.setEntity(new StringEntity(request));
            CloseableHttpResponse response = HttpClientBuilder.create().build().execute(post);

            JsonObject jsonObject = gson.fromJson(IOUtils.toString(response.getEntity().getContent()), JsonElement.class).getAsJsonObject();
            jsonObject.get("error");

            //getStatusLine().getStatusCode() + "";
        } catch (Exception e) {
            e.printStackTrace();
            statusCode="500";
        }
        return statusCode;
    }

    //UTILS
    public static Observable<Integer> log(Iterable<String> logEntries){
        return Observable.just(List.ofAll(logEntries).map(s -> {
            System.out.println(s);
            return s;
        }).size());
    }

    //VALIDATION OPERATIONS
    public static <E,T> Tuple2<Iterable<E>, Iterable<T>> tuple(Iterable<Validation<E, T>> validations ){
        return List.ofAll(validations)
                .groupBy(Validation::isValid)
                .transform(validationsMap -> Tuple.of(validationsMap.get(false),validationsMap.get(true)))
                .map((errorOptions, validOptions) -> Tuple.of(errorOptions.getOrElse(List.empty()),validOptions.getOrElse(List.empty())))
                .map((errors, valids) -> Tuple.of(errors.map(Validation::getError),valids.map(Validation::get)));
    }

    public static <T> Validation<String,T> toValidation(Try<T> tryObject){
        return Validation.fromEither(tryObject.toEither().mapLeft(Throwable::getMessage));

    }
}
