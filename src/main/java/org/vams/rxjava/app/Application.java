package org.vams.rxjava.app;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import rx.Observable;
import rx.observables.StringObservable;

import java.io.*;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;

/**
 * Created by vamsp7 on 27/03/16.
 */
public class Application {

    public static final String FILE_PATH = "/home/vamsp7/Downloads/popcorn-android/Inventory Sample Data/NewCarInventory_infiniti_US_en_20160308T074521.txt";

    public static void main(String[] args) throws FileNotFoundException {
        Observable<String> tweets = Observable.just("learning RxJava", "Writing blog about RxJava", "RxJava rocks!!");
        tweets.subscribe(Application::processMessage,(e) -> e.printStackTrace(), Application::processComplete);

        Observable<String> fileReadObservable2 = Application.getFileLineObservable(FILE_PATH);
        fileReadObservable2
                .filter(Application::validLine)
                .map(Application::toEntity)
                .buffer(100)
                .map(List::size)
                .subscribe(Application::processMessage,e -> System.out.println(e.getMessage()), Application::processComplete);

    }

    public static boolean validLine(String line){
        return !line.startsWith("#");
    }

    public static Object toEntity(String line){
        return line.replace("|","\t ");
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
}
