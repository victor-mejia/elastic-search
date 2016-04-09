package org.vams.javaslang.app;

import javaslang.collection.List;

/**
 * Created by vicmejia on 8/04/16.
 */
public class Application {

    public static void main (String... args){

        List.of(1,2,3,4,5,6,7,8,9,10,11)
                .sliding(3,1)
                .forEach(System.out::println);


    }

}
