package org.vams.elasticsearch.app;

import org.elasticsearch.action.ActionListener;

/**
 * Created by vamsp7 on 25/03/16.
 */
public interface ActionListenerElasticSearch<T> extends ActionListener<T>{
    void onResponse(T var1);


    default void onFailure(Throwable var1){
        var1.printStackTrace();
        onResponse(null);
    }
}
