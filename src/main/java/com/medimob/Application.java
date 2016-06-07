package com.medimob;

import com.medimob.web.WebServer;
import io.vertx.core.Vertx;

/**
 * Created by cyrille on 19/03/16.
 */
public class Application {

    public static void main(String[] args){
        Vertx.vertx().deployVerticle(new WebServer());
    }
}
