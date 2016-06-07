## Project Configuration :


Run configuration : 

* Main Class : ``io.vertx.core.Launcher``
* JVM Options : ``-Djava.util.logging.config.file=/home/cyrille/IdeaProjects/server-parent/src/main/resources/vertx-default-jul-logging.properties``
* Program args : ``run com.medimob.web.WebServer --redeploy=**/*.class --launcher-class=io.vertx.core.Launcher --java-opts="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8000" -cp build/classes/main -conf config.json``