# IS ASSIGNMENT 3
## Instructions
### Containers Start and Stop

- If it is the first time the containers are going to be run then it will be necessary to build them: ```docker compose up --build``` and ```^C``` to stop them.

- If not the first time:
    - And you want the containers to run in the standard mode: ```docker compose up``` and then ```^C``` to stop them.

    - If you want them to be run in the background: ```docker compose up -d``` and then ```docker compose down``` to stop them.

### Access a Container

- To access the server container: ```docker compose exec server /bin/zsh```.
<!-- - To access the client container: ```docker compose exec client /bin/zsh```. -->

### REST Server
- From the server container workspace run: ```mvn spring-boot:run```.

### Kafka-Connect
- If it is the first time, perform the following commands inside the container workspace: ```cd src/main/resources/kafka-connect/config```, ```chmod +x create_connectors.sh```, ```./create_connectors.sh```.
- To consume the dbinfo topics (the source ones): ```kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic dbinforoute_capacity --from-beginning```, ```kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic dbinforoute_supplier --from-beginning```.

### Run the Code
- If it is the first time or changes were made to the code, it will be necessary to compile: ```mvn compile```.
- To run the intended Java file, follow this example changing after "assignment3 word to whatever package desired": ```mvn exec:java -Dexec.mainClass="pt.is.assignment3.kafka.producers.RoutesProducer"```

### Testing
- To check the output of a topic (example)run:```kafka-console-consumer.sh --bootstrap-server broker1:9092 \
--topic resultstopic \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer```
- To descibe a topic run: ```kafka-topics.sh --describe --bootstrap-server broker1:9092 --topic resultstopic``` 
## Recommended VSCode Extensions

- **[Extension Pack for Java](https://marketplace.visualstudio.com/items?itemName=vscjava.vscode-java-pack)**  
   Popular extensions for Java development that provides Java IntelliSense, debugging, testing, Maven/Gradle support, project management and more.

- **[Better Comments](https://marketplace.visualstudio.com/items?itemName=aaron-bond.better-comments)**     
    Improve your code commenting by annotating with alert, informational, TODOs, and more!

- **[Docker](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-docker)**     
    Makes it easy to create, manage, and debug containerized applications.

- **[Error Lens](https://marketplace.visualstudio.com/items?itemName=usernamehw.errorlens)**        
    Improve highlighting of errors, warnings and other language diagnostics.

- **[GitLens — Git supercharged](https://marketplace.visualstudio.com/items?itemName=eamodio.gitlens)**     
    Supercharge Git within VS Code — Visualize code authorship at a glance via Git blame annotations and CodeLens, seamlessly navigate and explore Git repositories, gain valuable insights via rich visualizations and powerful comparison commands, and so much more

- **[Lombok Annotations Support for VS Code](https://marketplace.visualstudio.com/items?itemName=vscjava.vscode-lombok)**   
    Refactor code with Lombok annotations, or remove Lombok annotations with actual methods.

- **[Red Hat Dependency Analytics](https://marketplace.visualstudio.com/items?itemName=redhat.fabric8-analytics)**  
    Provides insights on security vulnerabilities in your application dependencies.
 