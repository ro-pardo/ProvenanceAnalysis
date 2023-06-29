package org.thesis;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Main {

    private static Jedis jedis;

    public static void main(String[] args) {

        jedis = new Jedis("localhost", 6379);
        jedis.connect();
//
//
//        /*
//            Output,id:10,dim:6
//            Input,id:2,dim:6
//        */
//

        String jsonString = "{\"discarded\": null,\"passed\": [{\"operator\": \"reduce\",\"value\": \"(N,O,53.0,47753.0,44221.08,46286.172000000006,53.0,47753.0,0.13,2)\"}],\"parents\": [0, 1],\"keys\": [\"NO\"]}";

        String requiredKey = "10";
        String elem = jedis.get(requiredKey);

        // Create a Gson instance
        Gson gson = new Gson();

        // Define the type for parsing the JSON
        Type responseType = new TypeToken<LineageNode>() {}.getType();


        LineageNode response = gson.fromJson(elem, responseType);

        // Access the parsed data
        System.out.println("Discarded: " + response.getDiscarded());
        System.out.println("Passed: " + response.getPassed());
        System.out.println("Parents: " + response.getParents());
        System.out.println("Keys: " + response.getKeys());

        outputDimensionTracker(requiredKey, response);
//        trackSources(requiredKey, response);
    }

    private static void trackSources(String requiredKey, LineageNode response) {
    }

    private static void outputDimensionTracker(String requiredKey, LineageNode node) {
        System.out.println("Arriving successfully!");

        // TODO: Esta escrita como lista de maps con un nodo, no como un map con multiples donde el operador es el key
        Map<String, String> operatorsExecuted = node.getPassed().get(0);

        System.out.println("Operations performed");
        operatorsExecuted.entrySet().iterator().forEachRemaining(
                t -> {
                    System.out.println(
                            requiredKey + ": " + t.getKey() + " - " + t.getValue()
                    );

                    // Habria que buscar segun el nombre de la operacion final en el nodo queriado
                    // todo: Orden de operaciones pipelineadas no se guarda ni aqui ni en AstParser (Creo que en parser podria ser mas simple)
                    // todo: Para hacer Backtrack en cualquier nodo, tenemos que guardar la ultima operacion realizada en cada nodo guardando redis
                    if(Objects.equals(t.getKey(), "operator") && Objects.equals(t.getValue(), "result")){
                        // Podriamos crear un tipo de datos que guarde (por tupla) las dimensiones iniciales (finales del sink o  operador a revisar)
                        // y despues sus transformaciones ... Solo hay un sink asi que es sencillo

                        // Despues llegaremos al 9:reduce
                        // Habria que guardar la relacion entre las dimensiones de 9 y 10 (en el tipo de dato por tupla)

                        //...
                        //Llegariamos a los struct as tup y guardariamos las sink dim y condiciones por operador
                    }

                    //CORTE
                    if(Objects.equals(t.getKey(), "operator") && Objects.equals(t.getValue(), "struct_as_tup")){

                        // aqui habria que checar en el struct_as_tup:

                    }
                }
        );

        if (node.getParents() != null && !node.getParents().isEmpty()) {
            node.getParents().stream().forEach(t -> {

                String elem = jedis.get(String.valueOf(t));

                // Create a Gson instance
                Gson gson = new Gson();

                // Define the type for parsing the JSON
                Type responseType = new TypeToken<LineageNode>() {
                }.getType();


                LineageNode response = gson.fromJson(elem, responseType);

//                checkResponseAsSource(response);

                outputDimensionTracker(String.valueOf(t), response);
            });
        }
    }

    private static List<String> checkResponseAsSource(LineageNode response) {
        List<String> source_value = (List<String>) response.getPassed().stream().filter(t -> Objects.equals(t.get("operator"), "source_operator")).map(t -> t.values());
        return source_value;
    }

}