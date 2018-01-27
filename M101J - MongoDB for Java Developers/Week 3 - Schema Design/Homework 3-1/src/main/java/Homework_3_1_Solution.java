import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.List;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;

public class Homework_3_1_Solution {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("school");
        MongoCollection<Document> students = db.getCollection("students");

        MongoCursor<Document> cursor = students.find().iterator();

        try{
            while(cursor.hasNext()){
                Document entry = cursor.next();

                List<Document> scores = (List<Document>) entry.get("scores");

                Double minHomeworkScore = Double.MAX_VALUE;
                Document minHomeworkScoreDocument = null;

                for(Document doc : scores){
                    String type = doc.getString("type");
                    Double score = doc.getDouble("score");

                    if(type.equals("homework") && score < minHomeworkScore){
                        minHomeworkScore = score;
                        minHomeworkScoreDocument = doc;
                    }
                }

                //remove the lowest score
                if(minHomeworkScoreDocument != null){
                    scores.remove(minHomeworkScoreDocument);
                }

                //update the record
                students.updateOne(Filters.eq("_id", entry.get("_id")),
                        new Document("$set", new Document("scores", scores)));
            }

            //get student with the highest average in the class
            AggregateIterable<Document> results = students.aggregate(asList(
                        unwind("$scores"),
                        group("$_id", avg("average", "$scores.score")),
                        sort(descending("average")), limit(1)));

            System.out.println("The answer is: " + results.iterator().next().toJson());
        } finally{
            cursor.close();   
        }
        
        client.close();
    }
}
