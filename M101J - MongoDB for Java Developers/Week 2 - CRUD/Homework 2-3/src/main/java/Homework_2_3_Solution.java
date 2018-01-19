import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;

public class Homework_2_3_Solution {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("students");
        MongoCollection<Document> grades = db.getCollection("grades");

        MongoCursor<Document> cursor = grades
                .find(eq("type", "homework"))
                .sort(ascending("student_id", "score"))
                .iterator();

        Object previousStudentId = null;
        try{
            while(cursor.hasNext()){
                Document entry = cursor.next();

                //If student_id is different - the current entry has the lowest score and is the one to delete
                if(!entry.get("student_id").equals(previousStudentId)){
                    grades.deleteOne(eq("_id", entry.get("_id")));
                }

                previousStudentId = entry.get("student_id");
            }

            AggregateIterable<Document> results = grades.aggregate(
                    asList(Aggregates.group("$student_id", avg("average", "$score")),
                        Aggregates.sort(descending("average")), limit(1)));

            System.out.println("The answer is: " + results.iterator().next().toJson());
        } finally{
            cursor.close();   
        }
        
        client.close();
    }
}
