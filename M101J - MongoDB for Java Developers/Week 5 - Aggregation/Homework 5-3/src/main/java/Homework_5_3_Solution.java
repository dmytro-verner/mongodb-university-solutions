import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.bson.Document;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;

public class Homework_5_3_Solution {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> grades = db.getCollection("grades");

        grades.aggregate(asList(
                unwind("$scores"),
                match(nin("scores.type", "quiz")),
                group(new Document("student_id", "$student_id")
                        .append("class_id", "$class_id"),
                        avg("studentClassAvgScore", "$scores.score")),
                group(new Document("class_id", "$_id.class_id"),
                        avg("classAvgScore", "$studentClassAvgScore")),
                sort(descending("classAvgScore"))
        )).forEach(printBlock);

        client.close();
    }

    static final Block<Document> printBlock = new Block<Document>() {
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
