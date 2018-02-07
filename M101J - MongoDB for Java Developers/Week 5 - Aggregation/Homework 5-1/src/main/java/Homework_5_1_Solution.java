import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.bson.Document;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static java.util.Arrays.asList;

public class Homework_5_1_Solution {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("blog");
        MongoCollection<Document> posts = db.getCollection("posts");

        posts.aggregate(asList(
                unwind("$comments"),
                group("$comments.author", sum("comments", 1)),
                sort(new Document("comments", -1)),
                limit(1)
        )).forEach(printBlock);

        client.close();
    }

    static Block<Document> printBlock = new Block<Document>() {
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
