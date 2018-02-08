import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.bson.Document;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static java.util.Arrays.asList;

public class Homework_5_2_Solution {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> zips = db.getCollection("zips");

        zips.aggregate(asList(
                match(in("state", "CA", "NY")),
                group(new Document("state", "$state").append("city", "$city"), sum("totalPop", "$pop")),
                match(gt("totalPop", 25000)),
                group(new Document("state", "$state").append("city", "$city"),avg("averagePopulation", "$totalPop")),
                project(new Document("_id", 0))
        )).forEach(printBlock);

        client.close();
    }

    static final Block<Document> printBlock = new Block<Document>() {
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
