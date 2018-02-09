import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.bson.Document;
import static java.util.Arrays.asList;
import static org.bson.Document.parse;

public class Homework_5_4_Solution {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> zips = db.getCollection("zips");

        zips.aggregate(asList(
                parse("{$project: {first_char: {$substr: [\"$city\", 0, 1]}, city: 1, pop: 1, zip: \"$_id\", state: 1}}"),
                parse("{$match: {\"first_char\": {'$in': [\"B\", \"D\", \"O\", \"G\", \"N\", \"M\"]}}}"),
                parse("{$group: {_id : null, pop : {$sum: \"$pop\"}}}"),
                parse("{$project: {_id: 0}}")
        )).forEach(printBlock);

        client.close();
    }

    static final Block<Document> printBlock = new Block<Document>() {
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
