use m101;

db.sysprofile.find({ns : "school2:students"}).sort({millis : -1}).limit(1).pretty;