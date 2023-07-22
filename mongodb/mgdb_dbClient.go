package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func fillMap(cur mongo.Cursor) (resultMap []map[string]string, err error) {

	var results []bson.M
	if err = cur.All(context.TODO(), &results); err != nil {
		return nil, err
	}

	for _, result := range results {
		resultM := make(map[string]string)
		cur.Decode(&result)

		for k, v := range result {
			field, ok := v.(string)
			if ok {
				resultM[k] = field
			} else {
				return nil, fmt.Errorf("Wrong data type: %v, %v\n", k, v)
			}
		}
		resultMap = append(resultMap, resultM)
	}

	return resultMap, nil
}

func dbInsert(db *mongo.Database, table string, key string,
	values map[string]string) (err error) {

	coll := db.Collection(table)
	insValues := bson.D{{"_id", key}}

	for f, v := range values {
		insValues = append(insValues, bson.E{f, v})
	}

	if _, err = coll.InsertOne(context.TODO(), insValues); err != nil {
		return err
	}
	return nil

}

func dbRead(db *mongo.Database, table string, key string,
	fields map[string]string) (results []map[string]string, err error) {

	coll := db.Collection(table)
	filter := bson.D{}
	if key == "" {
		log.Errorf("Find all in table", coll.Name())
	} else {
		filter = bson.D{{"_id", key}}
	}

	cursor, err := coll.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}

	results, err = fillMap(*cursor)
	if err != nil {
		return nil, err
	}

	if _, ok := fields["<all fields>"]; !ok {
		log.Errorf("Unsupported field projection")
	}

	if len(results) == 0 {
		log.Debugf("_id %s does not exist in table %s\n", key, coll.Name())
		return results, nil
	}

	return results, nil
}

func dbUpdate(db *mongo.Database, table string, key string,
	values map[string]string) (err error) {

	coll := db.Collection(table)
	filter := bson.D{{"_id", key}}
	var updateValues bson.D
	for f, v := range values {
		updateValues = append(updateValues, bson.E{f, v})
	}

	_, err = coll.UpdateMany(
		context.TODO(),
		filter,
		bson.D{{"$set", updateValues}},
	)
	if err != nil {
		return err
	}

	return nil
}

func dbScan(db *mongo.Database, table string, startkey string, recordcount int,
	fields map[string]string) (results []map[string]string, err error) {

	coll := db.Collection(table)
	filter := bson.D{{"_id", bson.D{{"$gte", startkey}}}}
	sort := bson.D{{"_id", 1}}

	cursor, err := coll.Find(
		context.TODO(),
		filter,
		options.Find().SetSort(sort),
		options.Find().SetLimit(int64(recordcount)),
	)
	if err != nil {
		return nil, err
	}
	if _, ok := fields["<all fields>"]; !ok {
		log.Errorf("Unsupported field projection")
	}

	results, err = fillMap(*cursor)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func dbDelete(db *mongo.Database, table string, key string) (err error) {
	coll := db.Collection(table)
	filter := bson.D{{"_id", key}}
	if key == "" {
		filter = bson.D{{}}
	}
	_, err = coll.DeleteMany(context.TODO(), filter)

	return err
}

func dbDrop(db *mongo.Database, table string) (err error) {
	coll := db.Collection(table)
	err = coll.Drop(context.TODO())

	return
}
