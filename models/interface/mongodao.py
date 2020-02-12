class MongoDAO:
    def __init__(self, collection):
        self._collection = collection

    def get_all(self, skip=0, limit=0):
        result_set = self._collection.find().skip(skip).limit(limit)
        return result_set

    def get(self, _id):
        filters = {"_id": _id}
        return self.get_first(filters)

    def get_many(self, ids):
        filters = {"_id": {"$in": ids}}
        return self.get_many_by(filters)

    def get_many_by(self, filters):
        return self._collection.find(filters)

    def get_first(self, filters):
        return self._collection.find_one(filters)

    def delete_one(self, _id):
        r = self._collection.delete_one({"_id": _id})
        return r.deleted_count

    def delete_many(self, ids):
        r = self._collection.delete_many({"_id": {"$in": ids}})
        return r.deleted_count

    def save_one(self, item):
        if "_id" in item and not item["_id"]:
            item.pop("_id")
        r = self._collection.insert_one(item)
        return r.inserted_id

    def save_many(self, items):
        for item in items:
            if "_id" in item and not item["_id"]:
                item.pop("_id")
        r = self._collection.insert_many(items)
        return r.inserted_ids

    def update_one(self, _id, data):
        r = self._collection.update_one({"_id": _id}, {"$set": data}, upsert=False)
        return r.modified_count

