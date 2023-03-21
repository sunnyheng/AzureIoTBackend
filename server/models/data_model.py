# coding: utf-8

import json



class DataModel(object):
    def __init__(self):
        super().__init__()
        self.content = [{"user":[]}]
        self.deletedID = []

    def set_content(self, data):
        if isinstance(data, list):
            self.content = data

    def set_deleted_id(self, deleted_id):
        if isinstance(deleted_id, list):
            self.deletedID = deleted_id

    def __repr__(self):
        if self.content != "[]" and self.deleted_id != "[]":
            return "The data content: %s, deleted id: %s" %(str(self.content), str(self.deletedId))

        elif self.content != "[]":
            return "Not delete message ,the data content: %s, ." % str(self.content)

        else:
            return "Delete message, delete id:%s." % str(self.deletedId)
