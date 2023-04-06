# coding: utf-8

import json


class DataModel(object):
    def __init__(self):
        super().__init__()
        self.content = []
        self.deletedID = []
        # userBought is user attr
        # self.userBought = False
        # buyCount is square attr
        # self.buyCount = 0

    def set_content(self, data):
        if isinstance(data, list):
            self.content = data

    def set_deleted_id(self, deleted_id):
        if isinstance(deleted_id, list):
            self.deletedID = deleted_id

    # def set_user_bought(self, is_buy):
    #     self.userBought = is_buy
    #
    # def set_buy_count(self, count):
    #     self.buyCount = count

    def __repr__(self):
        if self.content != "[]" and self.deleted_id != "[]":
            return "The data content: %s, deleted id: %s" %(str(self.content), str(self.deletedId))

        elif self.content != "[]":
            return "Not delete message ,the data content: %s, ." % str(self.content)

        else:
            return "Delete message, delete id:%s." % str(self.deletedId)


# content_list is list, deleted_id_list is list, scenario_type is one of ["ScenarioUser", "ScenarioSquare"]
def convert_data_model(content_list, deleted_id_list, scenario_type):
    data = {}
    data_model = DataModel()
    if deleted_id_list:
        data_model.set_deleted_id(deleted_id_list)
    if content_list:
        data_model.set_content(content_list)
    data[scenario_type] = data_model.__dict__
    return data
