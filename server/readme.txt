========================================================================================================================
=======================================doc of azure iot for python======================================================
========================================================================================================================
仅针对azure iothub 内置的eventhub

首先端设备在azure iothub 中注册device， 并生成device的连接字符串，端设备通过conn_str 与云进行通行（d2c message）

云端可以通过iothub中的内置endpoint 来获取来自端设备的消息(个人理解：一个hub只有一个eventhub endpoint，但是hub 可以建立多个连接多线程消费 设备端发来的消息)

云端通过hub conn_str(一个hub是相同一串)连接iothub 并指定对应的device 来发送消息(c2d message)；
同时可以指定过期时间 格式 “”“props.update(expiryTimeUtc=1678359410818)”“” 向 Message property中发送过期时间，时间参数是时间戳(精确到毫秒[time.time() 的后三位])
经测试，消息会在到期后自动从C2D 队列中删除

在云端可以通过函数 reps = registry_manager.get_device('car') 去 检测 id是car 的端设备的状态 包括：
    ['device_id': 'car', 'connection_state': 'Disconnected', 'cloud_to_device_message_count': 1
    'connection_state_updated_time': datetime.datetime(2023, 3, 9, 9, 21, 2, 445532, tzinfo=<isodate.tzinfo.Utc object at 0x00000244891850A0>),
    'last_activity_time'] 等

在云端也可以调用[response = registry_manager.protocol.cloud_to_device_messages.purge_cloud_to_device_message_queue('car')] 方法去删除队列中的C2D消息
    ['as_dict', 'deserialize', 'device_id', 'from_dict', 'is_xml_model', 'module_id', 'serialize', 'total_messages_purged', 'validate']


c2d 的消息可以设置ack， 指需要反馈消息(及是否被device端消费的状态) 使用[iothub-ack] 包括negotive, positive, full 等，默认不反馈消息
调用方法参考清空队列消息
