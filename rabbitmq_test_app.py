# 导入aio_pika库和logging库
import aio_pika
import logging
import asyncio
import concurrent.futures
import threading

logging.basicConfig(level=logging.INFO)

# 定义一个生产者类
class Producer:
    # 初始化方法，接受一个参数字典，包含服务器地址，交换机名称，路由键等信息
    def __init__(self, connecton,producer_config):
        self.connection = connecton
        self.channel = None
        self.exchange = None
        self.exchange_name = producer_config["exchange_name"]
        self.routing_key = producer_config["routing_key"]
    
    # 异步方法，连接到rabbitmq服务器，创建一个channel和一个exchange
    async def connect(self):
        try:
            # 创建一个channel
            self.channel = await self.connection.channel()
            # 获取一个已经存在的交换机
            self.exchange = await self.channel.get_exchange(self.exchange_name)
            # 使用logging记录连接成功的信息
            logging.info(f"RaabitMQ Producer Connected to exchange {self.exchange_name}")
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Producer Failed to connect to  exchange {self.exchange_name}: {e}")
            raise e
    
    # 异步方法，接受一个message参数，将message发送到exchange
    async def publish(self, message):
        # 获取当前线程的对象
        thread = threading.current_thread()
        # 打印线程的 id 和 name
        print(f"func is running in thread {thread.ident} {thread.name}")
        try:
            # 创建一个aio_pika的Message对象，使用参数字典中的编码方式
            # message = aio_pika.Message(body=message.encode("utf-8"))
            # 使用exchange的publish方法，将message发送到指定的路由键
            await self.exchange.publish(message, routing_key=self.routing_key)
            # 使用logging记录发送成功的信息
            logging.info(f"Sent message {message} to {self.exchange_name} with routing key {self.routing_key}")
        except Exception as e:
            # 使用logging记录发送失败的异常
            logging.error(f"RabbitMQ Producer Failed to send message {message.body}: {e}")
            raise e
    
    # 异步方法，关闭连接
    async def close(self):
        # 使用connection的close方法，关闭连接
        if not self.channel.is_closed:
            await self.channel.close()


# 定义一个消费者类
class Consumer:
    # 初始化方法，接受一个参数字典，包含服务器地址，交换机名称，队列名称，路由键等信息
    def __init__(self, connection,consumer_config):
        self.connection = connection
        self.queue_name = consumer_config["queue_name"]
        self.channel = None
        self.queue = None
        self.queue_auto_delete = True

    
    # 异步方法，连接到rabbitmq服务器，创建一个channel和一个queue
    async def connect(self):
        try:
            # 创建一个channel
            self.channel = await self.connection.channel()
            # 设置消费者每次从队列中获取的消息数量为1,不能太大，否则会导致装饰器递归错误
            await self.channel.set_qos(prefetch_count=1)
            # 获取一个已经存在的队列
            self.queue = await self.channel.get_queue(self.queue_name)
            self.queue_auto_delete = self.queue.auto_delete
            # 将队列绑定到交换机，使用参数字典中的交换机名称和路由键
            # await self.queue.bind(self.params["exchange_name"], self.params["routing_key"])
            # 使用logging记录连接成功的信息
            logging.info(f"RaabitMQ Consumer Connected to  queue {self.queue_name}")
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Consumer Failed to connect to {self.queue_name}: {e}")
            raise e
    
    # 异步方法，接受一个callback参数，使用aio_pika的consume方法，将callback作为消息处理函数，要求新建一个线程运行这个callback函数
    async def start_consuming(self, callback):
        try:
            # 使用queue的consume方法，传入callback函数和参数字典中的编码方式，返回一个消费者对象
            await self.queue.consume(callback)
            # 使用logging记录开始消费的信息
            logging.info(f"RaabitMQ Consumer Started consuming messages from queue {self.queue_name}")
            # 返回消费者对象
        except Exception as e:
            # 使用logging记录开始消费失败的异常
            logging.error(f"RaabitMQ Consumer Failed to start consuming messages from queue {self.queue_name}: {e}")
            raise e

    # 异步方法，关闭连接
    async def close(self):
        # 使用connection的close方法，关闭连接
        if not self.channel.is_closed:
            await self.channel.close()

# 定义一个MessageHandler类
class MessageHandler:
    # 初始化方法，接受两个参数字典，分别传给生产者和消费者的初始化方法
    def __init__(self, params):
        self.params = params
        self.connection = None
        self.consumer = None
        self.producer = None
        self._rabbit_message = {"exce": "empty","status": "empty", "received_message":None,"send_message":None} # {"status":["empty","received","send","done"]}
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        # 创建事件循环
        self.new_loop_handlemessage = asyncio.new_event_loop()
        # 创建事件循环
        self.new_loop_rabbitmq = asyncio.new_event_loop()

        self.listen()

    async def create_connection(self):
        self.connection_property = self.params["connection_config"]
        try:
            # 使用参数字典中的服务器地址创建一个连接
            self.connection = await aio_pika.connect_robust(**self.connection_property)
            return self.connection
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Client Failed to connect to {self.params['host']}: {e}")
            raise e

    async def close_connection(self):
        if self.connection is not None and not self.connection.is_closed():
            self.connection.close()
            logging.info(f"RabbiMQ Client closed to {self.params['host']}")

    @property
    def rabbit_message(self):
        return self._rabbit_message

    @rabbit_message.setter
    def rabbit_message(self, dict_value: dict):  # 确保都是在self.new_loop_rabbitmq中.
        for key in dict_value.keys():
            self.rabbit_message[key] = dict_value[key]
 
        if self._rabbit_message["exce"] == "done":
            print("确认消息")
            message = self._rabbit_message["received_message"]
            asyncio.run(message.ack())
        elif self._rabbit_message["exce"] == "received":
            print("处理消息")
            # self.task_handle_message =  self.thread_pool.submit(self.handle_message)  # 确保退出self.new_loop_handlemessage
            self.thread_handle_message = threading.Thread(name="thread_handle_message",target=self.handle_message)
            self.thread_handle_message.start()
        elif self._rabbit_message["exce"] == "send":
            print("发送消息")
            message = self._rabbit_message["send_message"]
            loop = asyncio.get_running_loop()
            loop.create_task(self._send_message(message=message))
            
            # asyncio.run(self._send_message(message=message))
 
    # 异步方法，打包异步函数
    async def main(self):
        print("启动rabbitMQ的消费者")
        # 创建连接
        self.connection = await self.create_connection()
        # 创建 consumer 发布者
        self.producer = Producer(self.connection, self.params['producer_config'])
        self.consumer = Consumer(self.connection, self.params['consumer_config'])
        # 连接到rabbitmq服务器
        await self.consumer.connect()
        # 注册消息处理函数
        await self.consumer.start_consuming(self.recevie_message)      

    async def choose_behavior(self):
        while True:
            print(f"执行： {self.rabbit_message['exce']}")
            self.rabbit_message["exce"] = self.rabbit_message["status"]
            await asyncio.sleep(1)

    # 放入新线程的函数，做一些准备工作
    def _prepare_thread(self):
        # 设置事件循环
        asyncio.set_event_loop(self.new_loop_rabbitmq)
        # 创建任务
        self.new_loop_rabbitmq.create_task(self.main())
        # 开始选择行为
        self.new_loop_rabbitmq.create_task(self.choose_behavior())
        # 运行new_loop
        self.new_loop_rabbitmq.run_forever()

    # 异步方法，连接到rabbitmq服务器，分别调用生产者和消费者的connect方法
    def listen(self):
        # self.task_rabbitmq = self.thread_pool.submit(self._prepare_thread)
        self.thread_rabbitmq = threading.Thread(name="thread_rabbitmq",target=self._prepare_thread)
        self.thread_rabbitmq.start()

    # 异步方法，接受一个message参数，作为消费者的callback函数
    async def recevie_message(self, message):
        try:
            print(message.body)
            self.rabbit_message = {"status": "received", "received_message": message}
            # 使用logging记录处理成功的信息
            logging.info(f"MessageHandler recevied message {message.body} and update")
        except Exception as e:
            # 使用logging记录处理失败的异常
            logging.error(f"MessageHandler Failed to handle message {message.body}: {e}")
            raise e

    def handle_message(self):
        print("启动消息处理")
        #设置事件循环
        asyncio.set_event_loop(self.new_loop_handlemessage)
        # 运行new_loop
        self.new_loop_handlemessage.run_until_complete(self._handle_message())
        
    async def _handle_message(self):
        print("处理消息")
        # 模拟处理消息的耗时
        for i in range(3):
            print(f"第{i}秒 处理消息")
            await asyncio.sleep(1)
        send_message = "payload"
        self.rabbit_message = {"status": "send", "send_message": send_message}
        print("处理完成")
        return True

    # 异步方法
    async def _send_message(self, message):
        # 创建连接
        await self.producer.connect()
        # 使用生产者的publish方法，将消息发送到另一个交换机
        await self.producer.publish(message)
        # 使用logging记录发送成功的信息
        logging.info(f"Sent message {message} to exchange {self.params['producer_config']['exchange_name']}")
        self.rabbit_message = {"status": "done", "received_message": None, "send_message": None}
        # 关闭连接
        await self.producer.close()



if __name__ == "__main__":
    consumer_config = {
        "host": "localhost",
        "port":5672,
        "virtualhost":"OOCL",
        "login": "OOCL",
        "password":"123321",
        "ssl": False,
        "timeout":5,
        "exchange_name": "SANL",
        "exchange_type": "direct",
        "queue_name": "CUS.TO.APP",
        "routing_key": "SERVER.TO.APP",
    }

    producer_config = {
        "host": "localhost",
        "port":5672,
        "virtualhost":"OOCL",
        "login": "OOCL",
        "password":"123321",
        "ssl": False,
        "timeout":5,
        "exchange_name": "SANL",
        "exchange_type": "direct",
        "queue_name": "APP.TO.CUS",
        "routing_key": "APP.TO.SERVER",
    }

    param = {
        "connection_config":{
            "host": "localhost",
            "port": 5672,
            "virtualhost": "OOCL",
            "login": "OOCL",
            "password": "123321",
            "ssl": False,
            "timeout": 5
        },
        "consumer_config":{
            "queue_name":"CUS.TO.APP"
        },
        "producer_config":{
            "exchange_name":"SANL",
            "routing_key":"APP.TO.SERVER"
        }
    }



    message_handler = MessageHandler(param)
 

    import time
    import threading


    def print_threads():
        threads = threading.enumerate()
        for thread in threads:
            print(thread.name)

    while True:
        print("这是主函数...")
        print(f"最大线程数量:  {message_handler.thread_pool._max_workers}")
        print(f"正在使用的线程数： {threading.activeCount()}")
        print_threads()

        time.sleep(5)
        pass


