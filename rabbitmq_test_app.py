# 导入aio_pika库和logging库
import aio_pika
import logging
import asyncio
import concurrent.futures
import threading
import queue

logging.basicConfig(level=logging.INFO)

# 定义一个生产者类
class Producer:
    # 初始化方法，接受一个参数字典，包含服务器地址，交换机名称，路由键等信息
    def __init__(self,connection, producer_config):
        self.connection = connection
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
        try:
            # 创建一个aio_pika的Message对象，使用参数字典中的编码方式
            message = aio_pika.Message(body=message.encode("utf-8"))
            # 使用exchange的publish方法，将message发送到指定的路由键
            await self.exchange.publish(message, routing_key=self.routing_key)
            # 使用logging记录发送成功的信息
            logging.info(f"Sent message {message.body} to {self.exchange_name} with routing key {self.routing_key}")
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
        self.connection = None
        self.params = params
        self.consumer = None
        self.producer = None
        # 创建事件循环
        self.new_loop_rabbitmq = asyncio.new_event_loop()
        self.handle_event = threading.Event()
        self.thread_message = None
        self.thread_rabbitmq = None
        self.queue = queue.Queue()
        self.listen()

    async def create_connection(self):
        connection_property = self.params["connection_config"]
        try:
            # 使用参数字典中的服务器地址创建一个连接
            self.connection = await aio_pika.connect_robust(**connection_property)
            return self.connection
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Client Failed to connect to {self.params['host']}: {e}")
            raise e

    async def close_connection(self):
        if self.connection is not None and not self.connection.is_closed():
            self.connection.close()
            logging.info(f"RabbiMQ Client closed to {self.params['host']}")

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

    # 放入新线程的函数，做一些准备工作
    def _prepare_thread(self):
        # 设置事件循环
        asyncio.set_event_loop(self.new_loop_rabbitmq)
        # 创建任务
        self.new_loop_rabbitmq.create_task(self.main())
        # 运行new_loop
        self.new_loop_rabbitmq.run_forever()

    # 异步方法，连接到rabbitmq服务器，分别调用生产者和消费者的connect方法
    def listen(self):
        # 开启线程，运行consumer和producer
        self.thread_rabbitmq = threading.Thread(name="thread_rabbitmq",target=self._prepare_thread)
        self.thread_rabbitmq.start()

    
    async def recevie_message(self, message):
        try:
            async with message.process():
                print("处理消息")
                # 模拟处理消息的耗时
                # 发送处理结果
                # 使用logging记录处理成功的信息
                logging.info(f"MessageHandler recevied message {message.body} ")
                #创建线程处理消息
                handle_msg = message.body
                self.thread_message = threading.Thread(target=self.handle_message,args=(handle_msg,))
                self.thread_message.start()
                #阻塞等待消息处理完成
                while not self.handle_event.is_set(): 
                    await asyncio.sleep(1)
                self.handle_event.clear()
                send_message = self.queue.get()
                await self._send_message(send_message)
                logging.info(f"{message.body} is sended")
        except Exception as e:
            # 使用logging记录处理失败的异常
            logging.error(f"MessageHandler Failed to handle message {message}: {e}")
            raise e


    # 异步方法，接受一个message参数，作为消费者的callback函数
    def handle_message(self, message):
        logging.info(f"MessageHandler are handling  message: {message} ")
        # 模拟处理消息的耗时
        import time
        for i in range(1200):
            print(f"第{i}秒 处理消息")
            time.sleep(1)
        send_message = "handle message"
        # 发送处理结果
        # 使用logging记录处理成功的信息
        self.queue.put(send_message)
        self.handle_event.set()




    # 异步方法
    async def _send_message(self, message):
        # 创建连接
        await self.producer.connect()
        # 使用生产者的publish方法，将消息发送到另一个交换机
        await self.producer.publish(message)
        # 使用logging记录发送成功的信息
        logging.info(f"Sent message {message} to exchange {self.params['producer_config']['exchange_name']}")
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
        print(f"正在使用的线程数： {threading.activeCount()}")
        print_threads()

        time.sleep(5)
        pass


