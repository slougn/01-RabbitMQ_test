# 导入aio_pika库和logging库
import aio_pika
import logging
import asyncio
import threading

logging.basicConfig(level=logging.INFO)

# 定义一个生产者类
class Producer:
    # 初始化方法，接受一个参数字典，包含服务器地址，交换机名称，路由键等信息
    def __init__(self, params):
        self.params = params
        self.connection = None
        self.channel = None
        self.exchange = None
    
    # 异步方法，连接到rabbitmq服务器，创建一个channel和一个exchange
    async def connect(self):
        self.connection_property = {
            "host": self.params['host'],
            "port":self.params['port'],
            "virtualhost":self.params['virtualhost'],
            "login": self.params['login'],
            "password":self.params['password'],
            "ssl": self.params['ssl'],
            "timeout":self.params['timeout']
        }
        try:
            # 使用参数字典中的服务器地址创建一个连接
            self.connection = await aio_pika.connect_robust(**self.connection_property)
            # 创建一个channel
            self.channel = await self.connection.channel()
            # 获取一个已经存在的交换机
            self.exchange = await self.channel.get_exchange(self.params["exchange_name"])
            # 使用logging记录连接成功的信息
            logging.info(f"RaabitMQ Producer Connected to {self.params['host']} and created exchange {self.params['exchange_name']}")
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Producer Failed to connect to {self.params['host']}: {e}")
            raise e
    
    # 异步方法，接受一个message参数，将message发送到exchange
    async def publish(self, message):
        try:
            # 创建一个aio_pika的Message对象，使用参数字典中的编码方式
            # message = aio_pika.Message(body=message.encode("utf-8"))
            # 使用exchange的publish方法，将message发送到指定的路由键
            await self.exchange.publish(message, routing_key=self.params["routing_key"])
            # 使用logging记录发送成功的信息
            logging.info(f"Sent message {message.body} to {self.params['exchange_name']} with routing key {self.params['routing_key']}")
        except Exception as e:
            # 使用logging记录发送失败的异常
            logging.error(f"Failed to send message {message.body}: {e}")
            raise e
    
    # 异步方法，关闭连接
    async def close(self):
        # 使用connection的close方法，关闭连接
        if not self.connection.is_closed:
            await self.connection.close()


# 定义一个消费者类
class Consumer:
    # 初始化方法，接受一个参数字典，包含服务器地址，交换机名称，队列名称，路由键等信息
    def __init__(self, params):
        self.params = params
        self.connection = None
        self.channel = None
        self.queue = None
        self.queue_auto_delete = True
        # 创建一个事件循环
        self.new_loop = asyncio.new_event_loop()
    
    # 异步方法，连接到rabbitmq服务器，创建一个channel和一个queue
    async def connect(self):
        self.connection_property = {
            "host": self.params['host'],
            "port":self.params['port'],
            "virtualhost":self.params['virtualhost'],
            "login": self.params['login'],
            "password":self.params['password'],
            "ssl": self.params['ssl'],
            "timeout":self.params['timeout'],
        }
        try:
            # 使用参数字典中的服务器地址创建一个连接
            self.connection = await aio_pika.connect_robust(**self.connection_property)
            # 创建一个channel
            self.channel = await self.connection.channel()
            # 设置消费者每次从队列中获取的消息数量为1,不能太大，否则会导致装饰器递归错误
            await self.channel.set_qos(prefetch_count=1)
            # 获取一个已经存在的队列
            self.queue = await self.channel.get_queue(self.params["queue_name"])
            self.queue_auto_delete = self.queue.auto_delete
            # 将队列绑定到交换机，使用参数字典中的交换机名称和路由键
            await self.queue.bind(self.params["exchange_name"], self.params["routing_key"])
            # 使用logging记录连接成功的信息
            logging.info(f"RaabitMQ Consumer Connected to {self.params['host']} and bound queue {self.params['queue_name']} to exchange {self.params['exchange_name']} with routing key {self.params['routing_key']}")
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Consumer Failed to connect to {self.params['host']}: {e}")
            raise e
    
    # 异步方法，接受一个callback参数，使用aio_pika的consume方法，将callback作为消息处理函数，要求新建一个线程运行这个callback函数
    async def start_consuming(self, callback):
        logging.info("RaabitMQ Consumer I am start_consuming...")
        try:
            # 使用queue的consume方法，传入callback函数和参数字典中的编码方式，返回一个消费者对象
            await self.queue.consume(callback)
            # 使用logging记录开始消费的信息
            logging.info(f"RaabitMQ Consumer Started consuming messages from queue {self.params['queue_name']}")
            # 返回消费者对象
        except Exception as e:
            # 使用logging记录开始消费失败的异常
            logging.error(f"RaabitMQ Consumer Failed to start consuming messages from queue {self.params['queue_name']}: {e}")
            raise e

    # 异步方法，关闭连接
    async def close(self):
        # 使用connection的close方法，关闭连接
        if not self.connection.is_closed:
            await self.connection.close()

# 定义一个MessageHandler类
class MessageHandler:
    # 初始化方法，接受两个参数字典，分别传给生产者和消费者的初始化方法
    def __init__(self, producer_params, consumer_params):
        self.producer = Producer(producer_params)
        self.consumer = Consumer(consumer_params)
        self.new_loop = asyncio.new_event_loop()
        self.listen()
        
    # 异步方法，打包异步函数
    async def main(self):
        # 连接到rabbitmq服务器
        await self.consumer.connect()
        # 注册消息处理函数
        await self.consumer.start_consuming(self.handle_message)

    # 放入新线程的函数，做一些准备工作
    def _prepare_thread(self):
        # 设置事件循环
        asyncio.set_event_loop(self.new_loop)
        # 创建任务
        self.new_loop.create_task(self.main())
        # 运行new_loop
        self.new_loop.run_forever()

    # 异步方法，连接到rabbitmq服务器，分别调用生产者和消费者的connect方法
    def listen(self):
        thread = threading.Thread(target=self._prepare_thread, args=())
        thread.start()
    
    # 异步方法，接受一个message参数，作为消费者的callback函数
    async def handle_message(self, message):
        # logging.info("MessageHandler There is a new thread to handle message...")
        try:
            async with message.process():
                if self.consumer.connection is not None and not self.consumer.connection.is_closed:
                    await self.consumer.connection.send_heartbeat()
                    logging.info("MessageHandler send_heartbeat...")
                # 使用logging记录接收到的消息
                logging.info(f"MessageHandler Received message {message.body} from queue {self.consumer.params['queue_name']}")
                # 对消息进行一些处理，例如转换成大写
                # message = message.body.upper()
                print(message)
                # 使用生产者的publish方法，将处理后的消息发送到另一个交换机
                await self.simulate_handle_message()
                if self.consumer.connection is not None and not self.consumer.connection.is_closed:
                    await self.consumer.connection.send_heartbeat()
                    logging.info("MessageHandler send_heartbeat...")

                await self._send_message(message)
                # 使用logging记录处理成功的信息
                logging.info(f"MessageHandler Handled message {message.body} and sent it to exchange {self.producer.params['exchange_name']}")
                print(f"self.consumer.need_auto_delete: {self.consumer.queue_auto_delete}")
                

                
        except Exception as e:
            # 使用logging记录处理失败的异常
            logging.error(f"MessageHandler Failed to handle message {message.body}: {e}")
            raise e

    # 异步方法，创建新线程，执行消息处理，但是只能创建一个线程，处理完毕后，关闭线程，再重新开启线程。
    async def simulate_handle_message(self):
        import time
        # 模拟处理消息的耗时
        for i in range(300):
            print(f"第{i}秒")
            time.sleep(1)

    # 异步方法
    async def _send_message(self, message):
        # 创建连接
        await self.producer.connect()
        # 使用生产者的publish方法，将消息发送到另一个交换机
        await self.producer.publish(message)
        # 使用logging记录发送成功的信息
        logging.info(f"Sent message {message.body} to exchange {self.producer.params['exchange_name']}")
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

    message_handler = MessageHandler(producer_config, consumer_config)
 

    import time
    while True:
        print("这是主函数...")
        time.sleep(10)        
    pass


