# 导入aio_pika库和logging库
import aio_pika
import logging
import asyncio

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
            # 创建一个交换机，使用参数字典中的交换机名称和类型
            #self.exchange = await self.channel.declare_exchange(self.params["exchange_name"], self.params["exchange_type"])
            # 获取一个已经存在的交换机
            self.exchange = await self.channel.get_exchange(self.params["exchange_name"])
            # 使用logging记录连接成功的信息
            logging.info(f"RaabitMQ Producer Connected to {self.params['host']} and created exchange {self.params['exchange_name']}")
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Producer Failed to connect to {self.params['host']}: {e}")
    
    async def publish(self, message):
        try:
            # 创建一个aio_pika的Message对象，使用参数字典中的编码方式
            message = aio_pika.Message(body=message.encode("utf-8"))  # 修复语法错误：添加缺少的括号
            # 使用exchange的publish方法，将message发送到指定的路由键
            await self.exchange.publish(message, routing_key=self.params["routing_key"])
            # 使用logging记录发送成功的信息
            logging.info(f"RaabitMQ Producer Sent message {message.body} to {self.params['exchange_name']} with routing key {self.params['routing_key']}")
        except Exception as e:
            # 使用logging记录发送失败的异常
            logging.error(f"RaabitMQ Producer Failed to send message {message.body}: {e}")

# 定义一个消费者类
class Consumer:
    # 初始化方法，接受一个参数字典，包含服务器地址，交换机名称，队列名称，路由键等信息
    def __init__(self, params):
        self.params = params
        self.connection = None
        self.channel = None
        self.queue = None
    
    # 异步方法，连接到rabbitmq服务器，创建一个channel和一个queue
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
            # 创建一个队列，使用参数字典中的队列名称
            # self.queue = await self.channel.declare_queue(self.params["queue_name"])
            # 获取一个已经存在的队列
            self.queue = await self.channel.get_queue(self.params["queue_name"])
            # 将队列绑定到交换机，使用参数字典中的交换机名称和路由键
            await self.queue.bind(self.params["exchange_name"], self.params["routing_key"])
            # 使用logging记录连接成功的信息
            logging.info(f"RaabitMQ Consumer Connected to {self.params['host']} and bound queue {self.params['queue_name']} to exchange {self.params['exchange_name']} with routing key {self.params['routing_key']}")
        except Exception as e:
            # 使用logging记录连接失败的异常
            logging.error(f"RaabitMQ Consumer Failed to connect to {self.params['host']}: {e}")
    
    # 异步方法，接受一个callback参数，使用aio_pika的consume方法，将callback作为消息处理函数，要求新建一个线程运行这个callback函数
    async def start_consuming(self, callback):
        try:
            # 使用queue的consume方法，传入callback函数和参数字典中的编码方式，返回一个消费者对象
            consumer = await self.queue.consume(callback)
            # 使用logging记录开始消费的信息
            logging.info(f"RaabitMQ Consumer Started consuming messages from queue {self.params['queue_name']}")
            # 返回消费者对象
            return consumer
        except Exception as e:
            # 使用logging记录开始消费失败的异常
            logging.error(f"RaabitMQ Consumer Failed to start consuming messages from queue {self.params['queue_name']}: {e}")


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
        "queue_name": "APP.TO.CUS",
        "routing_key": "SERVER.TO.CUS",
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
        "queue_name": "CUS.TO.APP",
        "routing_key": "CUS.TO.SERVER",
    }

    import time
    
    async def on_message(message):
        async with message.process():
            print(message.body)

    async def main():
        Com = Consumer(consumer_config)
        await Com.connect()
        await Com.start_consuming(on_message)

        Pub = Producer(producer_config)
        await Pub.connect()
        for i in range(15):
            await Pub.publish(f"hello {i}th world")
            time.sleep(1)

    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()

