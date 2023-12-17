# 导入 pika 模块
import pika

# 创建一个 PlainCredentials 对象，用于存储用户名和密码
credentials = pika.PlainCredentials("OOCL", "123321")

# 创建一个 ConnectionParameters 对象，用于设置连接参数
parameters = pika.ConnectionParameters(
    host="localhost",
    port=5672,
    virtual_host="OOCL",
    credentials=credentials,
    socket_timeout=5
)

# 创建一个 BlockingConnection 对象，用于建立与 RabbitMQ 的连接
connection = pika.BlockingConnection(parameters)

# 创建一个 Channel 对象，用于与 RabbitMQ 通信
channel = connection.channel()
print(channel)
# 声明一个 Exchange 对象，用于指定交换机的名称和类型
exchange = channel.exchange_declare(
    exchange="SANL",
    exchange_type="direct",
    durable=True,
    auto_delete=True
)

# 声明一个 Queue 对象，用于指定队列的名称
queue = channel.queue_declare(
    queue="APP.TO.CUS"
)

# 绑定队列和交换机，用于指定路由键
binding = channel.queue_bind(
    queue="APP.TO.CUS",
    exchange="SANL",
    routing_key="SERVER.TO.APP"
)

# 定义一个回调函数，用于处理从队列中获取的消息
def callback(ch, method, properties, body):
    # 打印消息的内容
    print(f"Received {body}")
    # 确认消息已经被消费
    ch.basic_ack(delivery_tag=method.delivery_tag)

# 注册回调函数，用于消费队列中的消息
consumer = channel.basic_consume(
    queue="APP.TO.CUS",
    on_message_callback=callback
)

# 开始消费消息
print("Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
