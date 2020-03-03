from concurrent import futures
import grpc
import test_pb2
import test_pb2_grpc
import threading
import os
import time


def run_client():
    pid = os.getpid()
    with grpc.insecure_channel('127.0.0.1:21000') as channel:
        stub = test_pb2_grpc.FTQueueStub(channel)
        while True:
            try:
                _label_ = input("enter label to create a que")
                queie_id_response = stub.qCreate(test_pb2.label(value=_label_)).id
                print(queie_id_response, type(queie_id_response))
                element = input("enter element to be inserted in que")
                x = stub.qPush(test_pb2.Push_message(queue_id=queie_id_response,value=element))
                _id= int(input("enter que id to be popped element"))
                x = stub.qPop(test_pb2.queueid(id=_id))
                print("popped element is", x)
                _id = int(input("enter que id to be topped element"))
                x = stub.qTop(test_pb2.queueid(id=_id))
                print("top element is", x)
                _id = int(input("enter que id to get size"))
                x = stub.qSize(test_pb2.queueid(id=_id))
                print("lenght is", x)
                name = input("enter label to delete queue")
                x = stub.qDestroy(test_pb2.label(value=name))

            except KeyboardInterrupt:
                print("Keyboard interrrupt")
                channel.unsubscribe(close)
                exit()


if __name__ == '__main__':
    run_client()








