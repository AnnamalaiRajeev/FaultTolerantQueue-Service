from concurrent import futures
import grpc
import test_pb2
import test_pb2_grpc
import threading
from collections import deque
import time


class Queue:
    def __init__(self, number):
        self.queue = deque()
        self.id = number


class Listener(test_pb2_grpc.FTQueueServicer, test_pb2_grpc.FTQueueDistributedServicer):
    queue_map_labels = {}
    queue_map_id = {}
    number = 0
    servers_list = '127.0.0.1:21000'

    def qCreate(self, request, context):
        if self.queue_map_labels.get(request.value, False) is not False:
            return test_pb2.queueid(id=self.queue_map_labels.get(request.value).id)
            # return Que_id
        else:
            new_queue = Queue(number=self.number)
            self.queue_map_labels[request.value] = new_queue  # map label with new que
            self.queue_map_id[new_queue.id] = new_queue.queue  # map id number to queue
            id_mapped=self.queue_map_labels.get(request.value).id
            self.number += 1
            print('que_id_value', new_queue.id)
            return test_pb2.queueid(id=id_mapped)  # return newly createdQue_id

    def qPush(self, request, context):
        print('requested_id {} type {}'.format(request.queue_id, type(request.queue_id)))
        if self.queue_map_id.get(request.queue_id, False) is not False:
            print("log quueu map", self.queue_map_id.get(request.queue_id, False))
            queue_to_push_context = self.queue_map_id.get(request.queue_id)
            queue_to_push_context.append(request.value)
            print("added value {} to queue {}".format(request.value, request.queue_id))
            return test_pb2.void()
        else:
            return test_pb2.void()

    def qId(self, request, context):
        label_requested = request.label
        if self.queue_map_labels.get(label_requested, False) is not False:
            return test_pb2.queueid(id=self.queue_map_labels.get(label_requested, False).id)
        else:
            print("Que name doesnt exist")
            return test_pb2.queueid(id=-1)  # return -1 que if label not present

    def qPop(self, request, context):
        que_to_pop_from = request.id
        if self.queue_map_id.get(que_to_pop_from, False) is not False:
            if len(self.queue_map_id[que_to_pop_from]) > 0:
                element = self.queue_map_id[que_to_pop_from].pop()  # returns an item from the front of a queue
            else:
                element = ' '
            return test_pb2.Item(value=element)
        else:
            return test_pb2.Item(value=' ')

    def qTop(self, request, context):
        que_to_pop_from = request.id
        if self.queue_map_id.get(que_to_pop_from, False) is not False:
            if len(self.queue_map_id[que_to_pop_from]) > 0:
                element = self.queue_map_id[que_to_pop_from][0]  # returns an item from the front of a queue
            else:
                element=' '
            return test_pb2.Item(value=element)
        else:
            return test_pb2.Item(value=' ')

    def qSize(self, request, context):
        que_to_pop_from = request.id
        if self.queue_map_id.get(que_to_pop_from, False) is not False:
            element = len(self.queue_map_id[que_to_pop_from])  # returns an item from the front of a queue
            return test_pb2.QueLength(length=element)
        else:
            return test_pb2.QueLength(length=-1)

    def qDestroy(self, request, context):
        que_label = request.value
        if self.queue_map_labels.get(que_label, False) is not False:
            del self.queue_map_id[self.queue_map_labels[que_label].id]
            del self.queue_map_labels[que_label]
            return test_pb2.void()
        else:
            return test_pb2.void()

    def qCreateDistributed(self, request, context):
        print("yo", request.sequence)
        if self.queue_map_labels.get(request.value, False) is not False:
            return test_pb2.void()
            # return Que_id
        else:
            new_queue = Queue(number=self.number)
            self.queue_map_labels[request.value] = new_queue  # map label with new que
            self.queue_map_id[new_queue.id] = new_queue.queue  # map id number to queue
            id_mapped = self.queue_map_labels.get(request.value).id
            self.number += 1
            print('que_id_value', new_queue.id)
            return test_pb2.void()  # return newly createdQue_id


def serve_ftqueue_service():
    print("Starting FTqueue service on port 21000")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    try:
        test_pb2_grpc.add_FTQueueDistributedServicer_to_server(Listener(), server)
        test_pb2_grpc.add_FTQueueServicer_to_server(Listener(), server)
        server.add_insecure_port("0.0.0.0:21000")
        server.start()
        while True:
            time.sleep(5)
            obj_2 = Listener()
            print(obj_2.queue_map_id)
            print(obj_2.queue_map_labels)
            pass
    except KeyboardInterrupt:
        print("Gracefull exit")
        server.stop(0)


if __name__ == '__main__':
    serve_ftqueue_service()
