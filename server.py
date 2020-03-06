from concurrent import futures
import grpc
import test_pb2
import test_pb2_grpc
from threading import Lock
from collections import deque
import time
import socket
import threading
from threader import run_thread


class Queue:
    def __init__(self, number):
        self.queue = deque()
        self.id = number


class UdpServer:
    def __init__(self, ip=None, port=None):
        try:

            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if ip is None and port is None:
                ip = '0.0.0.0'
                port = 22000
            server_address = (ip, port)
            self.sock.bind(server_address)
            print("UDP server service running on port 22000")
        except Exception as e:
            self.sock.close()
            print("unable to start udp server")


class SharedObject:
    sequence_num = 0


class Listener(test_pb2_grpc.FTQueueServicer, test_pb2_grpc.FTQueueDistributedServicer):

    queue_map_labels = {}
    queue_map_id = {}
    number = 0
    servers_list = ['10.168.0.3:21000']
    # sequence_num = 0
    lock = Lock()
    map_sequence_num_to_Clinet_request_calls = {}
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    number_of_servers = 2
    server_id = 0  # master

    def __init__(self, shared_object):
        super().__init__()
        self.sequence_num = shared_object.sequence_num

    def increment_sequence_num(self):
        with self.lock:
            self.sequence_num += 1
            num = self.sequence_num
        return num

    @run_thread
    def udp_recieve_service(self):
        server = UdpServer()
        socket_udp = server.sock
        while True:
            try:
                # print("threader id for udpserver", threading.current_thread().ident)
                data, address = socket_udp.recvfrom(4096)
                sequence_number, service = data.split(b'#%?')
                sequence_number = int.from_bytes(bytes=sequence_number, byteorder='little')
                Neg_ack_from_address = address[0] + ':' + str(address[1])
                Rpc_server_neg_ack = address[0] + ':' + str(21000)
                service = service.decode('utf-8')
                print("token recieved on udp {}".format(sequence_number))
                print("data {} recieved on udp from {}".format(data, Neg_ack_from_address))
                print("service request in token is : {} recieved on udp from {}".format(service, Neg_ack_from_address))

                if service == 'token':
                    print("circulated token recieved on UDP Socket from {}".format(Neg_ack_from_address))
                    if sequence_number == self.sequence_num+1: # if the sequence number received is the next sequence number
                        print("attempting to increment token ")
                        _ = self.increment_sequence_num()  # if token received from neighbor update sequence number

                    '''
                    if sequence_number > self.sequence_num+1: # then request sequence_number + 1 from master
                        # generate negative ack to all servers # master will respond with appropriate call
                        for socket in self.servers_list:
                            ip = socket.split(':')[0]
                            port = int(socket.split(':')[1])
                            self.udp_send_service(sequence_number=sequence_number+1, request_type=b'missing', server_address=(ip, port))
                    '''

                if service == 'missing':  # resend message to server that requested the missing sequence number
                    print("missing token request recieved on UDP Socket from {}".format(Rpc_server_neg_ack))
                    if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None):  # only server with cache will reply
                        # how to map the channel { 1 : { channel: 10.1.1.1:20000 }

                        @run_thread
                        def sync_token(self):
                            # only the coordinator server will reply the token
                            time.sleep(0.6)
                            if sequence_number % self.number_of_servers == self.server_id:
                                self.udp_send_service(sequence_number=sequence_number, request_type=b'token',
                                                      server_address=address)
                                
                        sync_token(self)

                        with grpc.insecure_channel(Rpc_server_neg_ack) as channel:
                            stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                            # execute RPC call
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qCreateDistributed':
                                _ = map(stub.qCreateDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qCreateDistributed' call request recieved from server {}".format(Neg_ack_from_address))
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qPushDistributed':
                                _ = map(stub.qPushDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qPushDistributed' call request recieved from server {}".format(
                                        Neg_ack_from_address))
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qIdDistributed':
                                _ = map(stub.qIdDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qIdDistributed' call request recieved from server {}".format(
                                        Neg_ack_from_address))
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qPopDistributed':
                                _ = map(stub.qPopDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qPopDistributed' call request recieved from server {}".format(
                                        Neg_ack_from_address))
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qTopDistributed':
                                _ = map(stub.qTopDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qTopDistributed' call request recieved from server {}".format(
                                        Neg_ack_from_address))
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qSizeDistributed':
                                _ = map(stub.qSizeDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qSizeDistributed' call request recieved from server {}".format(
                                        Neg_ack_from_address))
                            if self.map_sequence_num_to_Clinet_request_calls.get(sequence_number, None)['service'] == 'qDestroyDistributed':
                                _ = map(stub.qDestroyDistributed, [self.map_sequence_num_to_Clinet_request_calls[sequence_number]['params']])
                                print(
                                    "executed call 'qDestroyDistributed' call request recieved from server {}".format(
                                        Neg_ack_from_address))

                            for x in _:
                                y = x



            except Exception as e:
                print("UDP listening service raised error: ", e)
                pass

    def udp_send_service(self, sequence_number, request_type, server_address): # request_type = b'update'
        # self.udp_send_service(number, b'missing', (ip, port))
        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        delimiter = b'#%?'
        server_address_to_send = (server_address[0], 22000)
        print("sending token number {}, request type {} to server address {}".format(sequence_number, request_type,
                                                                                     server_address_to_send))
        y = sequence_number.to_bytes((sequence_number.bit_length() + 7) // 8, byteorder='little')
        print("sending message {}".format(y+delimiter+request_type))
        send_sock.sendto(y + delimiter + request_type, server_address_to_send)
        send_sock.close()

    def circulate_token(self, token_num):
        print("circulating token number {}".format(token_num))
        for socket in self.servers_list:
            ip = socket.split(':')[0]
            port = int(socket.split(':')[1])
            self.udp_send_service(sequence_number=token_num, request_type=b'token', server_address=(ip, port))

    def qCreate(self, request, context):
        sequence_number = self.increment_sequence_num()  # increment sequence number for a new request
        print("incremented_sequence_number{} {}".format(sequence_number,self.sequence_num))
        params = test_pb2.label_Dis(value=request.value, sequence=sequence_number)
        service = 'qCreateDistributed'

        @run_thread
        def message_pass(self): # map client request and corresponding messages to sequence number and respective recievers
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qCreateDistributed(params)

        def sync_systems(self):
            message_pass(self)

            print("syncing systems with message qCreateDistributed")

            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}
            time.sleep(0.1)

            if self.sequence_num % self.number_of_servers == self.server_id:
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        # Execute the client request
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
        sequence_number = self.increment_sequence_num()  # increment sequence number for a new request
        params = test_pb2.Push_message_Dis(queue_id=request.queue_id, value=request.value, sequence=sequence_number)
        service = 'qPushDistributed'
        print(self.map_sequence_num_to_Clinet_request_calls)

        @run_thread
        def message_pass(self):  # map client request and corresponding messages to sequence number and respective recievers
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qPushDistributed(params)

        def sync_systems(self):
            try:
                message_pass(self)
            except:
                pass

            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}
            time.sleep(0.3)
            if self.sequence_num % self.number_of_servers == self.server_id:
                print("Circulating token{} by server {}".format(sequence_number, self.server_id))
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        #  cater client request
        if self.queue_map_id.get(request.queue_id, False) is not False:
            print("log quueu map", self.queue_map_id.get(request.queue_id, False))
            queue_to_push_context = self.queue_map_id.get(request.queue_id)
            queue_to_push_context.append(request.value)
            print("added value {} to queue {}".format(request.value, request.queue_id))
            return test_pb2.void()
        else:
            return test_pb2.void()

    def qId(self, request, context):
        sequence_number = self.increment_sequence_num()
        label_requested = request.label
        params = test_pb2.label_Dis(value=request.value, sequence=sequence_number)
        service = 'qIdDistributed'

        @run_thread
        def message_pass(self):
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qIdDistributed(params)

        def sync_systems(self):
            message_pass(self)
            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}
            time.sleep(0.2)
            if self.sequence_num % self.number_of_servers == self.server_id:
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        if self.queue_map_labels.get(label_requested, False) is not False:
            id_val = self.queue_map_labels.get(label_requested, False).id
        else:
            id_val = -1

        return test_pb2.queueid(id=id_val)

    def qPop(self, request, context):
        que_to_pop_from = request.id
        sequence_number = self.increment_sequence_num()
        params = test_pb2.queueid_Dis(id=que_to_pop_from, sequence=sequence_number)
        service = 'qPopDistributed'

        @run_thread
        def message_pass(self):  # map client request and corresponding messages to sequence number and respective recievers
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qPopDistributed(params)

        def sync_systems(self):
            message_pass(self)

            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}
            time.sleep(0.3)

            if self.sequence_num % self.number_of_servers == self.server_id:
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        # cater client

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
        sequence_number = self.increment_sequence_num()
        params = test_pb2.queueid_Dis(id=que_to_pop_from, sequence=sequence_number)
        service = 'qTopDistributed'

        @run_thread
        def message_pass(self):  # map client request and corresponding messages to sequence number and respective recievers
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qTopDistributed(params)

        def sync_systems(self):
            message_pass(self)

            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}
            time.sleep(0.2)
            if self.sequence_num % self.number_of_servers == self.server_id:
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        # cater client

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
        sequence_number = self.increment_sequence_num()
        params = test_pb2.queueid_Dis(id=que_to_pop_from, sequence=self.sequence_num)
        service = 'qSizeDistributed'

        @run_thread
        def message_pass(self):  # map client request and corresponding messages to sequence number and respective recievers
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qSizeDistributed(test_pb2.queueid_Dis(id=que_to_pop_from, sequence=sequence_number))

        def sync_systems(self):
            message_pass(self)
            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}
            time.sleep(0.2)

            if self.sequence_num % self.number_of_servers == self.server_id:
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        # cater client
        if self.queue_map_id.get(que_to_pop_from, False) is not False:
            element = len(self.queue_map_id[que_to_pop_from])  # returns an item from the front of a queue
            return test_pb2.QueLength(length=element)
        else:
            return test_pb2.QueLength(length=-1)

    def qDestroy(self, request, context):

        que_label = request.value
        sequence_number = self.increment_sequence_num()
        print("destroy _sequence Track {} {}".format(sequence_number, self.sequence_num))
        params = test_pb2.label_Dis(value=que_label, sequence=sequence_number)
        service = 'qDestroyDistributed'

        @run_thread
        def message_pass(self):  # map client request and corresponding messages to sequence number and respective recievers
            for socket in self.servers_list:  # socket = '192.168.56.101:21000'
                with grpc.insecure_channel(socket) as channel:
                    stub = test_pb2_grpc.FTQueueDistributedStub(channel)
                    _ = stub.qDestroyDistributed(params)

        def sync_systems(self):
            message_pass(self)
            with self.lock:
                self.map_sequence_num_to_Clinet_request_calls[sequence_number] = {'service': service, 'params': params}

            time.sleep(0.2)
            if self.sequence_num % self.number_of_servers == self.server_id:
                # circulate token if ur the next server to serve token
                self.circulate_token(token_num=sequence_number)

        # sync systems # message passing
        try:
            sync_systems(self)
        except Exception as e:
            print("failed to perform sync systems due to error {}".format(e))

        if self.queue_map_labels.get(que_label, False) is not False:
            del self.queue_map_id[self.queue_map_labels[que_label].id]
            del self.queue_map_labels[que_label]
            return test_pb2.void()
        else:
            return test_pb2.void()

    def request_message_retry(self, number):
        print("Sending request_message_retry for token number {}".format(number))
        time.sleep(1)
        for socket in self.servers_list:
            # request message from all servers master responds
            ip = socket.split(':')[0]
            port = int(socket.split(':')[1])
            self.udp_send_service(sequence_number=number, request_type=b'missing', server_address=(ip, port))  # request_type = b'update'
            # self.udp_send_service(sequence_number=token_num, request_type=b'token', server_address=(ip, port))

    def perform_if_seq_greater(self, token_num):
        sequence_number_to_request = self.sequence_num + 1
        while sequence_number_to_request < token_num:
            sequence_number_to_request = self.sequence_num + 1
            self.request_message_retry(sequence_number_to_request)
            time.sleep(1)

    def qCreateDistributed(self, request, context):
        token_num = request.sequence
        print("qCreateDistri token number-- >recieved {} sequence  number present {}".format(token_num, self.sequence_num))

        def deliver_message_to_ftque(self):
            if self.queue_map_labels.get(request.value, False) is not False:
                return test_pb2.void_Dis()
                # return Que_id
            else:
                new_queue = Queue(number=self.number)
                self.queue_map_labels[request.value] = new_queue  # map label with new que
                self.queue_map_id[new_queue.id] = new_queue.queue  # map id number to queue
                id_mapped = self.queue_map_labels.get(request.value).id
                self.number += 1
                print('que_id_value', new_queue.id)
                return test_pb2.void_Dis()  # return newly createdQue_id

        if token_num == self.sequence_num + 1:
            if token_num % self.number_of_servers == self.server_id:

                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                if token_num == self.sequence_num:
                    print("Token match delivering message to que")
                    return_message = deliver_message_to_ftque(self)
                    return return_message
                else:
                    return test_pb2.void_Dis()
                # send synchronize numbers
            start = time.time()
            while True:
                time.sleep(0.1)
                if time.time() - start >= float(2):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                if token_num == self.sequence_num:
                    print("token_number verified to {}".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()

    def qPushDistributed(self, request, context):
        token_num = request.sequence
        print("qPushDistri token number in message {}, sequence number present {}".format(token_num,self.sequence_num))

        def deliver_message_to_ftque(self):

            print('requested_id {} type {}'.format(request.queue_id, type(request.queue_id)))
            if self.queue_map_id.get(request.queue_id, False) is not False:
                print("log map", self.queue_map_id.get(request.queue_id, False))
                queue_to_push_context = self.queue_map_id.get(request.queue_id)
                queue_to_push_context.append(request.value)
                print("added value {} to queue {}".format(request.value, request.queue_id))
                return test_pb2.void_Dis()
            else:
                return test_pb2.void_Dis()

        if token_num == self.sequence_num:
            return_meesage = deliver_message_to_ftque(self)
            return return_meesage

        if token_num == self.sequence_num + 1:
            time.sleep(0.2)
            print("yes the token recieved is +1 of the existing sequence number")
            if token_num % self.number_of_servers == self.server_id:
                print("I'm the coordinator for this message")
                return_message = deliver_message_to_ftque(self)
                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                return return_message

            start = time.time()
            while True:
                # print("threader id for qpushdist", threading.current_thread().ident)
                print("I,m not the coordinator for this message")
                print("waiting for message sync for token "
                      "from coordinator server_id".format(token_num % self.number_of_servers))
                if token_num == self.sequence_num:
                    print("token_number is now matching with sequence number : {}  Delivering message to queue".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message
                time.sleep(0.5)
                if time.time() - start >= float(3):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                print(self.sequence_num)

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()

    def qIdDistributed(self, request, context):
        token_num = request.sequence
        print("qId ", token_num)
        label_requested = request.label

        def deliver_message_to_ftque(self):
            if self.queue_map_labels.get(label_requested, False) is not False:
                return test_pb2.void_Dis()
            else:
                print("Que name doesnt exist")
                return test_pb2.void_Dis()  # return -1 que if label not present

        if token_num == self.sequence_num + 1:
            if token_num % self.number_of_servers == self.server_id:
                return_message = deliver_message_to_ftque(self)
                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                return return_message
            start = time.time()
            while True:
                time.sleep(0.1)
                if time.time() - start >= float(2):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                if token_num == self.sequence_num:
                    print("token_number verified to {}".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()

    def qPopDistributed(self, request, context):
        token_num = request.sequence
        print("qPop ", token_num)
        que_to_pop_from = request.id

        def deliver_message_to_ftque(self):
            if self.queue_map_id.get(que_to_pop_from, False) is not False:
                if len(self.queue_map_id[que_to_pop_from]) > 0:
                    element = self.queue_map_id[que_to_pop_from].pop()  # returns an item from the front of a queue
                else:
                    element = ' '
                return test_pb2.void_Dis()
            else:
                return test_pb2.void_Dis()

        if token_num == self.sequence_num + 1:
            if token_num % self.number_of_servers == self.server_id:
                return_value = deliver_message_to_ftque(self)
                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                return return_value
            start = time.time()
            while True:
                time.sleep(0.1)
                if time.time() - start >= float(2):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                if token_num == self.sequence_num:
                    print("token_number verified to {}".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()

    def qTopDistributed(self, request, context):
        token_num = request.sequence
        print("qTop ", token_num)
        que_to_pop_from = request.id

        def deliver_message_to_ftque(self):
            if self.queue_map_id.get(que_to_pop_from, False) is not False:
                if len(self.queue_map_id[que_to_pop_from]) > 0:
                    element = self.queue_map_id[que_to_pop_from][0]  # returns an item from the front of a queue
                else:
                    element = ' '
                return test_pb2.void_Dis()
            else:
                return test_pb2.void_Dis()

        if token_num == self.sequence_num + 1:
            if token_num % self.number_of_servers == self.server_id:
                return_message = deliver_message_to_ftque(self)
                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                return return_message
            start = time.time()
            while True:
                time.sleep(0.1)
                if time.time() - start >= float(2):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                if token_num == self.sequence_num:
                    print("token_number verified to {}".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()

    def qSizeDistributed(self, request, context):
        token_num = request.sequence
        print("qSize ", token_num)
        que_to_pop_from = request.id

        def deliver_message_to_ftque(self):
            if self.queue_map_id.get(que_to_pop_from, False) is not False:
                element = len(self.queue_map_id[que_to_pop_from])  # returns an item from the front of a queue
                return test_pb2.void_Dis()
            else:
                return test_pb2.void_Dis()

        if token_num == self.sequence_num + 1:
            if token_num % self.number_of_servers == self.server_id:
                return_message = deliver_message_to_ftque(self)
                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                return return_message
            start = time.time()
            while True:
                time.sleep(0.1)
                if time.time() - start >= float(2):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                if token_num == self.sequence_num:
                    print("token_number verified to {}".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()

    def qDestroyDistributed(self, request, context):
        token_num = request.sequence
        print("destroy ", token_num)
        que_label = request.value

        def deliver_message_to_ftque(self):
            if self.queue_map_labels.get(que_label, False) is not False:
                del self.queue_map_id[self.queue_map_labels[que_label].id]
                del self.queue_map_labels[que_label]
                return test_pb2.void_Dis()
            else:
                return test_pb2.void_Dis()

        if token_num == self.sequence_num + 1:
            if token_num % self.number_of_servers == self.server_id:
                return_message = deliver_message_to_ftque(self)
                new_number = self.increment_sequence_num()
                self.circulate_token(token_num=new_number)
                return return_message
            start = time.time()
            while True:
                time.sleep(0.1)
                if time.time() - start >= float(2):  # Assuming upper bound of 2 Sec
                    self.request_message_retry(token_num)  # request_type = b'missing'
                    # request seq_message
                    break
                if token_num == self.sequence_num:
                    print("token_number verified to {}".format(self.sequence_num))
                    return_message = deliver_message_to_ftque(self)
                    return return_message

        elif token_num > self.sequence_num + 1:
            self.perform_if_seq_greater(token_num)

        return test_pb2.void_Dis()


def serve_ftqueue_service():
    print("Starting FTqueue service on port 21000")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=6))
    try:
        shared_object = SharedObject()
        service = Listener(shared_object)
        test_pb2_grpc.add_FTQueueDistributedServicer_to_server(service, server)
        test_pb2_grpc.add_FTQueueServicer_to_server(service, server)
        server.add_insecure_port("0.0.0.0:21000")
        server.start()
        service.udp_recieve_service()
        while True:
            time.sleep(5)
            print(service.queue_map_id)
            print(service.queue_map_labels)
            print(service.sequence_num)
            # print("mapping of service calls :", service.map_sequence_num_to_Clinet_request_calls)
            # print(threading.enumerate())
            pass
    except KeyboardInterrupt:
        print("Gracefull exit")
        server.stop(0)


if __name__ == '__main__':
    serve_ftqueue_service()
