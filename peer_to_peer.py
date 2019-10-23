import socket
import serializer
import sys
import random
import threading
import command_list_parser
import time


class Peer2PeerWorker:
    def __init__(self, ip, port, num_nodes):
        self.__data = {}
        self.num_commands = 0
        self.serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serv.bind((ip, port))
        self.serv.listen(num_nodes)
        self.num_nodes = num_nodes

    def init_connections(self):
        print("DATAKEEPER INFO: Waiting connection from nodes", file=sys.stderr)
        self.conn = []
        for i in range(self.num_nodes):
            conn, addr = self.serv.accept()
            print("DATAKEEPER INFO: Node {} connected. ID={}".format(addr, i), file=sys.stderr)
            self.conn.append(conn)

    def listen(self, node_id):
        print("DATAKEEPER INFO: Start listen {} node".format(node_id), file=sys.stderr)
        conn = self.conn[node_id]
        while True:
            data = conn.recv(serializer.COMMAND_SIZE)
            if not data: break
            try:
                cmd, *args = serializer.encode_command(data)
                print("DATAKEEPER INFO: command {} {}".format(cmd, args), file=sys.stderr)
                self.num_commands += 1
                if cmd == "SELECT":
                    if len(args) == 0:
                        for k, v in self.__data.items():
                            encoded_data = serializer.decode_answer(key=k,
                                                                    value=v,
                                                                    padding=True)
                            conn.send(encoded_data)
                        conn.send(b'0')
                    elif args[0] in self.__data:
                        encoded_data = serializer.decode_answer(key=args[0],
                                                                value=self.__data[args[0]])
                        conn.send(encoded_data)
                    else:
                        conn.send(b'0')
                elif cmd == "INSERT":
                    self.__data[args[0]] = args[1]
                    print("DATAKEEPER INFO: INSERTED key={}, value={}".format(*args), file=sys.stderr)
                else:
                    raise RuntimeError("Wrong command {}".format((cmd, *args)))
            except Exception as e:
                print("WARNING: Execute '{}' failed.\n".format(data) +
                      "Exception - '{}'".format(e), file=sys.stderr)
        print("DATAKEEPER INFO: Connection {} closed.".format(node_id), file=sys.stderr)
        print("DATAKEEPER INFO: Num commands {}. Data contains {}".format(self.num_commands, len(self.__data)),
              file=sys.stderr)
        conn.close()


class Peer2PeerMaster:
    def __init__(self, node_list):
        self.nodes = []
        self.node_list = node_list

    def init_connections(self):
        for ipport in self.node_list:
            node = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("MASTER INFO: Init connection to {}".format(ipport), file=sys.stderr)
            node.connect(ipport)
            print("MASTER INFO: Connected to node {}".format(ipport), file=sys.stderr)
            self.nodes.append(node)

    def close(self):
        for node in self.nodes:
            node.close()

    def get_all_items(self):
        cmd = "SELECT"
        #TODO
        items = {}
        for i, node in enumerate(self.nodes):
            data = serializer.decode_command(cmd)
            node.send(data)
            while True:
                answ = node.recv(serializer.ANSWER_SIZE)
                if answ == b'0':
                    break
                s = serializer.encode_answer(answ)
                items[s[0]]=s[1]
        return "\n".join(map(lambda x: "{} {}".format(x[0], x[1]), items.items()))

    def get_item_from_node(self, cmd, key, node_id):
        data = serializer.decode_command(cmd=cmd, key=key)
        self.nodes[node_id].send(data)
        answ = self.nodes[node_id].recv(serializer.ANSWER_SIZE)
        if answ == None:
            return None
        if answ == b'0':
            return None
        s = serializer.encode_answer(answ)
        return s

    def select(self,  args):
        cmd = "SELECT"
        for i, node in enumerate(self.nodes):
            answ = self.get_item_from_node(cmd=cmd,
                                           key=args[0],
                                           node_id=i)
            if answ:
               return i, answ
        return None, None

    def insert_item_to_node(self, key, value, node_id,):
        cmd = "INSERT"
        data = serializer.decode_command(cmd, key, value)
        self.nodes[node_id].send(data)

    def apply_command(self, command):
        cmd, *args = command.upper().split()
        if cmd == "SELECT":
            if len(args) == 0:
                return self.get_all_items()
            node_id, answ = self.select(args)
            if answ:
                return "{} {}".format(*answ)
            raise "Key {} not found in database".format(args[0])
        if cmd == "INSERT":
            # Чекаем не update ли это
            node_id, select_answ = self.select(args)
            if node_id==None:
                # выбираем индекс случайно
                node_id = random.choice(range(len(self.nodes)))
            print("MASTER INFO: Inserting to node {}".format(node_id), file=sys.stderr)
            self.insert_item_to_node(key=args[0],
                                     value=args[1],
                                     node_id=node_id)
            return
        raise RuntimeError("Wrong command")

    def apply_task_file(self, fname):
        commands_queue = command_list_parser.command_loader(fname)

        start_time = time.time()
        while not commands_queue.empty():
            t, com = commands_queue.get()
            print("MASTER INFO: Start '{}' command. Time {}. Command time - {}".format(com, time.time() - start_time, t),
                  file=sys.stderr)
            delta_t = start_time + t - time.time() - args.latency
            if delta_t > 0:
                print("MASTER INFO: Sleep {} seconds".format(delta_t),
                      file=sys.stderr)
                time.sleep(start_time + t - time.time() - args.latency)
            if time.time() - start_time > t:
                print("WARNING: Command was {} ms late".format(round((time.time() - start_time - t) * 1000)),
                      file=sys.stderr)
            answ = self.apply_command(com)
            print(com)
            if answ:
                print(answ)
            print("MASTER INFO: Done '{}' at time {}".format(com, time.time() - start_time), file=sys.stderr)
        print("MASTER INFO: Done at time {}".format(time.time() - start_time), file=sys.stderr)

def initialize_cluster(nodes_ip):
    nodes_worker = []
    nodes_master =[]
    nodes_worker_threads = []
    nodes_master_threads = []
    for ip, port in nodes_ip:
        worker = Peer2PeerWorker(ip, port, len(nodes_ip))
        nodes_worker.append(worker)
        thr = threading.Thread(target=Peer2PeerWorker.init_connections, args=(worker,))
        nodes_worker_threads.append(thr)
        thr.start()
    for i in range(len(nodes_ip)):
        master = Peer2PeerMaster(nodes_ip)
        nodes_master.append(master)
        thr = threading.Thread(target=Peer2PeerMaster.init_connections, args=(master,))
        nodes_master_threads.append(thr)
        thr.start()
    for thr in nodes_master_threads:
        thr.join()
    for thr in nodes_worker_threads:
        thr.join()
    return nodes_master, nodes_worker






if __name__=="__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", nargs="*", help="ip:port:file")
    parser.add_argument("--latency", type=float, default=50/1000)
    parser.add_argument("--logfile", nargs="?",  type=argparse.FileType('w'), default=sys.stderr)
    args = parser.parse_args()
    print(vars(args), file=sys.stderr)
    # хак для удобного логирования
    sys.stderr = args.logfile
    nodes_conf = [(ip, int(port), file) for ip, port, file in map(lambda x: x.split(":"), args.nodes)]

    nodes_master, nodes_worker = initialize_cluster([(ip, port) for ip, port, _ in nodes_conf])
    listener_thrs = [threading.Thread(target=Peer2PeerWorker.listen, args=(worker, i)) for i in range(len(nodes_conf))
                     for worker in nodes_worker]
    for thr in listener_thrs:
        thr.start()

    reader_thrs = [threading.Thread(target=Peer2PeerMaster.apply_task_file, args=(nodes_master[i], fname))
                   for i, (_, _, fname) in enumerate(nodes_conf)]

    for thr in reader_thrs:
        thr.start()

    for thr in reader_thrs:
        thr.join()
    for m in nodes_master:
        m.close()
    print("Readers done", file=sys.stderr)
    for thr in listener_thrs:
        thr.join()

    print("Datakeepers done", file=sys.stderr)


